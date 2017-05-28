package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"flag"

	"os"

	"github.com/mylxsw/mq-customer/log"
	"github.com/mylxsw/mq-customer/signal"
	"github.com/streadway/amqp"
)

type Config struct {
	AmqpURL      string  `json:"amqp_url"`
	ExchangeName string  `json:"exchange_name"`
	Queues       []Queue `json:"queues"`
}

type Queue struct {
	Name        string   `json:"name"`
	RoutingKey  string   `json:"routing_key"`
	WorkerCount uint     `json:"worker_count"`
	Action      []string `json:"action"`
}

type Result struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func startWorker(ctx context.Context, ch *amqp.Channel, queue Queue, config *Config) {

	log.Debug("[%s] start worker.", queue.Name)

	q, _ := ch.QueueDeclare(queue.Name, true, false, false, false, nil)
	ch.QueueBind(q.Name, queue.RoutingKey, config.ExchangeName, false, nil)

	ch.Qos(int(queue.WorkerCount), 0, false)
	msgs, _ := ch.Consume(q.Name, "", false, false, false, false, nil)

	for {
		select {
		case <-ctx.Done():
			log.Debug("[%s] Worker exit.", q.Name)
			return
		case d := <-msgs:
			go func(queueName string, msg amqp.Delivery) {
				log.Debug("[%s] Received a message: %s", queueName, msg.Body)

				startTime := time.Now()
				defer func() {
					log.Debug(
						"[%s] time-consuming %v",
						queueName,
						time.Since(startTime),
					)

					if r := recover(); r != nil {
						// 如果任务处理失败，则重新将任务加入队列
						msg.Nack(false, true)
					}
				}()

				arguments := "base64://" + base64.StdEncoding.EncodeToString(msg.Body)
				commandArgs := []string{}
				for _, arg := range queue.Action {
					commandArgs = append(commandArgs, strings.Replace(arg, "{data}", arguments, -1))
				}

				log.Debug("[%s] exec: %s", queueName, strings.Join(commandArgs, " "))

				cmd := exec.Command(commandArgs[0], commandArgs[1:]...)
				cmd.SysProcAttr = &syscall.SysProcAttr{
					Setpgid: true,
				}

				output, _ := cmd.Output()
				log.Debug("[%s] output: %s", queueName, output)

				var res Result
				json.Unmarshal(output, &res)

				if res.Code == 200 {
					log.Debug("[%s] Success", queueName)

					// 处理成功时，返回ack确认
					msg.Ack(false)
				} else {
					log.Debug("[%s] Failed", queueName)

					// 如果任务处理失败，则重新将任务加入队列
					msg.Nack(false, true)
				}
			}(q.Name, d)
		}
	}
}

func exchangeDeclare(conn *amqp.Connection, config *Config) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %v", err)
	}
	defer ch.Close()

	ch.ExchangeDeclare(config.ExchangeName, "topic", true, false, false, false, nil)

	return nil
}

func parseConfig(filename string) (*Config, error) {
	cfgData, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("read file %s failed: %v", filename, err)
	}

	var config Config
	err = json.Unmarshal(cfgData, &config)
	if err != nil {
		return nil, fmt.Errorf("parse config failed: %v", err)
	}

	for index, queue := range config.Queues {
		if queue.WorkerCount == 0 {
			config.Queues[index].WorkerCount = 1
		}
	}

	return &config, nil
}

var confPath string

func main() {

	flag.StringVar(&confPath, "conf", "", "配置文件路径")
	flag.Parse()

	// parse config file
	config, err := parseConfig(confPath)
	if err != nil {
		fmt.Printf("ERROR: %v", err)
		os.Exit(1)
	}

	// init logger
	log.InitLogger(os.Stdout)

	// register signal handler
	ctx, cancel := context.WithCancel(context.Background())
	signal.InitSignalReceiver(ctx, cancel)

	// create a connection to rabbitmq
	conn, err := amqp.Dial(config.AmqpURL)
	if err != nil {
		log.Fatal("connect to amqp failed: %v", err)
	}
	defer conn.Close()

	err = exchangeDeclare(conn, config)
	if err != nil {
		log.Fatal("declare exchange failed: %v", err)
	}

	var wg sync.WaitGroup
	for _, q := range config.Queues {
		wg.Add(1)
		go func(conn *amqp.Connection, queue Queue) {
			defer wg.Done()

			ch, _ := conn.Channel()
			startWorker(ctx, ch, queue, config)
		}(conn, q)
	}

	log.Debug(" [*] Waiting for messages. To exit press CTRL+C")

	wg.Wait()

	log.Debug(" [*] a smooth exit.")
}
