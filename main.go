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
	Name       string   `json:"name"`
	RoutingKey string   `json:"routing_key"`
	Action     []string `json:"action"`
}

type Result struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func startWorker(ctx context.Context, ch *amqp.Channel, queue Queue, config *Config) {

	log.Debug("[%s] start worker.", queue.Name)

	q, _ := ch.QueueDeclare(queue.Name, true, false, false, false, nil)
	ch.QueueBind(q.Name, queue.RoutingKey, config.ExchangeName, false, nil)

	msgs, _ := ch.Consume(q.Name, "", true, false, false, false, nil)

	for {
		select {
		case <-ctx.Done():
			log.Debug("[%s] Worker exit.", q.Name)
			return
		case d := <-msgs:
			log.Debug("[%s] Received a message: %s", q.Name, d.Body)
			startTime := time.Now()

			arguments := "base64://" + base64.StdEncoding.EncodeToString(d.Body)
			commandArgs := []string{}
			for _, arg := range queue.Action {
				commandArgs = append(commandArgs, strings.Replace(arg, "{data}", arguments, -1))
			}

			log.Debug("[%s] exec: %s", q.Name, strings.Join(commandArgs, " "))

			cmd := exec.Command(commandArgs[0], commandArgs[1:]...)
			cmd.SysProcAttr = &syscall.SysProcAttr{
				Setpgid: true,
			}

			output, _ := cmd.Output()
			log.Debug("[%s] output: %s", q.Name, output)

			var res Result
			json.Unmarshal(output, &res)

			if res.Code == 200 {
				log.Debug("[%s] Success", q.Name)
			} else {
				log.Debug("[%s] Failed", q.Name)
			}

			log.Debug(
				"[%s] time-consuming %v",
				q.Name,
				time.Since(startTime),
			)
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

	return &config, nil
}

var confPath string

func main() {

	flag.StringVar(&confPath, "conf", "", "配置文件路径")
	flag.Parse()

	config, err := parseConfig(confPath)
	if err != nil {
		fmt.Printf("ERROR: %v", err)
		os.Exit(1)
	}

	log.InitLogger(os.Stdout)

	conn, err := amqp.Dial(config.AmqpURL)
	if err != nil {
		log.Fatal("connect to amqp failed: %v", err)
	}
	defer conn.Close()

	err = exchangeDeclare(conn, config)
	if err != nil {
		log.Fatal("declare exchange failed: %v", err)
	}

	// register signal handler
	ctx, cancel := context.WithCancel(context.Background())
	signal.InitSignalReceiver(ctx, cancel)

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
}
