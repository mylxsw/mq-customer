package log

import (
	"io"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("mq-customer")

// InitLogger 用来初始化日志输出文件
func InitLogger(logFile io.Writer) {

	var format = "%{time:2006-01-02 15:04:05} [%{level}] %{message}"

	logging.SetBackend(
		logging.NewBackendFormatter(
			logging.NewLogBackend(logFile, "", 0),
			logging.MustStringFormatter(format),
		),
	)
}

// Info function print a info message
func Info(message string, a ...interface{}) {
	log.Info(message, a...)
}

// Error function print a error message
func Error(message string, a ...interface{}) {
	log.Error(message, a...)
}

// Debug function print a debug message
func Debug(message string, a ...interface{}) {
	log.Debug(message, a...)
}

// Warning function print a warning log
func Warning(message string, a ...interface{}) {
	log.Warning(message, a...)
}

// Notice function print a notice message
func Notice(message string, a ...interface{}) {
	log.Notice(message, a...)
}

// Fatal function print log and exit
func Fatal(message string, a ...interface{}) {
	log.Fatalf(message, a...)
}
