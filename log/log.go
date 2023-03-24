package log

import (
	"Distributed_DB_Project/config"
	"fmt"
	"io"
	"log"
	"os"
)

const (
	ERROR = iota
	INFO
	WARNING
	DEBUG
)

type Log struct {
	errorLogger   *log.Logger
	infoLogger    *log.Logger
	warningLogger *log.Logger
	debugLogger   *log.Logger
	level         int
}

var Logger *Log

// NewLog Create a new Logger
func NewLog(logLevel string, target io.Writer) {
	Logger = new(Log)
	Logger.infoLogger = log.New(target, "[INFO] ", log.LstdFlags|log.Lshortfile|log.Lmsgprefix)
	Logger.debugLogger = log.New(target, "[DEBUG] ", log.LstdFlags|log.Lshortfile|log.Lmsgprefix)
	Logger.warningLogger = log.New(target, "[WARNING] ", log.LstdFlags|log.Lshortfile|log.Lmsgprefix)
	Logger.errorLogger = log.New(target, "[ERROR] ", log.LstdFlags|log.Lshortfile|log.Lmsgprefix)
	// Set Log Level with Config Content
	switch logLevel {
	case "error":
		{
			Logger.level = ERROR
			break
		}
	case "info":
		{
			Logger.level = INFO
			break
		}
	case "warning":
		{
			Logger.level = WARNING
			break
		}
	case "debug":
		{
			Logger.level = DEBUG
			break
		}
	default:
		Logger.level = INFO
	}

}

// INFO output INFO message
func (l Log) INFO(msg string) {
	if l.level < INFO {
		return
	}
	l.infoLogger.Output(2, msg)
}

// DEBUG output DEBUG message
func (l Log) DEBUG(msg string) {
	if l.level < DEBUG {
		return
	}
	l.debugLogger.Output(2, msg)
}

// ERROR output ERROR message
func (l Log) ERROR(msg string) {
	l.errorLogger.Output(2, msg)
}

// WARNING output WARNING message
func (l Log) WARNING(msg string) {
	l.warningLogger.Output(2, msg)
}

// INFOf format output info Log
func (l Log) INFOf(format string, v ...any) {
	if l.level < INFO {
		return
	}
	l.infoLogger.Output(2, fmt.Sprintf(format, v...))
}

// DEBUGf format output debug Log
func (l Log) DEBUGf(format string, v ...any) {
	if l.level < DEBUG {
		return
	}
	l.debugLogger.Output(2, fmt.Sprintf(format, v...))
}

// ERRORf format output error Log
func (l Log) ERRORf(format string, v ...any) {
	l.errorLogger.Output(2, fmt.Sprintf(format, v...))
}

// WARNINGf format output warning Log
func (l Log) WARNINGf(format string, v ...any) {
	l.warningLogger.Output(2, fmt.Sprintf(format, v...))
}

func InitializeLog() *os.File {
	logConfig := InitializeLogConfig()
	target, err := os.OpenFile(logConfig.OutputPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("open file error: %v", err)
	}

	NewLog(logConfig.Level, target)
	return target
}

func InitializeLogConfig() config.LogConfig {
	// Get Configuration
	config := config.Config{}
	LogConfig, err := config.GetLogConfig()
	if err != nil {
		fmt.Println(err.Error())
	}
	return LogConfig
}
