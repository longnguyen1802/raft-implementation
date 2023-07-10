package logging

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

var (
	instance zerolog.Logger
	once     sync.Once
)

type Logger struct {
	*zerolog.Logger
}

func newLogger() zerolog.Logger {
	logLevel := zerolog.DebugLevel

	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		f := file
		split := strings.Split(file, "raft-implement/")
		if len(split) > 1 {
			f = strings.Join(split[1:], "")
		}
		return fmt.Sprintf("%s:%d", f, line)
	}

	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
		FormatLevel: func(i interface{}) string {
			return strings.ToUpper(fmt.Sprintf("|%-5s|", i))
		},
		FormatMessage: func(i interface{}) string {
			return fmt.Sprintf(" %s ", i)
		},
		FormatFieldName: func(i interface{}) string {
			return fmt.Sprintf("%s: ", i)
		},
		FormatFieldValue: func(i interface{}) string {
			return fmt.Sprintf("%s", i)
		},
	}

	return zerolog.New(output).Level(logLevel).With().Timestamp().Logger()
}

func GetInstance() *Logger {
	once.Do(func() {
		instance = newLogger()
	})
	return &Logger{&instance}
}
