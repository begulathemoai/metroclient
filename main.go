package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/begulathemoai/metroserverclient/internal/metroclient"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, err := config.Build()

	if err != nil {
		fmt.Println("Could not set up logging")
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	client, err := metroclient.NewClient("wss://metroserverx.begulathemoai.dev/ws", logger)
	if err != nil {
		logger.Fatal("error : %v", zap.Error(err))
	}

	var code string
	fmt.Scanln(&code)

	err = client.JoinRoom(code)
	if err != nil {
		logger.Error("error : %v", zap.Error(err))
	}
	<-interrupt

	logger.Info("Closing connection...")
	client.Close()
}
