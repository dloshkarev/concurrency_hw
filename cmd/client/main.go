package main

import (
	"bufio"
	"concurrency_hw/internal/database/network"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"io"
	"log"
	"os"
)

// Клиент максимально колхозный, т.к предполагается что у нашей БД может быть множество клиентов и качество их гарантировать нельзя
// Поэтому тут нет никаких ограничений и проверок на что либо
func main() {
	logger, _ := zap.NewProduction()

	address := flag.String("address", "localhost:3223", "tcp server address")

	client, err := network.NewTCPClient(*address)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Disconnect(); err != nil {
			logger.Fatal("error on disconnect", zap.Error(err))
		}
	}()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		queryString, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Error("server connection closed",
					zap.Error(err),
				)
			} else {
				logger.Error("cannot read query",
					zap.String("query", queryString),
					zap.Error(err),
				)
			}

			return
		}

		response, err := client.Execute(queryString)
		if err != nil {
			logger.Error("cannot execute query",
				zap.String("query", queryString),
				zap.Error(err),
			)

			return
		}

		fmt.Println(string(response))
	}
}
