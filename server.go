package main

import (
	"bufio"
	"fmt"
	"github.com/Shopify/sarama"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
	"strings"
)

const (
	SERVER_PORT      = "9000"
	SERVER_HOST      = "0.0.0.0"
	SERVER_TYPE      = "tcp"
	TIMEOUT_DURATION = 5 * time.Second
	KAFKA_BROKER     = "broker-1:9092,broker-2:9092"
	KAFKA_TOPIC      = "topic"
	KAFKA_CLIENT_ID  = "producer-client"
)

func main() {
	messageChan := make(chan string)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go startTcpServer(wg, messageChan)
	fmt.Println("Starting kafka producer")
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go startProducer(wg, messageChan)
		wg.Wait()
	}

}

func startProducer(wg *sync.WaitGroup, messageChan chan string) {
	defer wg.Done()
	producer, err := initProducer()
	defer producer.AsyncClose()
	switch err {
	case nil:
		break
	default:
		panic(err.Error())
	}
	fmt.Println()
	publish(producer, messageChan)
}

func publish(producer sarama.AsyncProducer, messageChan chan string) {
	for {
		message := <-messageChan
		kafkaMessage := &sarama.ProducerMessage{
			Topic: KAFKA_TOPIC,
			Value: sarama.StringEncoder(message),
		}
		producer.Input() <- kafkaMessage
	}
}

func initProducer() (sarama.AsyncProducer, error) {
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)
	config := sarama.NewConfig()
	config.ClientID = KAFKA_CLIENT_ID
	config.Producer.Retry.Max = 5
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	config.Producer.RequiredAcks = sarama.WaitForLocal
	producer, err := sarama.NewAsyncProducer(strings.Split(KAFKA_BROKER,","), config)
	return producer, err
}

func startTcpServer(wg *sync.WaitGroup, messageChan chan string) {

	server, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	if err != nil {
		fmt.Println("Error listening:")
		panic(err.Error())
	}
	//close the server on exit
	defer server.Close()
	defer wg.Done()

	for {
		connection, err := server.Accept()
		defer connection.Close()
		if err != nil {
			fmt.Println("Can't accept connection")
			panic(err.Error())
		}
		go handlerequest(connection, messageChan)
	}
}

func handlerequest(connection net.Conn, messageChan chan<- string) {
	bufReader := bufio.NewReader(connection)
	defer connection.Close()
	for {
		// Set a deadline for reading. Read operation will fail if no data
		// is received after deadline.
		connection.SetReadDeadline(time.Now().Add(TIMEOUT_DURATION))

		// Read tokens delimited by newline
		bytes, err := bufReader.ReadBytes('\n')
		switch err {
		case io.EOF:
			{
				fmt.Println("Client closed connection")
				return
			}
		case nil:
			{
				msg := string(bytes)
				//fmt.Println(msg)
				messageChan <- msg
			}
		default:
			panic(err.Error())
		}
	}
}
