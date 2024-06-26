package main

import (
    "log"
    "os"
    "time"

    "github.com/streadway/amqp"
)

func main() {
    logFile, err := os.OpenFile("service.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
    if err != nil {
        log.Fatalf("Failed to open log file: %s", err)
    }
    defer logFile.Close()

    log.SetOutput(logFile)

    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatalf("Failed to connect to RabbitMQ: %s", err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatalf("Failed to open a channel: %s", err)
    }
    defer ch.Close()

    consumeLogs(ch, "logging_queue")

    log.Println("Monitoring & Logging Service running...")
    select {}
}

func consumeLogs(ch *amqp.Channel, queueName string) {
    msgs, err := ch.Consume(
        queueName, // queue
        "",        // consumer
        true,      // auto-ack
        false,     // exclusive
        false,     // no-local
        false,     // no-wait
        nil,       // args
    )
    if err != nil {
        log.Fatalf("Failed to register a consumer: %s", err)
    }

    go func() {
        for d := range msgs {
            log.Printf("[%s] Received log: %s", time.Now().Format(time.RFC3339), d.Body)
        }
    }()
}
