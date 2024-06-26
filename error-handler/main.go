package main

import (
    "log"

    "github.com/streadway/amqp"
)

func main() {
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

    consumeDLQ(ch, "product_dlq")
    consumeDLQ(ch, "order_dlq")
    consumeDLQ(ch, "customer_dlq")

    log.Println("Error Handler Service running...")
    select {}
}

func consumeDLQ(ch *amqp.Channel, queueName string) {
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
            log.Printf("Received a message from %s: %s", queueName, d.Body)
        }
    }()
}
