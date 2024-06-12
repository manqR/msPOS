package main

import (
    "log"
    "net/http"
    "github.com/gorilla/mux"
    "product-service/config"
    "product-service/handlers"
    "product-service/middleware"
    "product-service/repository"
    "product-service/service"
    "github.com/rabbitmq/amqp091-go"
)

func main() {
    config.LoadConfig() // Load configuration

    // Initialize MongoDB client
    err := repository.InitMongoClient()
    if err != nil {
        log.Fatalf("Error initializing MongoDB client: %v", err)
    }

    // Initialize RabbitMQ
    conn, err := amqp091.Dial("amqp://guest:guest@127.0.0.1:5672/")
    if err != nil {
        log.Fatalf("Failed to connect to RabbitMQ: %v", err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatalf("Failed to open a channel: %v", err)
    }
    defer ch.Close()

    // Declare exchange
    err = ch.ExchangeDeclare(
        "product_exchange", // name
        "direct",           // type
        true,               // durable
        false,              // auto-deleted
        false,              // internal
        false,              // no-wait
        nil,                // arguments
    )
    if err != nil {
        log.Fatalf("Failed to declare an exchange: %v", err)
    }

    // Declare and bind queues
    queues := map[string]string{
        "product_insert_queue": "product.insert",
        "product_update_queue": "product.update",        
        
    }

    for queue, routingKey := range queues {
        _, err := ch.QueueDeclare(
            queue, // name
            true,  // durable
            false, // delete when unused
            false, // exclusive
            false, // no-wait
            nil,   // arguments
        )
        if err != nil {
            log.Fatalf("Failed to declare a queue: %v", err)
        }

        err = ch.QueueBind(
            queue,             // queue name
            routingKey,        // routing key
            "product_exchange",// exchange
            false,
            nil,
        )
        if err != nil {
            log.Fatalf("Failed to bind a queue: %v", err)
        }
    }

    service.InitRabbitMQ(ch) // Initialize RabbitMQ channel in the service

    r := mux.NewRouter()

    // Apply middleware
    r.Use(middleware.LoggingMiddleware)
    r.Use(middleware.AuthMiddleware)

    // Register routes
    r.HandleFunc("/product/insert", handlers.InsertProduct).Methods("POST")
    r.HandleFunc("/product/select", handlers.SelectProduct).Methods("GET")
    r.HandleFunc("/product/update", handlers.UpdateProduct).Methods("PUT")
    r.HandleFunc("/product/delete", handlers.DeleteProduct).Methods("DELETE")

    log.Println("Server started at :8080")
    log.Fatal(http.ListenAndServe(":8080", r))
}
