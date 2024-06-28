package main

import (
    "log"
    "net/http"
    "os"

    "github.com/gorilla/mux"
    "github.com/rabbitmq/amqp091-go"
    "product-service/config"
    "product-service/handlers"
    "product-service/middleware"
    "product-service/repository"
    "product-service/service"
)

func main() {
    // Load configuration
    config.LoadConfig()

    // Initialize MongoDB client
    if err := repository.InitMongoClient(); err != nil {
        log.Fatalf("Error initializing MongoDB client: %v", err)
    }

    // Initialize RabbitMQ connection
    rabbitMQURL := os.Getenv("RABBITMQ_URL")
    if rabbitMQURL == "" {
        rabbitMQURL = "amqp://guest:guest@127.0.0.1:5672/"
    }
    conn, err := amqp091.Dial(rabbitMQURL)
    if err != nil {
        log.Fatalf("Failed to connect to RabbitMQ: %v", err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatalf("Failed to open a channel: %v", err)
    }
    defer ch.Close()

    // Declare exchanges
    exchanges := []string{"product_exchange", "error_exchange"}
    for _, exchange := range exchanges {
        if err := ch.ExchangeDeclare(
            exchange, // name
            "direct", // type
            true,     // durable
            false,    // auto-deleted
            false,    // internal
            false,    // no-wait
            nil,      // arguments
        ); err != nil {
            log.Fatalf("Failed to declare exchange '%s': %v", exchange, err)
        }
    }

    // Declare and bind product-related queues
    productQueues := map[string]string{
        "product_insert_queue": "product.insert",
        "product_update_queue": "product.update",
    }

    for queue, routingKey := range productQueues {
        if _, err := ch.QueueDeclare(
            queue, // name
            true,  // durable
            false, // delete when unused
            false, // exclusive
            false, // no-wait
            amqp091.Table{
                "x-dead-letter-exchange": "error_exchange",
                "x-dead-letter-routing-key": "product.dlq",
            },
        ); err != nil {
            log.Fatalf("Failed to declare queue '%s': %v", queue, err)
        }

        if err := ch.QueueBind(
            queue,             // queue name
            routingKey,        // routing key
            "product_exchange", // exchange
            false,
            nil,
        ); err != nil {
            log.Fatalf("Failed to bind queue '%s': %v", queue, err)
        }
    }

    // Declare and bind error-related queues with routing keys
    errorQueues := map[string]string{
        "product_dlq":   "product.dlq",
        "logging_queue": "logging.error",
    }

    for queue, routingKey := range errorQueues {
        if _, err := ch.QueueDeclare(
            queue,
            true,
            false,
            false,
            false,
            nil,
        ); err != nil {
            log.Fatalf("Failed to declare queue '%s': %v", queue, err)
        }

        if err := ch.QueueBind(
            queue,             // queue name
            routingKey,        // routing key
            "error_exchange",  // exchange
            false,
            nil,
        ); err != nil {
            log.Fatalf("Failed to bind queue '%s': %v", queue, err)
        }
    }

    // Initialize RabbitMQ channel in the service
    service.InitRabbitMQ(ch)

    // Initialize router
    r := mux.NewRouter()

    // Apply middleware
    r.Use(middleware.LoggingMiddleware)
    r.Use(middleware.AuthMiddleware)

    // Register routes
    r.HandleFunc("/product/insert", handlers.InsertProduct).Methods("POST")
    r.HandleFunc("/product/select", handlers.SelectProduct).Methods("GET")
    r.HandleFunc("/product/update", handlers.UpdateProduct).Methods("PUT")
    r.HandleFunc("/product/delete", handlers.DeleteProduct).Methods("DELETE")

    // Start the server
    log.Println("Server started at :8080")
    log.Fatal(http.ListenAndServe(":8080", r))
}
