package main

import (
    "context"
    "log"
    "os"
    "time"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"

    "github.com/rabbitmq/amqp091-go"

	"monitoring-logging-service/config" 
)

const (
    rabbitMQURL  = "amqp://guest:guest@localhost:5672/"
    logQueueName = "logging_queue"
)

func main() {
	//load config
	config.LoadConfig()

    // Initialize logging to a file
    logFile, err := os.OpenFile("service.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
    if err != nil {
        log.Fatalf("Failed to open log file: %s", err)
    }
    defer logFile.Close()
    log.SetOutput(logFile)

    // Get MongoDB configuration from environment variables
    mongoURI := os.Getenv("MONGO_URI")
    dbName := os.Getenv("DB_NAME")
    collectionName := os.Getenv("COLLECTION_NAME")

    if mongoURI == "" || dbName == "" || collectionName == "" {
        log.Fatalf("MongoDB configuration environment variables not set")
    }

    // Connect to MongoDB
    clientOptions := options.Client().ApplyURI(mongoURI)
    mongoClient, err := mongo.Connect(context.Background(), clientOptions)
    if err != nil {
        log.Fatalf("Failed to connect to MongoDB: %s", err)
    }
    defer mongoClient.Disconnect(context.Background())

    // Ensure MongoDB connection is established
    if err := mongoClient.Ping(context.Background(), nil); err != nil {
        log.Fatalf("Failed to ping MongoDB: %s", err)
    }
    log.Println("Connected to MongoDB")

    // Connect to RabbitMQ
    conn, err := amqp091.Dial(rabbitMQURL)
    if err != nil {
        log.Fatalf("Failed to connect to RabbitMQ: %s", err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatalf("Failed to open a channel: %s", err)
    }
    defer ch.Close()

    consumeLogs(ch, logQueueName, mongoClient, dbName, collectionName)

    log.Println("Monitoring & Logging Service running...")
    select {}
}

func consumeLogs(ch *amqp091.Channel, queueName string, mongoClient *mongo.Client, dbName, collectionName string) {
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

    collection := mongoClient.Database(dbName).Collection(collectionName)

    go func() {
        for d := range msgs {
            logMsg := bson.M{
                "timestamp": time.Now().Format(time.RFC3339),
                "message":   string(d.Body),
            }

            // Insert log message into MongoDB
            _, err := collection.InsertOne(context.Background(), logMsg)
            if err != nil {
                log.Printf("%s", err)
            } else {
                log.Printf("%s", d.Body)
            }
        }
    }()
}
