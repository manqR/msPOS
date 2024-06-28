package main

import (
    "context"
    "log"
    "os"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"

    "github.com/rabbitmq/amqp091-go"

	"error-handler/config" 
)

const (
    rabbitMQURL  = "amqp://guest:guest@localhost:5672/"
    logQueueName = "product_dlq"
)

func main() {

	// Load environment variables
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
    productCollectionName := os.Getenv("PRODUCT_COLLECTION_NAME")

    if mongoURI == "" || dbName == "" || productCollectionName == "" {
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

    consumeDLQ(ch, logQueueName, mongoClient, dbName, productCollectionName)

    log.Println("Error Handler Service running...")
    select {}
}

func consumeDLQ(ch *amqp091.Channel, queueName string, mongoClient *mongo.Client, dbName,  productCollectionName string) {
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
    
    productCollection := mongoClient.Database(dbName).Collection(productCollectionName)

    go func() {
        for d := range msgs {
            log.Printf("Received a message from %s: %s", queueName, d.Body)

            // Assuming the message contains product ID and other details in JSON format
            var msg bson.M
            if err := bson.UnmarshalExtJSON(d.Body, true, &msg); err != nil {
                log.Printf("Failed to unmarshal message: %s", err)
                continue
            }

            // Insert log message into MongoDB          
			productID, ok := msg["itemcode"].(string)
			if !ok {
				log.Printf("Failed to extract itemcode from message: %s", d.Body)
				continue
			}

			// Delete product document from product collection
			filter := bson.M{"itemcode": productID}
			_, err = productCollection.DeleteOne(context.Background(), filter)
			if err != nil {
			  log.Printf("Failed to delete product from MongoDB: %s", err)
			} else {
			  log.Printf("Deleted product with itemcode %s from MongoDB", productID)
			}
        }
    }()
}
