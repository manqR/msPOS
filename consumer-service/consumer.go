package main

import (
	"context"
	"encoding/json"
	"log"
	"time"
	"database/sql"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"consumer-service/models"
	_ "github.com/go-sql-driver/mysql"
)

var rabbitMQChannel *amqp091.Channel
var mongoClient *mongo.Client
var db *sql.DB

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func publishToRabbitMQ(routingKey string, message string) {
	err := rabbitMQChannel.Publish(
		"product_exchange", // exchange
		routingKey,         // routing key
		false,              // mandatory
		false,              // immediate
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        []byte(message),
		})
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}
	log.Printf(" [x] Sent %s: %s", routingKey, message)
}

func ConsumeRabbitMQMessages() {
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	rabbitMQChannel, err = conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer rabbitMQChannel.Close()

	queues := []string{"product_insert_queue", "product_update_queue"}
	for _, queue := range queues {
		msgs, err := rabbitMQChannel.Consume(
			queue,
			"",
			true,
			false,
			false,
			false,
			nil,
		)
		failOnError(err, "Failed to register a consumer")

		go processMessages(msgs, queue)
	}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	select {}
}

func processMessages(msgs <-chan amqp091.Delivery, queueName string) {
	for d := range msgs {
		log.Printf("Received a message from %s: %s", queueName, d.Body)

		var product models.Product
		err := json.Unmarshal(d.Body, &product)
		if err != nil {
			log.Printf("Error decoding JSON: %v", err)
			publishToLoggingQueue(fmt.Sprintf("Error decoding JSON from %s: %v", queueName, err))
			continue
		}

		switch queueName {
		case "product_insert_queue":
			err = insertProductMysql(product)
		case "product_update_queue":
			err = updateProductMysql(product)
		default:
			log.Printf("Unsupported queue: %s", queueName)
			publishToLoggingQueue(fmt.Sprintf("Unsupported queue: %s", queueName))
			continue
		}

		if err != nil {
            log.Printf("Failed to process message from %s: %v", queueName, err)
            publishToDeadLetterQueue(d.Body, err)
			publishToLoggingQueue(fmt.Sprintf("Failed to process message from %s: %v", queueName, err))
        }
	}
}

//Publish to Product_dlq
func publishToDeadLetterQueue(message []byte, err error) {
    errMsg := fmt.Sprintf("Failed to process message: %v. Error: %v", string(message), err)
    log.Printf(errMsg)
    
    err = rabbitMQChannel.Publish(
        "error_exchange",   // Dead-letter exchange
        "product.dlq",      // Dead-letter routing key
        false,              // mandatory
        false,              // immediate
        amqp091.Publishing{
            ContentType: "application/json",
            Body:        message,
        })
    if err != nil {
        log.Fatalf("Failed to publish to dead-letter queue: %v", err)
    }
    log.Printf(" [x] Sent to dead-letter queue: %s", message)
}

//Publish to logging_queue
func publishToLoggingQueue(message string) {
   
    err := rabbitMQChannel.Publish(
        "error_exchange",   // Dead-letter exchange
        "logging.error",      // Dead-letter routing key
        false,              // mandatory
        false,              // immediate
        amqp091.Publishing{
			ContentType: "text/plain",
            Body:        []byte(message),
        })
    if err != nil {
        log.Fatalf("Failed to publish to Logging queue: %v", err)
    }
    log.Printf(" [x] Sent to Logging queue: %s", message)
}

// UpdateProduct updates a product in the MySQL database
func updateProductMysql(product models.Product) error {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    query := "UPDATE product SET productName = ? WHERE productId = ?"
    _, err := db.ExecContext(ctx, query, product.Name, product.ItemCode)
    if err != nil {
        return fmt.Errorf("could not update product: %v", err)
    }

    log.Printf("Updated product in MySQL: %+v", product)
	publishToLoggingQueue(fmt.Sprintf("Updated product in MySQL: %+v", product))
    return nil
}

// InsertProduct inserts a product into the MySQL database
func insertProductMysql(product models.Product) error {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    query := "INSERT INTO product (productId, productName) VALUES (?, ?)"
    _, err := db.ExecContext(ctx, query, product.ItemCode, product.Name)
    if err != nil {		
        return fmt.Errorf("could not insert product: %v", err)
    }

	publishToLoggingQueue(fmt.Sprintf("Inserted product into MySQL: %+v", product))
    log.Printf("Inserted product into MySQL: %+v", product)
    return nil
}


// Initialize the database connection
func initDB() {
    dsn := "root:1q2w3e4r5t@tcp(127.0.0.1:3306)/order-service"
    var err error
    db, err = sql.Open("mysql", dsn)
    if err != nil {
        log.Fatal(err)
    }

    if err := db.Ping(); err != nil {
        log.Fatal(err)
    }
}


func main() {
	var err error
	mongoClient, err = mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	failOnError(err, "Failed to create MongoDB client")

	err = mongoClient.Connect(context.Background())
	failOnError(err, "Failed to connect to MongoDB")
	defer mongoClient.Disconnect(context.Background())

	initDB() // Initialize the MySQL database connection
	ConsumeRabbitMQMessages()
}
