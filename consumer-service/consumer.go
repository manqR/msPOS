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
	"go.mongodb.org/mongo-driver/bson"

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
		_, err = rabbitMQChannel.QueueDeclare(
			queue, // name
			true,  // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		failOnError(err, "Failed to declare queue")

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
			continue
		}

		switch queueName {
		case "product_insert_queue":
			err = insertProductMysql(product)
		case "product_update_queue":
			err = updateProduct(product)
		default:
			log.Printf("Unsupported queue: %s", queueName)
			continue
		}

		if err != nil {
			log.Printf("Failed to process message from %s: %v", queueName, err)
		}
	}
}

// InsertProduct inserts a product into the MySQL database
func insertProductMysql(product models.Product) error {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    query := "INSERT INTO products (productId, productName) VALUES (?, ?)"
    _, err := db.ExecContext(ctx, query, product.ItemCode, product.Name)
    if err != nil {
        return fmt.Errorf("could not insert product: %v", err)
    }

    log.Printf("Inserted product into MySQL: %+v", product)
    return nil
}

func insertProduct(product models.Product) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	collection := mongoClient.Database("product").Collection("product_collection")
	_, err := collection.InsertOne(ctx, product)
	if err != nil {
		return err
	}
	log.Printf("Inserted product into MongoDB: %+v", product)
	return nil
}

func updateProduct(product models.Product) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.D{{"itemcode", product.ItemCode}}
	update := bson.D{{"$set", product}}

	collection := mongoClient.Database("product").Collection("product_collection")
	_, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}
	log.Printf("Updated product in MongoDB: %+v", product)
	return nil
}

// Initialize the database connection
func initDB() {
    dsn := "user:password@tcp(127.0.0.1:3306)/order-service"
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
	defer mongoClient.Disconnect(context.Background())

	err = mongoClient.Connect(context.Background())
	failOnError(err, "Failed to connect to MongoDB")
	defer mongoClient.Disconnect(context.Background())

	ConsumeRabbitMQMessages()
	initDB()
}
