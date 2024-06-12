package service

import (
	"encoding/json"
	"log"
	"errors"
	"product-service/models"
	"product-service/repository"

	"github.com/rabbitmq/amqp091-go"
)

var rabbitMQChannel *amqp091.Channel

func InitRabbitMQ(channel *amqp091.Channel) {
	rabbitMQChannel = channel
}

func publishToRabbitMQ(routingKey string, body []byte) error {
	if rabbitMQChannel == nil {
		return errors.New("RabbitMQ channel is not initialized")

	}

	err := rabbitMQChannel.Publish(
		"product_exchange", // exchange
		routingKey,         // routing key
		false,              // mandatory
		false,              // immediate
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		log.Printf("Failed to publish message: %v", err)
		return err
	}
	log.Printf(" [x] Sent %s: %s", routingKey, body)
	return nil
}

func PublishInsertProduct(product models.Product) error {
	productJSON, err := json.Marshal(product)
	if err != nil {
		return err
	}
	return publishToRabbitMQ("product.insert", productJSON)
}

func PublishUpdateProduct(product models.Product) error {
	productJSON, err := json.Marshal(product)
	if err != nil {
		return err
	}
	return publishToRabbitMQ("product.update", productJSON)
}

func DeleteProduct(name string) error {
	// Directly delete product from MongoDB
	return repository.DeleteProduct(name)
}

func SelectProduct(name string) (models.Product, error) {
	// Directly select product from MongoDB
	return repository.SelectProduct(name)
}
