package handlers

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "product-service/models"
    "product-service/service"
    "product-service/utils"    

    "go.mongodb.org/mongo-driver/mongo"
    "github.com/rabbitmq/amqp091-go"
)

var ch *amqp091.Channel

func InitRabbitMQ(channel *amqp091.Channel) {
    ch = channel
}

func InsertProduct(w http.ResponseWriter, r *http.Request) {
    var product models.Product
    if err := json.NewDecoder(r.Body).Decode(&product); err != nil {
        publishToQueue("logging_queue", err.Error())
        utils.RespondWithError(w, http.StatusBadRequest, "Invalid request payload")
        return
    }
    defer r.Body.Close()

    // Insert Master Product to MongoDB
    if err := service.InsertProduct(product); err != nil {
        publishToQueue("product_dlq", product)
        utils.RespondWithError(w, http.StatusInternalServerError, err.Error())
        return
    }

    // Call service function to update the product via RabbitMQ
    if err := service.PublishInsertProduct(product); err != nil {       
        utils.RespondWithError(w, http.StatusInternalServerError, "Failed to publish insert product message")
        return
    }

    // Respond with JSON response indicating successful update
    utils.RespondWithJSON(w, http.StatusCreated, product)
}

func UpdateProduct(w http.ResponseWriter, r *http.Request) {
    var product models.Product

    // Decode JSON payload from request body into models.Product struct
    if err := json.NewDecoder(r.Body).Decode(&product); err != nil {
        publishToQueue("logging_queue", err.Error())
        log.Printf("Error decoding JSON: %v", err)
        utils.RespondWithError(w, http.StatusBadRequest, "Invalid request payload")
        return
    }
    defer r.Body.Close()

    // Update Master Product to MongoDB
    if err := service.UpdateProduct(product); err != nil {
        publishToQueue("product_dlq", product)
        utils.RespondWithError(w, http.StatusInternalServerError, err.Error())
        return
    }

    // Call service function to update the product via RabbitMQ
    if err := service.PublishUpdateProduct(product); err != nil {        
        utils.RespondWithError(w, http.StatusInternalServerError, "Failed to publish update product message")
        return
    }

    // Respond with JSON response indicating successful update
    utils.RespondWithJSON(w, http.StatusOK, product)
}

func SelectProduct(w http.ResponseWriter, r *http.Request) {
    name := r.URL.Query().Get("name")
    product, err := service.SelectProduct(name)
    if err != nil {
        if err == mongo.ErrNoDocuments {
            utils.RespondWithError(w, http.StatusNotFound, "Product not found")
        } else {
            publishToQueue("logging_queue", err.Error())
            utils.RespondWithError(w, http.StatusInternalServerError, err.Error())
        }
        return
    }
    utils.RespondWithJSON(w, http.StatusOK, product)
}

func DeleteProduct(w http.ResponseWriter, r *http.Request) {
    name := r.URL.Query().Get("name")
    fmt.Println("name ", name)
    if err := service.DeleteProduct(name); err != nil {
        publishToQueue("logging_queue", err.Error())
        utils.RespondWithError(w, http.StatusInternalServerError, err.Error())
        return
    }
    utils.RespondWithJSON(w, http.StatusOK, map[string]string{"result": "success"})
}

func publishToQueue(queueName string, message interface{}) {
    body, err := json.Marshal(message)
    if err != nil {
        log.Printf("Failed to marshal message: %s", err)
        return
    }

    err = ch.Publish(
        "error_exchange",        // exchange
        queueName, // routing key (queue name)
        false,     // mandatory
        false,     // immediate
        amqp091.Publishing{
            ContentType: "application/json",
            Body:        body,
        })
    if err != nil {
        log.Printf("Failed to publish message to queue %s: %s", queueName, err)
    }
}
