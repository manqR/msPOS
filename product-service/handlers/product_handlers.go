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
)



func InsertProduct(w http.ResponseWriter, r *http.Request) {
	var product models.Product
	if err := json.NewDecoder(r.Body).Decode(&product); err != nil {
		utils.RespondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}
	defer r.Body.Close()

	if err := service.InsertProduct(product); err != nil {
        utils.RespondWithError(w, http.StatusInternalServerError, err.Error())
        return
    }

	if err := service.PublishInsertProduct(product); err != nil {
		utils.RespondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}
	utils.RespondWithJSON(w, http.StatusCreated, product)
}


func SelectProduct(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	product, err := service.SelectProduct(name)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			utils.RespondWithError(w, http.StatusNotFound, "Product not found")
		} else {
			utils.RespondWithError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	utils.RespondWithJSON(w, http.StatusOK, product)
}

func UpdateProduct(w http.ResponseWriter, r *http.Request) {
	var product models.Product

	// Decode JSON payload from request body into models.Product struct
	if err := json.NewDecoder(r.Body).Decode(&product); err != nil {
		log.Printf("Error decoding JSON: %v", err)
		utils.RespondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}
	defer r.Body.Close()

	log.Printf("Received product update request: %+v", product)

	// Call service function to update the product via RabbitMQ
	if err := service.PublishUpdateProduct(product); err != nil {
		log.Printf("Error updating product via RabbitMQ: %v", err)
		utils.RespondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Respond with JSON response indicating successful update
	utils.RespondWithJSON(w, http.StatusOK, product)
}

func DeleteProduct(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
    fmt.Println("name ",name)
	if err := service.DeleteProduct(name); err != nil {
		utils.RespondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}
	utils.RespondWithJSON(w, http.StatusOK, map[string]string{"result": "success"})
}
