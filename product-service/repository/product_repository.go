package repository

import (
	"context"
	"fmt"
	"log"
	"product-service/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var client *mongo.Client

// InitMongoClient initializes the MongoDB client
func InitMongoClient() error {
    // Set client options
    clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

    // Connect to MongoDB
    var err error
    client, err = mongo.Connect(context.Background(), clientOptions)
    if err != nil {
        return err
    }

    // Check the connection
    err = client.Ping(context.Background(), nil)
    if err != nil {
        return err
    }

    log.Println("Connected to MongoDB!")
    return nil
}


func InsertProduct(product models.Product) error {
    collection := client.Database("product").Collection("product_collection")
    _, err := collection.InsertOne(context.TODO(), product)
    return err
}

func SelectProduct(name string) (models.Product, error) {
    var product models.Product
    collection := client.Database("product").Collection("product_collection")
    err := collection.FindOne(context.TODO(), bson.D{{"itemcode", name}}).Decode(&product)
    return product, err
}

func UpdateProduct(product models.Product) error {
    collection := client.Database("product").Collection("product_collection")
    filter := bson.D{{"itemcode", product.ItemCode}}
    update := bson.D{{"$set", product}}
    _, err := collection.UpdateOne(context.TODO(), filter, update)
    return err
}

func DeleteProduct(name string) error {
    collection := client.Database("product").Collection("product_collection")
	fmt.Print("name ",name)
    _, err := collection.DeleteOne(context.TODO(), bson.D{{"itemcode", name}})
    return err
}
