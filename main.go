package main

import (
    "log"
    "net/http"
    "github.com/gorilla/mux"
    "msproject/product-service/config"
    "msproject/product-service/handlers"
    "msproject/product-service/middleware"
    "msproject/product-service/repository"
)

func main() {
    config.LoadConfig() // Load configuration

    // Initialize MongoDB client
    err := repository.InitMongoClient()
    if err != nil {
        log.Fatalf("Error initializing MongoDB client: %v", err)
    }

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
