package config

import (
    "log"
    "github.com/joho/godotenv"
    "os"
)

func LoadConfig() {
    err := godotenv.Load()
    if err != nil {
        log.Fatalf("Error loading .env file: %v", err)
    }
}

func GetMongoURI() string {
    return os.Getenv("MONGO_URI")
}

func GetDBName() string {
    return os.Getenv("DB_NAME")
}

func GetProductCollectionName() string {
    return os.Getenv("PRODUCT_COLLECTION_NAME")
}
