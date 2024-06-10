package service

import "msproject/product-service/models"
import "msproject/product-service/repository"

func InsertProduct(product models.Product) error {
    // Business logic (if any)
    return repository.InsertProduct(product)
}

func SelectProduct(itemcode string) (models.Product, error) {
    // Business logic (if any)
    return repository.SelectProduct(itemcode)
}

func UpdateProduct(product models.Product) error {
    // Business logic (if any)
    return repository.UpdateProduct(product)
}

func DeleteProduct(itemcode string) error {
    // Business logic (if any)
    return repository.DeleteProduct(itemcode)
}
