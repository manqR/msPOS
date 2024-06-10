package models

type Product struct {
    ItemCode  string  `json:"itemcode" bson:"itemcode"`
    Price     float64 `json:"price" bson:"price"`
    Category  string  `json:"category" bson:"category"`
    Jenis     string  `json:"jenis" bson:"jenis"`
}
