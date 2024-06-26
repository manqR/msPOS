package main

import (
    "encoding/json"
    "log"
    "net/http"

    "github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
    ReadBufferSize:  1024,
    WriteBufferSize: 1024,
    CheckOrigin: func(r *http.Request) bool {
        return true
    },
}

func websocketHandler(w http.ResponseWriter, r *http.Request) {
    // Upgrade HTTP connection to WebSocket
    conn, err := upgrader.Upgrade(w, r, nil)
    if err != nil {
        log.Println("Upgrade:", err)
        return
    }
    defer conn.Close()

    for {
        // Read message from WebSocket client
        _, message, err := conn.ReadMessage() // Removed messageType variable as it's not needed
        if err != nil {
            log.Println("Read:", err)
            break
        }

        // Handle message
        var msg map[string]interface{}
        if err := json.Unmarshal(message, &msg); err != nil {
            log.Println("JSON unmarshal:", err)
            continue
        }

        action, ok := msg["action"].(string)
        if !ok {
            log.Println("Action is not a string")
            continue
        }

        // Handle insert or update action
        switch action {
        case "insert":
            // Simulate backend processing (replace with actual logic)
            product := msg["product"].(map[string]interface{})
            log.Println("Inserting product:", product)

            // Simulate success or failure
            success := true // Replace with actual success condition
            if success {
                sendMessage(conn, "success", "Product inserted successfully")
            } else {
                sendMessage(conn, "error", "Failed to insert product")
            }

        case "update":
            // Simulate backend processing (replace with actual logic)
            updatedProduct := msg["product"].(map[string]interface{})
            log.Println("Updating product:", updatedProduct)

            // Simulate success or failure
            success := true // Replace with actual success condition
            if success {
                sendMessage(conn, "success", "Product updated successfully")
            } else {
                sendMessage(conn, "error", "Failed to update product")
            }
        }
    }
}

func sendMessage(conn *websocket.Conn, messageType string, message string) {
    // Send message back to WebSocket client
    msg := map[string]string{
        "type":  messageType,
        "error": message,
    }
    if err := conn.WriteJSON(msg); err != nil {
        log.Println("WriteJSON:", err)
    }
}

func main() {
    // Serve static files (frontend.html)
    http.Handle("/", http.FileServer(http.Dir(".")))

    // WebSocket handler
    http.HandleFunc("/ws", websocketHandler)

    // Start server
    log.Println("Server running on http://localhost:3000")
    log.Fatal(http.ListenAndServe(":3000", nil))
}
