package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/PhubetK/goWorkshop/app/models"
	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var client *mongo.Client

func connect() {
	var err error
	client, err = mongo.NewClient(options.Client().ApplyURI("mongodb+srv://bosbesp:Pbkrbs170245@cluster0.nr5ruuc.mongodb.net/?retryWrites=true&w=majority"))
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MongoDB Atlas!")
}

func createHandler(w http.ResponseWriter, r *http.Request) {
	var product models.Product
	json.NewDecoder(r.Body).Decode(&product)
	collection := client.Database("testdb").Collection("testcollection")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := collection.InsertOne(ctx, product)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(result)
}

func readHandler(w http.ResponseWriter, r *http.Request) {
	collection := client.Database("testdb").Collection("testcollection")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var results []models.Product
	if err = cursor.All(ctx, &results); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(results)
}

func updateHandler(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	var product models.Product
	json.NewDecoder(r.Body).Decode(&product)
	collection := client.Database("testdb").Collection("testcollection")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	filter := bson.M{"name": params["name"]}
	update := bson.M{"$set": product}
	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(result)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	collection := client.Database("testdb").Collection("testcollection")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	filter := bson.M{"name": params["name"]}
	result, err := collection.DeleteOne(ctx, filter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(result)
}

func main() {
	connect()

	router := mux.NewRouter()
	router.HandleFunc("/create", createHandler).Methods("POST")
	router.HandleFunc("/read", readHandler).Methods("GET")
	router.HandleFunc("/update/{name}", updateHandler).Methods("PUT")
	router.HandleFunc("/delete/{name}", deleteHandler).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8080", router))
}
