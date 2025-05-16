package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
)

type User struct {
	Id        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
	Status    bool      `json:"status"`
}

var (
	create   int
	getId    int
	update   int
	deleteId int
	getAll   int
)

const counterFile = "counter.txt"

func readCounter() int {
	data, err := ioutil.ReadFile(counterFile)
	if err != nil {
		// File doesn't exist yet
		return 0
	}
	counter, err := strconv.Atoi(string(data))
	if err != nil {
		return 0
	}
	return counter
}

func writeCounter(counter int) {
	ioutil.WriteFile(counterFile, []byte(strconv.Itoa(counter)), 0644)
}

var db *sql.DB

var ctx = context.Background()

var rdb *redis.Client

var p *kafka.Producer

var err2 error

var counter int

var apiHits = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "api_hits_total",
		Help: "Number of hits to API endpoints",
	},
	[]string{"/users"},
)

var apiLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "api_latency_seconds",
		Help:    "Latency of API requests",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"/users"},
)

var apiErrors = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "api_errors_total",
		Help: "Number of API errors (status code >= 500)",
	},
	[]string{"/users"},
)

func init() {
	prometheus.MustRegister(apiHits)
	prometheus.MustRegister(apiLatency)
	prometheus.MustRegister(apiErrors)
}

type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

func main() {

	counter = readCounter()
	writeCounter(counter)

	// Database name : USERDB
	// TableName: usertable
	conStr := "postgres://YOURUSERNAME:YOURPASSWORD@localhost:5432/USERDB?sslmode=disable"
	var err error
	db, err = sql.Open("postgres", conStr)
	if err = db.Ping(); err != nil {

		log.Fatal("Cannot connect to database:", err)
	}
	fmt.Println("Connected to Postgresql database")

	query := `CREATE TABLE IF NOT EXISTS usertable (id UUID PRIMARY KEY,name TEXT,email TEXT,createdAT TIMESTAMP,status BOOLEAN);`

	_, err = db.Exec(query)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	defer db.Close()

	// Connection with Redis

	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Redis address
		Password: "",
		DB:       0, // Using Database 0
	})

	// Ping to check connection

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		fmt.Println("Error connecting to redis", err.Error())
	}
	fmt.Println("Connected to Redis:", pong)

	defer rdb.Close()

	p, err2 = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"}) // localhost:9092
	if err2 != nil {
		log.Fatal("Kafka producer error:", err2)
	}
	defer p.Close()

	router := mux.NewRouter()

	
	router.HandleFunc("/users", createUser).Methods("POST")
	router.HandleFunc("/users/:id", getUser).Methods("GET")
	router.HandleFunc("/users/:id", updateUser).Methods("PUT")
	router.HandleFunc("/users/:id", deleteUser).Methods("DELETE")
	router.HandleFunc("/users/{id}/status", markUserInactive).Methods("PUT")
	router.HandleFunc("/users", getAllUser).Methods("GET")
	router.HandleFunc("/metric", metricHandle).Methods("GET")
	router.Use(prometheusMiddleware)
	router.Handle("/metrics", promhttp.Handler())

	log.Println("Server is running on port 8080")
	log.Fatal(http.ListenAndServe(":8080", router))

}

func prometheusMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := mux.CurrentRoute(r)
		path, _ := route.GetPathTemplate()
		timer := prometheus.NewTimer(apiLatency.WithLabelValues(path))
		defer timer.ObserveDuration()

		srw := &statusResponseWriter{ResponseWriter: w, statusCode: 200}
		next.ServeHTTP(srw, r)

		apiHits.WithLabelValues(path).Inc()
		if srw.statusCode >= 500 {
			apiErrors.WithLabelValues(path).Inc()
		}
	})
}

func insertDB(newUser User) {

	query := `INSERT INTO usertable (id, name, email, createdat, status) VALUES ($1, $2, $3, $4, $5)`
	rows, err := db.Exec(query, newUser.Id, newUser.Name, newUser.Email, newUser.CreatedAt, 1)

	if err != nil {
		log.Println("Error inserting into DB:", err)
		return
	}

	fmt.Println("User created: ", rows)

}

func createUser(w http.ResponseWriter, r *http.Request) {

	fmt.Println("Received POST/users request")

	create++
	counter = readCounter()
	counter++
	writeCounter(counter)

	id := uuid.New()
	currTime := time.Now()

	newUser := User{Id: id, CreatedAt: currTime}

	err := json.NewDecoder(r.Body).Decode(&newUser)
	if err != nil {
		log.Fatal(err)

	}

	insertDB(newUser)

	event := fmt.Sprintf(`{"id":"%s","name":"%s","email":"%s","createdAt":"%s","status":"%s"}`, newUser.Id.String(), newUser.Name, newUser.Email, newUser.CreatedAt.Format(time.RFC3339), strconv.FormatBool(newUser.Status))
	topic := "user-events"
	putKafka(event, topic, p)
}

func putKafka(event string, topic string, p *kafka.Producer) {
	var err error
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(event),
	}, nil)

	if err != nil {
		log.Fatal("Error producing message:", err)
	}

	fmt.Println("Kafka event produced!")

}

func addREDIS(newUser User) {

	myId := newUser.Id
	err3 := rdb.Set(ctx, myId.String(), newUser.Name, 0).Err()

	if err3 != nil {

		fmt.Println("Error inserting into Cache ")
		log.Fatal(err3)
	}

}

func searchREDIS(newUser User) bool {
	resp, _ := rdb.Get(ctx, newUser.Id.String()).Result()

	if resp == "" {
		fmt.Println("User not found in Cache, Searching in Database")
	} else {
		fmt.Println("User found in Cache")
		fmt.Println(resp)

		return true
	}

	return false

}

func searchDB(newUser User) bool {

	query := `SELECT name, email FROM usertable WHERE id = $1`
	row := db.QueryRow(query, newUser.Id.String())

	var user User
	err2 := row.Scan(&user.Name, &user.Email)

	if err2 != nil {
		if err2 == sql.ErrNoRows {
			fmt.Println("No user found in DB")
			return false
		}
		log.Fatal("Scan error:", err2)
	}
	fmt.Println("User Found in Database:: UserName is ", user.Name)

	return true

}

func updateDB(newUser User) {
	myId := newUser.Id
	query := `UPDATE usertable SET name = $1, email = $2 WHERE id = $3`
	_, err := db.Exec(query, newUser.Name, newUser.Email, myId)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("updated user name ", newUser.Name)

}

func deleteDB(newUser User) {
	myId := newUser.Id
	query := `DELETE FROM usertable WHERE id = $1`
	_, err := db.Exec(query, myId)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("deleted user")

}
func markDB(myUser User) {
	query := `UPDATE usertable SET status = $2 WHERE id = $1`
	_, err := db.Exec(query, myUser.Id, false)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("marked user inactive")

}

func deleteREDIS(newUser User) {
	// Delete from Redis
	myId := newUser.Id
	err2 := rdb.Del(ctx, myId.String()).Err()
	if err2 != nil {
		log.Fatal(err2)
	}
	println("Deleted from Cache")

}

func getUser(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received GET/users request")

	getId++
	counter = readCounter()
	counter++
	writeCounter(counter)

	newUser := User{}
	err := json.NewDecoder(r.Body).Decode(&newUser)
	if err != nil {
		log.Fatal(err)
	}

	found := searchREDIS(newUser)

	if found {
		return
	}

	found2 := searchDB(newUser)
	if !found2 {
		return

	}

	addREDIS(newUser)
}

func updateUser(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received UPDATE user request")
	update++
	counter = readCounter()
	counter++
	writeCounter(counter)
	newUser := User{}
	err := json.NewDecoder(r.Body).Decode(&newUser)
	if err != nil {
		log.Fatal(err)
	}

	updateDB(newUser)
	deleteREDIS(newUser)

}

func deleteUser(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received DELETE user request")
	deleteId++
	counter = readCounter()
	counter++
	writeCounter(counter)
	newUser := User{}

	err := json.NewDecoder(r.Body).Decode(&newUser)
	if err != nil {
		log.Fatal(err)
	}

	deleteDB(newUser)
	deleteREDIS(newUser)

}

func markUserInactive(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received UPDATE user request")
	counter = readCounter()
	counter++
	writeCounter(counter)

	myUser := User{}
	err := json.NewDecoder(r.Body).Decode(&myUser)
	if err != nil {
		log.Fatal(err)
	}

	markDB(myUser)

}

func getAllUser(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received GET users request")

	counter = readCounter()
	counter++
	writeCounter(counter)

	query := `SELECT id, name, email, createdat, status FROM usertable`
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)

	}
	defer rows.Close()
	var users []User

	for rows.Next() {
		var user User
		getId++
		err := rows.Scan(&user.Id, &user.Name, &user.Email, &user.CreatedAt, &user.Status)
		if err != nil {
			log.Fatal("Error scanning row:", err)
		}
		users = append(users, user)
	}

	for _, user := range users {
		fmt.Printf("ID: %s, Name: %s, Email: %s, CreatedAt: %s, Status: %v\n",
			user.Id, user.Name, user.Email, user.CreatedAt.Format(time.RFC3339), user.Status)
	}

}

func metricHandle(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Metrics are: ")
	counter = readCounter()
	fmt.Println(getId, create, getAll, update, deleteId, counter)

	writeCounter(counter)

}
