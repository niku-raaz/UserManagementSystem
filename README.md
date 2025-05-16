# User Management System with PostgreSQL, Redis, Kafka, and Prometheus

This Go-based web application provides a user management system with the following features:

* CRUD operations for users
* Caching with Redis
* Event streaming and reading with Kafka
* Monitoring with Prometheus

## Features

### Technologies Used:

* **Go** (Golang)
* **PostgreSQL** for database
* **Redis** for caching
* **Kafka** for streaming events
* **Prometheus** for monitoring
* **Gorilla Mux** for routing

### API Endpoints

| Endpoint             | Method | Description                |
| -------------------- | ------ | -------------------------- |
| `/users`             | POST   | Create a new user          |
| `/users/:id`         | GET    | Get a user by ID           |
| `/users/:id`         | PUT    | Update user by ID          |
| `/users/:id`         | DELETE | Delete user by ID          |
| `/users/{id}/status` | PUT    | Mark a user as inactive    |
| `/users`             | GET    | Fetch all users            |
| `/metrics`           | GET    | Prometheus metrics         |
| `/metric`            | GET    | Show hit counters (custom) |

## Getting Started

### Prerequisites

* Go installed
* PostgreSQL server running
* Redis server running
* Kafka broker running

### Setup

1. **Clone the repository**

   ```bash
   git clone <repo-url>
   cd UserManagement
   ```

2. **Update PostgreSQL connection string**
   In `producer.go`, update the following line with your credentials:

   ```go
   conStr := "postgres://YOURUSERNAME:YOURPASSWORD@localhost:5432/USERDB?sslmode=disable"
   ```

3. **Run Kafka locally**
   Make sure Kafka is running at `localhost:9092`.

4. **Run Redis**
   Ensure Redis is running at `localhost:6379`.

5. **Run the application**

   ```bash
   go run producer.go
   ```

## Kafka Usage

The app produces user creation events to Kafka under the topic `user-events`.

## Redis Usage

* Users are cached in Redis after retrieval from the database.
* On update/delete, the corresponding Redis entry is invalidated.

## Prometheus Monitoring

* The app exposes metrics at `/metrics`
* Tracks:

  * Number of API hits (`api_hits_total`)
  * Latency of each API call (`api_latency_seconds`)
  * Number of server errors (`api_errors_total`)


## Author

* Built by Niku Raj as part of a full-stack distributed systems project.
