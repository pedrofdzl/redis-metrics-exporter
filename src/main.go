package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
)

type Config struct {
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	Queues        []string
	ListenAddr    string
}

func loadConfig() (*Config, error) {
	if err := godotenv.Load(".env"); err != nil {
		log.Println("Warning: .env not found, using environment variables")
	}

	redisHost := getEnv("REDIS_HOST", "localhost")
	redisPort := getEnv("REDIS_PORT", "6379")
	redisAddr := redisHost + ":" + redisPort
	redisPassword := getEnv("REDIS_PASSWORD", "")

	redisDB := 0
	if dbStr := os.Getenv("REDIS_DB"); dbStr != "" {
		var err error
		redisDB, err = strconv.Atoi(dbStr)
		if err != nil {
			return nil, fmt.Errorf("invalid REDIS_DB value: %v", err)
		}
	}

	queuesStr := os.Getenv("MONITOR_QUEUES")
	if queuesStr == "" {
		return nil, fmt.Errorf("MONITOR_QUEUES environment variable is required")
	}

	queues := []string{}
	for _, q := range strings.Split(queuesStr, ",") {
		queue := strings.TrimSpace(q)
		if queue != "" {
			queues = append(queues, queue)
		}
	}

	if len(queues) == 0 {
		return nil, fmt.Errorf("at least one queue must be specified in MONITOR_QUEUES")
	}

	listenAddr := getEnv("LISTEN_ADDR", ":9808")

	return &Config{
		RedisAddr:     redisAddr,
		RedisPassword: redisPassword,
		RedisDB:       redisDB,
		Queues:        queues,
		ListenAddr:    listenAddr,
	}, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func createRedisClient(cfg *Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	return client, nil
}

func metricsHandler(client *redis.Client, queues []string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")

		for _, queue := range queues {
			length, err := client.LLen(ctx, queue).Result()
			if err != nil {
				log.Printf("Error getting length for queue %s: %v", queue, err)
				continue
			}
			fmt.Fprintf(w, "redis_queue_length{queue=\"%s\"} %d\n", queue, length)
		}

		info, err := client.Info(ctx).Result()
		if err == nil {
			if strings.Contains(info, "used_memory:") {
				for _, line := range strings.Split(info, "\r\n") {
					if strings.HasPrefix(line, "used_memory:") {
						parts := strings.Split(line, ":")
						if len(parts) == 2 {
							memory, err := strconv.ParseInt(parts[1], 10, 64)
							if err == nil {
								fmt.Fprintf(w, "redis_memory_used_bytes %d\n", memory)
							}
						}
					}
				}
			}
		}
	}
}

func healthHandler(client *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		_, err := client.Ping(ctx).Result()
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "Redis connection failed: %v", err)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "OK")
	}
}

func main() {
	log.Println("Starting Redis Metrics Exporter")
	
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	client, err := createRedisClient(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer client.Close()

	log.Printf("Connected to Redis at %s", cfg.RedisAddr)
	log.Printf("Monitoring queues: %s", strings.Join(cfg.Queues, ", "))

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", metricsHandler(client, cfg.Queues))
	mux.HandleFunc("/health", healthHandler(client))

	server := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		log.Println("Shutdown signal received, shutting down gracefully...")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Server shutdown error: %v", err)
		}
	}()

	log.Printf("Starting server on %s", cfg.ListenAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed: %v", err)
	}

	log.Println("Server shutdown complete")
}
