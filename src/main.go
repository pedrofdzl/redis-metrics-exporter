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
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
)

// Represents configuration for a single Redis instance
type RedisInstanceConfig struct {
	Name     string
	Addr     string
	DB       int
	Password string
}

// Represents a queue to be monitored on a specific instance
type MonitoredQueue struct {
	InstanceName string
	QueueName    string
}

// Represents a stream and group to be monitored on a specific instance
type MonitoredStream struct {
	InstanceName string
	StreamName   string
	GroupName    string
}

type Config struct {
	// Map of instance name to its configuration
	Instances  map[string]RedisInstanceConfig
	Queues     []MonitoredQueue
	Streams    []MonitoredStream
	ListenAddr string
}

func loadConfig() (*Config, error) {
	if err := godotenv.Load(".env"); err != nil {
		log.Println("Warning: .env not found, using environment variables")
	}

	redisHosts := parseCSV(getEnv("REDIS_HOSTS", "localhost"))
	redisPorts := parseCSV(getEnv("REDIS_PORTS", "6379"))
	redisDBs := parseCSV(getEnv("REDIS_DBS", "0"))

	rawPasswords := os.Getenv("REDIS_PASSWORDS")
	passwordParts := strings.Split(rawPasswords, ",")
	redisPasswords := make([]string, len(passwordParts))
	for i, p := range passwordParts {
		redisPasswords[i] = strings.TrimSpace(p)
	}

	if len(redisHosts) == 0 {
		return nil, fmt.Errorf("REDIS_HOSTS must be set")
	}
	if len(redisPorts) != len(redisHosts) || len(redisDBs) != len(redisHosts) || len(redisPasswords) != len(redisHosts) {
		return nil, fmt.Errorf("REDIS_HOSTS, REDIS_PORTS, REDIS_DBS, REDIS_PASSWORDS must have the same number of comma-separated values (hosts=%d, ports=%d, dbs=%d, passwords=%d)",
			len(redisHosts), len(redisPorts), len(redisDBs), len(redisPasswords))
	}

	instances := make(map[string]RedisInstanceConfig)
	for i, host := range redisHosts {
		host = strings.TrimSpace(host)
		if host == "" {
			return nil, fmt.Errorf("invalid empty host name at index %d in REDIS_HOSTS", i)
		}
		if _, exists := instances[host]; exists {
			return nil, fmt.Errorf("duplicate host name %q found in REDIS_HOSTS", host)
		}

		port := strings.TrimSpace(redisPorts[i])
		dbStr := strings.TrimSpace(redisDBs[i])
		password := redisPasswords[i]

		db, err := strconv.Atoi(dbStr)
		if err != nil {
			return nil, fmt.Errorf("invalid REDIS_DB value %q for host %q: %v", dbStr, host, err)
		}

		instances[host] = RedisInstanceConfig{
			Name:     host,
			Addr:     host + ":" + port,
			Password: password,
			DB:       db,
		}
	}

	queuesRaw := parseCSV(os.Getenv("MONITOR_QUEUES"))
	queues := make([]MonitoredQueue, 0, len(queuesRaw))
	for _, q := range queuesRaw {
		parts := strings.SplitN(q, ";", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid MONITOR_QUEUES entry %q, use format 'instance;queue'", q)
		}
		instanceName := strings.TrimSpace(parts[0])
		queueName := strings.TrimSpace(parts[1])
		if _, ok := instances[instanceName]; !ok {
			return nil, fmt.Errorf("unknown redis instance %q specified in MONITOR_QUEUES entry %q", instanceName, q)
		}
		queues = append(queues, MonitoredQueue{InstanceName: instanceName, QueueName: queueName})
	}

	streamsRaw := parseCSV(os.Getenv("MONITOR_STREAMS"))
	streams := make([]MonitoredStream, 0, len(streamsRaw))
	for _, s := range streamsRaw {
		parts := strings.SplitN(s, ";", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid MONITOR_STREAMS entry %q, use format 'instance;stream:group'", s)
		}
		instanceName := strings.TrimSpace(parts[0])
		streamGroup := strings.TrimSpace(parts[1])

		sgParts := strings.SplitN(streamGroup, ":", 2)
		if len(sgParts) != 2 || sgParts[0] == "" || sgParts[1] == "" {
			return nil, fmt.Errorf("invalid stream:group format %q in MONITOR_STREAMS entry %q", streamGroup, s)
		}
		streamName := strings.TrimSpace(sgParts[0])
		groupName := strings.TrimSpace(sgParts[1])

		if _, ok := instances[instanceName]; !ok {
			return nil, fmt.Errorf("unknown redis instance %q specified in MONITOR_STREAMS entry %q", instanceName, s)
		}
		streams = append(streams, MonitoredStream{
			InstanceName: instanceName,
			StreamName:   streamName,
			GroupName:    groupName,
		})
	}

	listenAddr := getEnv("LISTEN_ADDR", ":9808")

	return &Config{
		Instances:  instances,
		Queues:     queues,
		Streams:    streams,
		ListenAddr: listenAddr,
	}, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func createRedisClients(cfg *Config) (map[string]*redis.Client, error) {
	clients := make(map[string]*redis.Client)
	var errs []string

	for name, instanceCfg := range cfg.Instances {
		client := redis.NewClient(&redis.Options{
			Addr:     instanceCfg.Addr,
			Password: instanceCfg.Password,
			DB:       instanceCfg.DB,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.Ping(ctx).Result()
		cancel()

		if err != nil {
			log.Printf("Failed to connect to Redis instance %q at %s: %v", name, instanceCfg.Addr, err)
			errs = append(errs, fmt.Sprintf("%s (%s): %v", name, instanceCfg.Addr, err))
			client.Close()
		} else {
			clients[name] = client
			log.Printf("Successfully connected to Redis instance %q at %s", name, instanceCfg.Addr)
		}
	}

	if len(clients) == 0 && len(cfg.Instances) > 0 {
		return nil, fmt.Errorf("failed to connect to any Redis instances. Errors: %s", strings.Join(errs, "; "))
	}

	if len(errs) > 0 {
		log.Printf("Warning: Failed to connect to some Redis instances: %s", strings.Join(errs, "; "))
	}

	return clients, nil
}

func metricsHandler(clients map[string]*redis.Client, cfg *Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		var wg sync.WaitGroup
		var mu sync.Mutex

		for _, mq := range cfg.Queues {
			client, ok := clients[mq.InstanceName]
			if !ok {
				log.Printf("Skipping queue %s on %s: client not connected", mq.QueueName, mq.InstanceName)
				continue
			}
			wg.Add(1)
			go func(q MonitoredQueue, c *redis.Client) {
				defer wg.Done()
				length, err := c.LLen(ctx, q.QueueName).Result()
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					log.Printf("Error getting length for queue %s on instance %s: %v", q.QueueName, q.InstanceName, err)
					return
				}
				fmt.Fprintf(w, "redis_queue_length{instance=\"%s\",queue=\"%s\"} %d\n", q.InstanceName, q.QueueName, length)
			}(mq, client)
		}

		for _, ms := range cfg.Streams {
			client, ok := clients[ms.InstanceName]
			if !ok {
				log.Printf("Skipping stream %s:%s on %s: client not connected", ms.StreamName, ms.GroupName, ms.InstanceName)
				continue
			}
			wg.Add(1)
			go func(s MonitoredStream, c *redis.Client) {
				defer wg.Done()
				summary, err := c.XPending(ctx, s.StreamName, s.GroupName).Result()
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					if redis.Nil != err && !strings.Contains(err.Error(), "NOGROUP") {
						log.Printf("XPENDING %s %s on instance %s error: %v", s.StreamName, s.GroupName, s.InstanceName, err)
					}
					return
				}
				fmt.Fprintf(
					w,
					"redis_stream_pending{instance=\"%s\",stream=\"%s\",group=\"%s\"} %d\n",
					s.InstanceName, s.StreamName, s.GroupName, summary.Count,
				)
			}(ms, client)
		}

		for instanceName, client := range clients {
			wg.Add(1)
			go func(name string, c *redis.Client) {
				defer wg.Done()
				info, err := c.Info(ctx, "memory").Result()
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					log.Printf("Error getting INFO for instance %s: %v", name, err)
					return
				}

				if strings.Contains(info, "used_memory:") {
					for _, line := range strings.Split(info, "\r\n") {
						if strings.HasPrefix(line, "used_memory:") {
							parts := strings.Split(line, ":")
							if len(parts) == 2 {
								memory, err := strconv.ParseInt(parts[1], 10, 64)
								if err == nil {
									fmt.Fprintf(w, "redis_memory_used_bytes{instance=\"%s\"} %d\n", name, memory)
								}
							}
							break
						}
					}
				}
			}(instanceName, client)
		}

		wg.Wait()
	}
}

func healthHandler(clients map[string]*redis.Client, cfg *Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		var unhealthyInstances []string
		var wg sync.WaitGroup
		var mu sync.Mutex

		for name := range cfg.Instances {
			client, isConnected := clients[name]

			if isConnected {
				wg.Add(1)
				go func(instanceName string, c *redis.Client) {
					defer wg.Done()
					_, err := c.Ping(ctx).Result()
					if err != nil {
						mu.Lock()
						unhealthyInstances = append(unhealthyInstances, fmt.Sprintf("%s (ping failed: %v)", instanceName, err))
						mu.Unlock()
					}
				}(name, client)
			} else {
				mu.Lock()
				unhealthyInstances = append(unhealthyInstances, fmt.Sprintf("%s (initial connection failed)", name))
				mu.Unlock()
			}
		}

		wg.Wait()

		if len(unhealthyInstances) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "Redis connection failed for instances: %s", strings.Join(unhealthyInstances, ", "))
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

	clients, err := createRedisClients(cfg)
	if err != nil {
		if len(clients) == 0 {
			log.Fatalf("Failed to establish initial connection to any Redis instance: %v", err)
		} else {
			log.Printf("Warning: Proceeding with partially connected Redis instances: %v", err)
		}
	}

	defer func() {
		log.Println("Closing Redis connections...")
		for name, client := range clients {
			if err := client.Close(); err != nil {
				log.Printf("Error closing connection to instance %s: %v", name, err)
			}
		}
		log.Println("Finished closing Redis connections.")
	}()

	log.Printf("Monitoring %d Redis instances.", len(cfg.Instances))
	if len(cfg.Queues) > 0 {
		queueStrs := make([]string, len(cfg.Queues))
		for i, q := range cfg.Queues {
			queueStrs[i] = fmt.Sprintf("%s;%s", q.InstanceName, q.QueueName)
		}
		log.Printf("Monitoring queues: %s", strings.Join(queueStrs, ", "))
	} else {
		log.Println("Monitoring queues: None")
	}

	if len(cfg.Streams) > 0 {
		streamStrs := make([]string, len(cfg.Streams))
		for i, s := range cfg.Streams {
			streamStrs[i] = fmt.Sprintf("%s;%s:%s", s.InstanceName, s.StreamName, s.GroupName)
		}
		log.Printf("Monitoring streams: %s", strings.Join(streamStrs, ", "))
	} else {
		log.Println("Monitoring streams: None")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", metricsHandler(clients, cfg))
	mux.HandleFunc("/health", healthHandler(clients, cfg))

	server := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		log.Println("Shutdown signal received, shutting down gracefully...")

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

// Helpers

func parseCSV(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}
