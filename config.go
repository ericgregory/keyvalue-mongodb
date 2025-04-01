package main

import (
	"errors"
	"log"
)

type MongoConfigArgs struct {
	Name     string
	URI      string
	Database string
	KV       string
}

// Create MongoDB connection args from config
func validateMongoConfig(config map[string]string, name string) (MongoConfigArgs, error) {
	log.Println("Handling MongoDB configuration")
	configArgs := MongoConfigArgs{}

	// Get config name (for logging purposes) from target link config
	if name == "" {
		return configArgs, errors.New("no config name found")
	} else {
		configArgs.Name = name
		globalLinkName = name
	}

	// Get URI from target link config
	if uri, ok := config["uri"]; !ok || uri == "" {
		return configArgs, errors.New("URI configuration is required - include URI in link configuration or secret (preferred)")
	} else {
		configArgs.URI = uri
		globalUri = configArgs.URI
		log.Printf("Using MongoDB URI configuration from link: %s. (Consider using a secret for sensitive configuration values.)", globalLinkName)
	}

	// Get database from target link config
	if database, ok := config["database"]; !ok || database == "" {
		return configArgs, errors.New("database config is required")
	} else {
		configArgs.Database = database
		globalDb = configArgs.Database
		log.Printf("Using MongoDB database configuration from link: %s. (Consider using a secret for sensitive configuration values.)", globalLinkName)
	}

	// Get optional key-value group value from target link config
	if kv, ok := config["kv"]; !ok || kv == "" {
		log.Println("No key-value group supplied, using default value: kv")
		configArgs.KV = "kv"
	} else {
		configArgs.KV = kv
		globalKv = configArgs.KV
		log.Printf("Using custom key-value group configuration from link: %s", globalLinkName)
	}

	return configArgs, nil
}
