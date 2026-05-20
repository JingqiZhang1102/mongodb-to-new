package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"github.com/gsbingo17/mongodb-migration/pkg/migration"
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "mongodb_replication_config.json", "Path to configuration file")
	mode := flag.String("mode", "migrate", "Operation mode: 'migrate', 'live', or 'live-only'")
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, error")
	logFile := flag.String("log-file", "", "Path to log file (logs to both stdout and file when specified)")
	help := flag.Bool("help", false, "Display help information")
	flag.Parse()

	// Display help if requested
	if *help {
		displayUsage()
		os.Exit(0)
	}

	// Create logger
	log := logger.New()
	log.SetLevel(*logLevel)

	// Set up log file if specified
	if *logFile != "" {
		file, err := log.SetOutputFile(*logFile)
		if err != nil {
			log.Fatalf("Failed to open log file %s: %v", *logFile, err)
		}
		defer file.Close()
		log.Infof("Logging to file: %s", *logFile)
	}

	// Load configuration
	log.Info("Loading configuration...")
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Validate mode
	if *mode != "migrate" && *mode != "live" && *mode != "live-only" {
		log.Fatalf("Invalid mode: %s. Please choose either 'migrate', 'live', or 'live-only'", *mode)
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChan
		log.Info("Received interrupt signal. Shutting down...")
		cancel()
		// Give some time for graceful shutdown
		time.Sleep(2 * time.Second)
		os.Exit(0)
	}()

	// Create migrator
	migrator := migration.NewMigrator(cfg, log)

	// Start migration/replication
	startTime := time.Now()
	log.Infof("Starting MongoDB to MongoDB %s process", *mode)

	if err := migrator.Start(ctx, *mode); err != nil {
		// Check if the error is due to context cancellation (Ctrl+C)
		if err == context.Canceled {
			log.Info("Process stopped due to user interrupt (Ctrl+C)")
		} else {
			log.Fatalf("Error during %s process: %v", *mode, err)
		}
	}

	// Log completion for migrate mode (live mode keeps running)
	if *mode == "migrate" {
		duration := time.Since(startTime)
		log.Infof("Migration completed in %.2f seconds", duration.Seconds())
		os.Exit(0) // Explicitly exit after migration is complete
	}
}

// displayUsage displays usage information
func displayUsage() {
	fmt.Println("\nMongoDB to MongoDB Replication Tool")
	fmt.Println("===================================")
	fmt.Println("Usage: migrate [options]")
	fmt.Println("Options:")
	fmt.Println("  -config string")
	fmt.Println("        Path to configuration file (default \"mongodb_replication_config.json\")")
	fmt.Println("  -mode string")
	fmt.Println("        Operation mode: 'migrate', 'live', or 'live-only' (default \"migrate\")")
	fmt.Println("  -log-level string")
	fmt.Println("        Log level: debug, info, warn, error (default \"info\")")
	fmt.Println("  -log-file string")
	fmt.Println("        Path to log file (logs to both stdout and file when specified)")
	fmt.Println("  -help")
	fmt.Println("        Display this help information")
	fmt.Println("Examples:")
	fmt.Println("  migrate -mode=live")
	fmt.Println("  migrate -mode=live-only")
	fmt.Println("  migrate -config=custom_config.json -mode=migrate -log-level=debug")
	fmt.Println("  migrate -mode=live -log-file=migration.log")
}
