package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"github.com/gsbingo17/mongodb-migration/pkg/migration"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "mongodb_replication_config.json", "Path to configuration file")
	mode := flag.String("mode", "migrate", "Operation mode: 'migrate', 'live', or 'live-only'")
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, error")
	logFile := flag.String("log-file", "", "Path to log file (logs to both stdout and file when specified)")
	cdcStartTimeStr := flag.String("cdc-start-timestamp", "", "Start timestamp for live-only replication (Unix epoch seconds or RFC3339 format)")
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

	// Display and log the loaded configuration with sensitive values masked
	log.Infof("Active Configuration:\n%s", getSanitizedConfigJSON(cfg))



	// Validate mode
	if *mode != "migrate" && *mode != "live" && *mode != "live-only" {
		log.Fatalf("Invalid mode: %s. Please choose either 'migrate', 'live', or 'live-only'", *mode)
	}

	// Parse and validate cdc-start-timestamp option.
	// This flag specifies a custom historical starting point (Unix epoch seconds or RFC3339 date)
	// from which the incremental change stream/oplog replication should begin.
	// Note: This is only valid in "live-only" mode because standard migration modes always
	// automatically capture the starting position prior to performing the initial copy phase.
	var cdcStartTime *primitive.Timestamp
	if *cdcStartTimeStr != "" {
		if *mode != "live-only" {
			log.Fatal("Error: -cdc-start-timestamp can only be specified when -mode is 'live-only'")
		}
		ts, err := parseStartTimestamp(*cdcStartTimeStr)
		if err != nil {
			log.Fatalf("Failed to parse -cdc-start-timestamp: %v", err)
		}
		cdcStartTime = ts
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
	migrator.CdcStartTime = cdcStartTime

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
	fmt.Println("        Modes:")
	fmt.Println("          migrate:")
	fmt.Println("            Perform a one-time full migration. Copies all data and indexes from")
	fmt.Println("            source to target, then exits immediately.")
	fmt.Println("          live:")
	fmt.Println("            Perform a full migration followed by real-time replication. Captures")
	fmt.Println("            the replication position, copies all existing data and indexes (if configured), then")
	fmt.Println("            automatically transitions to streaming real-time changes. Runs continuously.")
	fmt.Println("          live-only:")
	fmt.Println("            Perform real-time incremental replication only. Skips the initial data")
	fmt.Println("            copy phase. Starts streaming real-time changes from the last saved resume token")
	fmt.Println("            (or current moment if none exists), or from a custom position when")
	fmt.Println("            -cdc-start-timestamp is specified.")
	fmt.Println("  -log-level string")
	fmt.Println("        Log level: debug, info, warn, error (default \"info\")")
	fmt.Println("  -log-file string")
	fmt.Println("        Path to log file (logs to both stdout and file when specified)")
	fmt.Println("  -cdc-start-timestamp string")
	fmt.Println("        Start timestamp for live-only replication (Unix epoch seconds or RFC3339 format)")
	fmt.Println("        Debian command-line examples to get 'now':")
	fmt.Printf("          * Unix epoch seconds:             date +%%s\n")
	fmt.Println("          * RFC3339 format:                 date --rfc-3339=seconds   (or: date -Iseconds)")
	fmt.Println("  -help")
	fmt.Println("        Display this help information")
	fmt.Println("Examples:")
	fmt.Println("  migrate -mode=live")
	fmt.Println("  migrate -mode=live-only")
	fmt.Println("  migrate -mode=live-only -cdc-start-timestamp=1716234000")
	fmt.Println("  migrate -mode=live-only -cdc-start-timestamp=2026-05-20T21:00:00Z")
	fmt.Println("  migrate -config=custom_config.json -mode=migrate -log-level=debug")
	fmt.Println("  migrate -mode=live -log-file=migration.log")
}

// parseStartTimestamp parses a user-provided timestamp string as either a raw Unix epoch
// timestamp in seconds (e.g. 1716234000) or a standard RFC3339 formatted string (e.g. 2026-05-20T21:00:00Z).
// It constructs and returns a BSON primitive.Timestamp structure with the scanned seconds component (T)
// and an initial increment (I) set to 1, suitable for MongoDB change streams and oplog tailing.
func parseStartTimestamp(s string) (*primitive.Timestamp, error) {
	if s == "" {
		return nil, nil
	}

	// Try parsing the string strictly as an integer. We use strconv.ParseInt rather than
	// fmt.Sscan to ensure the entire string is a valid base-10 number (avoiding partial matches
	// such as extracting "2026" from "2026-05-20...").
	if secs, err := strconv.ParseInt(s, 10, 64); err == nil {
		return &primitive.Timestamp{T: uint32(secs), I: 1}, nil
	}

	// If it is not a plain integer, fall back to parsing as an RFC3339 date string.
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp format, must be Unix epoch seconds or RFC3339 (e.g. 2026-05-20T21:00:00Z): %w", err)
	}
	return &primitive.Timestamp{T: uint32(t.Unix()), I: 1}, nil
}

// sanitizeConnectionString masks sensitive user credentials in MongoDB URIs.
func sanitizeConnectionString(uri string) string {
	if uri == "" {
		return ""
	}
	var prefix string
	if strings.HasPrefix(uri, "mongodb://") {
		prefix = "mongodb://"
	} else if strings.HasPrefix(uri, "mongodb+srv://") {
		prefix = "mongodb+srv://"
	} else {
		return uri
	}

	remaining := uri[len(prefix):]
	atIndex := strings.LastIndex(remaining, "@")
	if atIndex == -1 {
		return uri
	}

	credentials := remaining[:atIndex]
	hostAndParams := remaining[atIndex:]

	colonIndex := strings.Index(credentials, ":")
	if colonIndex == -1 {
		return prefix + credentials + hostAndParams
	}

	username := credentials[:colonIndex]
	return prefix + username + ":*****" + hostAndParams
}

// getSanitizedConfigJSON generates an indented JSON string representation of Config with credentials masked.
func getSanitizedConfigJSON(cfg *config.Config) string {
	if cfg == nil {
		return "{}"
	}

	sanitizedCfg := *cfg
	sanitizedCfg.DatabasePairs = make([]config.DatabasePair, len(cfg.DatabasePairs))
	for i, pair := range cfg.DatabasePairs {
		sanitizedPair := pair
		sanitizedPair.Source.ConnectionString = sanitizeConnectionString(pair.Source.ConnectionString)
		sanitizedPair.Target.ConnectionString = sanitizeConnectionString(pair.Target.ConnectionString)
		sanitizedCfg.DatabasePairs[i] = sanitizedPair
	}

	data, err := json.MarshalIndent(sanitizedCfg, "", "  ")
	if err != nil {
		return fmt.Sprintf("error marshalling config: %v", err)
	}
	return string(data)
}
