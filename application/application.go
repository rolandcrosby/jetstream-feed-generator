package application

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	confpkg "jetstream-feed-generator/config"
	"jetstream-feed-generator/consumer"
	dbpkg "jetstream-feed-generator/db"
	"jetstream-feed-generator/feedgen"

	_ "modernc.org/sqlite"
)

func Run(config confpkg.Config) error {
	var logger *slog.Logger
	handlerOptions := slog.HandlerOptions{}
	logLevel := slog.LevelVar{}
	var err error
	if config.LogLevel != "" {
		err = logLevel.UnmarshalText([]byte(config.LogLevel))
		if err == nil {
			handlerOptions.Level = &logLevel
		}
	}
	if config.LogFormat == "json" {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &handlerOptions))
	} else {
		if config.LogFormat != "" && config.LogFormat != "text" {
			err = fmt.Errorf("invalid log format: %s", config.LogFormat)
		}
		logger = slog.New(slog.NewTextHandler(os.Stdout, &handlerOptions))
	}
	slog.SetDefault(logger)
	if err != nil {
		logger.Warn("failed to parse log options", "error", err)
	}
	confpkg.LogViperEnvVars(config, "", logger)
	err = config.Validate()
	if err != nil {
		return fmt.Errorf("failed to parse config: %v", err)
	}
	db, err := sql.Open("sqlite", "file:"+config.DBFilename+"?cache=shared&_journal_mode=WAL")
	if err != nil {
		return fmt.Errorf("failed to open db: %v", err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			logger.Error("failed to close db", "error", err)
		}
	}(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := db.ExecContext(ctx, dbpkg.SchemaSQL); err != nil {
		return fmt.Errorf("failed to set up database schema: %v", err)
	}
	var wg sync.WaitGroup

	if config.Consumer.Enabled {
		consumerConfig := consumer.Config{
			JetstreamURL: config.Consumer.JetstreamURL,
			StartCursor:  config.Consumer.StartCursor,
			DB:           db,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if runErr := consumer.RunConsumer(ctx, consumerConfig); runErr != nil {
				slog.Error("consumer error", "error", runErr)
				cancel()
			}
		}()
	}

	if config.Feedgen.Enabled {
		feedgenConfig := feedgen.Config{
			FeedActorDID:    config.Feedgen.FeedActorDID,
			ServiceEndpoint: config.Feedgen.ServiceEndpoint,
			Port:            config.Feedgen.Port,
			FeedNames:       config.FeedNames,
			DB:              db,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if runErr := feedgen.RunFeedGenerator(ctx, feedgenConfig); runErr != nil {
				slog.Error("feed generator error", "error", runErr)
				cancel()
			}
		}()
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	wg.Wait()
	return nil
}
