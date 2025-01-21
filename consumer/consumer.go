package consumer

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	dbpkg "jetstream-feed-generator/db/sqlc"

	apibsky "github.com/bluesky-social/indigo/api/bsky"
	jetstreamClient "github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/sequential"
	"github.com/bluesky-social/jetstream/pkg/models"
)

const DefaultJetstreamURL = "wss://jetstream1.us-east.bsky.network/subscribe"

type Config struct {
	JetstreamURL string
	StartCursor  int64
	DB           *sql.DB
}

type Feed interface {
	Name() string
	DB() *dbpkg.Queries
	HandlePost(ctx context.Context, event *models.Event, post *apibsky.FeedPost) error
}

func RunConsumer(ctx context.Context, config Config) error {
	logger := slog.With("component", "consumer")
	handler := handler{
		feeds: []Feed{
			NewComposerErrorsFeed("composer-errors", logger, config.DB),
			NewEnglishTextFeed("english-text", logger, config.DB),
		},
		latestCursor: config.StartCursor,
	}

	for _, f := range handler.feeds {
		if err := f.DB().UpsertFeed(ctx, f.Name()); err != nil {
			return fmt.Errorf("failed to initialize feed %s: %v", f.Name(), err)
		}
	}

	var lag float64
	if handler.latestCursor != 0 {
		lag = time.Since(time.UnixMicro(handler.latestCursor)).Seconds()
		logger.Info("starting at requested cursor", "cursor", handler.latestCursor, "lag_s", lag)
	} else {
		var resumeCursor int64
		for _, f := range handler.feeds {
			var err error
			feedCursor := int64(0)
			feed, err := f.DB().GetFeed(ctx, f.Name())
			if err == nil && feed.LatestCursor.Valid {
				feedCursor = feed.LatestCursor.Int64
			}

			if err != nil {
				return fmt.Errorf("failed to get latest cursor for feed %s: %v", f.Name(), err)
			}
			if feedCursor != 0 && (resumeCursor == 0 || feedCursor < resumeCursor) {
				resumeCursor = feedCursor
			}
		}

		if resumeCursor == 0 {
			handler.latestCursor = time.Now().UnixMicro()
			logger.Info("no saved cursor in database, starting at current time", "cursor", handler.latestCursor)
		} else {
			lag = time.Since(time.UnixMicro(resumeCursor)).Seconds()
			logger.Info("resuming from saved cursor", "saved_cursor", resumeCursor, "lag_s", lag)
			handler.latestCursor = resumeCursor
		}
	}
	lag = time.Since(time.UnixMicro(handler.latestCursor)).Seconds()
	logger.Info("starting consumer", "cursor", handler.latestCursor, "lag_s", lag)

	jetstreamConfig := jetstreamClient.DefaultClientConfig()
	jetstreamConfig.WebsocketURL = config.JetstreamURL
	jetstreamConfig.Compress = true
	jetstreamConfig.WantedCollections = append(jetstreamConfig.WantedCollections, "app.bsky.feed.post")

	scheduler := sequential.NewScheduler("jetstream-feed-generator", logger, handler.HandleEvent)

	c, err := jetstreamClient.NewClient(jetstreamConfig, logger, scheduler)
	if err != nil {
		return fmt.Errorf("failed to create Jetstream client: %v", err)
	}

	// Every 5 seconds print stats and update the high-water mark in the DB
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case t := <-ticker.C:
				for _, f := range handler.feeds {
					err := f.DB().UpdateFeedCursor(ctx, dbpkg.UpdateFeedCursorParams{
						LatestCursor: sql.NullInt64{Int64: handler.latestCursor, Valid: true},
						FeedName:     f.Name(),
					})
					if err != nil {
						logger.Error("failed to save cursor", "feed", f.Name(), "error", err)
					}
				}
				if t.Second()%5 == 0 {
					eventsRead := c.EventsRead.Load()
					bytesRead := c.BytesRead.Load()
					avgEventSize := bytesRead / eventsRead
					lag := time.Now().Sub(time.UnixMicro(handler.latestCursor)).Seconds()
					logger.Info(
						"stats", "events_read", eventsRead, "bytes_read", bytesRead,
						"avg_event_size", avgEventSize, "latest_cursor", handler.latestCursor, "lag_s", lag,
					)
				}
			case <-ctx.Done():
				logger.Info("shutdown", "latest_cursor", handler.latestCursor)
				return
			}
		}
	}()

	if err := c.ConnectAndRead(ctx, &handler.latestCursor); err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}

	logger.Info("shutdown")
	return nil
}

type handler struct {
	feeds        []Feed
	latestCursor int64
}

func (h *handler) HandleEvent(ctx context.Context, event *models.Event) error {
	if event.Commit != nil && (event.Commit.Operation == models.CommitOperationCreate || event.Commit.Operation == models.CommitOperationUpdate) {
		switch event.Commit.Collection {
		case "app.bsky.feed.post":
			var post apibsky.FeedPost
			if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
				return fmt.Errorf("failed to unmarshal post: %w", err)
			}
			for _, f := range h.feeds {
				if err := f.HandlePost(ctx, event, &post); err != nil {
					return err
				}
			}
		}
	}

	h.latestCursor = event.TimeUS

	return nil
}
