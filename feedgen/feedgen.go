package feedgen

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	db "jetstream-feed-generator/db/sqlc"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/ericvolp12/go-bsky-feed-generator/pkg/auth"
	"github.com/ericvolp12/go-bsky-feed-generator/pkg/feedrouter"
	ginendpoints "github.com/ericvolp12/go-bsky-feed-generator/pkg/gin"
	"github.com/gin-gonic/gin"
	sloggin "github.com/samber/slog-gin"
)

type Config struct {
	FeedActorDID    string
	ServiceEndpoint string
	Port            int
	FeedNames       []string
	DB              *sql.DB
}

func RunFeedGenerator(ctx context.Context, config Config) error {
	// Set the acceptable DIDs for the feed generator to respond to
	// We'll default to the FeedActorDID and the Service Endpoint as a did:web
	serviceURL, err := url.Parse(config.ServiceEndpoint)
	if err != nil {
		return fmt.Errorf("error parsing service endpoint: %w", err)
	}
	if serviceURL.Hostname() == "" {
		return fmt.Errorf("service endpoint must have a hostname")
	}

	logger := slog.With("component", "feedgen")

	serviceWebDID := "did:web:" + serviceURL.Hostname()

	acceptableDIDs := []string{config.FeedActorDID, serviceWebDID}

	feedRouter, err := feedrouter.NewFeedRouter(ctx, config.FeedActorDID,
		serviceWebDID, acceptableDIDs, config.ServiceEndpoint)
	if err != nil {
		return fmt.Errorf("error creating feed router: %w", err)
	}

	queries := db.New(config.DB)
	for _, feedName := range config.FeedNames {
		feedRouter.AddFeed([]string{feedName}, DbFeed{
			FeedActorDID: config.FeedActorDID,
			FeedName:     feedName,
			Q:            queries,
		})
	}

	// Create a gin router with default middleware for logging and recovery
	router := gin.New()
	router.Use(sloggin.New(logger))
	router.Use(gin.Recovery())

	// Add unauthenticated routes for feed generator
	ep := ginendpoints.NewEndpoints(feedRouter)
	router.GET("/.well-known/did.json", ep.GetWellKnownDID)
	router.GET("/xrpc/app.bsky.feed.describeFeedGenerator", ep.DescribeFeeds)

	// Plug in Authentication Middleware
	auther, err := auth.NewAuth(
		100_000,
		time.Hour*12,
		5,
		serviceWebDID,
	)
	if err != nil {
		return fmt.Errorf("failed to create Auth: %v", err)
	}

	router.Use(auther.AuthenticateGinRequestViaJWT)

	// Add authenticated routes for feed generator
	router.GET("/xrpc/app.bsky.feed.getFeedSkeleton", ep.GetFeedSkeleton)

	logger.Info("starting server", "port", config.Port, "service_did", serviceWebDID)

	srv := http.Server{
		Addr:    fmt.Sprintf(":%d", config.Port),
		Handler: router,
	}
	serverError := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverError <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown")
		shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 5*time.Second)
		defer shutdownCancel()

		// Shutdown the server gracefully
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("failed to shut down feed generator: %v", err)
		}
		return nil
	case err := <-serverError:
		return fmt.Errorf("feed generator error: %v", err)
	}
}
