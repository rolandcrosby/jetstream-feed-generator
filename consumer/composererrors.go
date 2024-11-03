package consumer

import (
	"context"
	"database/sql"
	"fmt"
	apibsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/jetstream/pkg/models"
	dbpkg "jetstream-feed-generator/db/sqlc"
	"log/slog"
	"regexp"
	"strings"
)

type ComposerErrorsFeed struct {
	name   string
	logger *slog.Logger
	q      *dbpkg.Queries
}

func NewComposerErrorsFeed(name string, logger *slog.Logger, db *sql.DB) *ComposerErrorsFeed {
	feedLogger := logger.With("feed", name)
	return &ComposerErrorsFeed{
		name:   name,
		logger: feedLogger,
		q:      dbpkg.New(db),
	}
}

func (f *ComposerErrorsFeed) Name() string {
	return f.name
}

func (f *ComposerErrorsFeed) Initialize(ctx context.Context) error {
	if err := f.q.UpsertFeed(ctx, f.Name()); err != nil {
		return fmt.Errorf("failed to upsert feed: %v", err)
	}
	return nil
}

func (f *ComposerErrorsFeed) LatestCursor(ctx context.Context) (int64, error) {
	feed, err := f.q.GetFeed(ctx, f.Name())
	if err != nil {
		return 0, err
	}
	if feed.LatestCursor.Valid {
		return feed.LatestCursor.Int64, nil
	}
	return 0, nil
}

func (f *ComposerErrorsFeed) SaveCursor(ctx context.Context, cursor int64) error {
	err := f.q.UpdateFeedCursor(ctx, dbpkg.UpdateFeedCursorParams{
		LatestCursor: sql.NullInt64{Int64: cursor, Valid: true},
		FeedName:     f.Name(),
	})
	if err != nil {
		return err
	}
	return nil
}

func (f *ComposerErrorsFeed) HandlePost(ctx context.Context, event *models.Event, post *apibsky.FeedPost) error {
	if isComposerError(post) {
		f.logger.Debug(
			"post matched", "did", event.Did, "rkey", event.Commit.RKey,
			"text", post.Text, "uri", post.Embed.EmbedExternal.External.Uri,
		)
		err := f.q.UpsertFeedPost(ctx, dbpkg.UpsertFeedPostParams{
			FeedName: f.Name(),
			TimeUs:   event.TimeUS,
			Did:      event.Did,
			Rkey:     event.Commit.RKey,
		})
		if err != nil {
			return fmt.Errorf("failed to upsert feed post: %w", err)
		}
	}
	return nil
}

var domainRe = regexp.MustCompile("^https://(([A-Za-z0-9-]+)\\.([A-Za-z0-9]+))$")

func isComposerError(post *apibsky.FeedPost) bool {
	if post.Embed == nil || post.Embed.EmbedExternal == nil || post.Embed.EmbedExternal.External == nil {
		return false
	}
	uri := post.Embed.EmbedExternal.External.Uri
	matches := domainRe.FindStringSubmatch(uri)
	if len(matches) < 3 {
		return false
	}
	for _, facet := range post.Facets {
		for _, feature := range facet.Features {
			if feature.RichtextFacet_Link != nil && feature.RichtextFacet_Link.Uri == uri {
				return false
			}
		}
	}
	spaced := matches[2] + " " + matches[3]
	return strings.Contains(post.Text, spaced)
}
