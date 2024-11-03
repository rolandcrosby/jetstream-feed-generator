package feedgen

import (
	"context"
	"fmt"
	db "jetstream-feed-generator/db/sqlc"
	"log/slog"
	"strconv"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
)

type DbFeed struct {
	FeedActorDID string
	FeedName     string
	Q            *db.Queries
}

func (dbf DbFeed) GetPage(
	ctx context.Context, feed string, userDID string,
	limit int64, cursor string,
) ([]*bsky.FeedDefs_SkeletonFeedPost, *string, error) {
	slog.Info("generating feed", "component", "dbfeed",
		"feed", feed, "user_did", userDID, "limit", limit, "cursor", cursor)
	// not using userDID, same posts for everybody
	cursorAsInt := time.Now().UnixMicro()
	var err error

	if cursor != "" {
		cursorAsInt, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("cursor is not an integer: %w", err)
		}
	}

	var posts []*bsky.FeedDefs_SkeletonFeedPost
	dbPosts, err := dbf.Q.GetFeedPosts(ctx, db.GetFeedPostsParams{
		FeedName: dbf.FeedName,
		TimeUs:   cursorAsInt,
		Limit:    limit,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get posts: %w", err)
	}

	for _, post := range dbPosts {
		posts = append(posts, &bsky.FeedDefs_SkeletonFeedPost{
			Post: "at://" + post.Did + "/app.bsky.feed.post/" + post.Rkey,
		})
	}

	var newCursor *string
	if len(dbPosts) > 0 {
		newCursor = new(string)
		*newCursor = strconv.FormatInt(dbPosts[len(dbPosts)-1].TimeUs, 10)
	}
	return posts, newCursor, nil
}

func (dbf DbFeed) Describe(ctx context.Context) ([]bsky.FeedDescribeFeedGenerator_Feed, error) {
	return []bsky.FeedDescribeFeedGenerator_Feed{
		{
			Uri: "at://" + dbf.FeedActorDID + "/app.bsky.feed.generator/" + dbf.FeedName,
		},
	}, nil
}
