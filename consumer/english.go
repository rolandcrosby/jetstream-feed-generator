package consumer

import (
	"context"
	apibsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/jetstream/pkg/models"
	"log/slog"
	"slices"
	"unicode"
)

// EmojiRange represents a Unicode range for emoji characters
type EmojiRange struct {
	Start rune
	End   rune
	Name  string
}

// this list came from Claude; who knows how wrong it is
var emojiRanges = []EmojiRange{
	{0x2600, 0x26FF, "Miscellaneous Symbols"},
	{0x2700, 0x27BF, "Dingbats"},
	{0x2B50, 0x2B55, "Star and Other Symbols"},
	{0xFE00, 0xFE0F, "Variation Selectors"},
	{0x1F000, 0x1FFFF, "Really high characters"},
}

func IsEmoji(r rune) bool {
	for _, rang := range emojiRanges {
		if r >= rang.Start && r <= rang.End {
			return true
		}
	}
	return false
}

func ContainsEmoji(s string) bool {
	for _, r := range s {
		if IsEmoji(r) {
			return true
		}
	}
	return false
}

type EnglishTextFeed struct {
	name   string
	logger *slog.Logger
}

func (f *EnglishTextFeed) Initialize(ctx context.Context) error {
	return nil
}

func (f *EnglishTextFeed) LatestCursor(ctx context.Context) (int64, error) {
	return 0, nil
}

func (f *EnglishTextFeed) SaveCursor(ctx context.Context, cursor int64) error {
	return nil
}

func NewEnglishTextFeed(name string, logger *slog.Logger) *EnglishTextFeed {
	feedLogger := logger.With("feed", name)
	return &EnglishTextFeed{name, feedLogger}
}

func (f *EnglishTextFeed) Name() string {
	return f.name
}

func (f *EnglishTextFeed) HandlePost(ctx context.Context, event *models.Event, post *apibsky.FeedPost) error {
	if f.isEnglishText(post) {
		f.logger.Debug(
			"post matched", "did", event.Did, "rkey", event.Commit.RKey,
			"text", post.Text,
		)
	}
	return nil
}

func (f *EnglishTextFeed) isEnglishText(post *apibsky.FeedPost) bool {
	if post.Embed != nil || post.Reply != nil || len(post.Text) == 0 || len(post.Facets) > 0 {
		return false
	}
	if !slices.Contains(post.Langs, "en") {
		return false
	}
	nonascii := 0
	for _, r := range post.Text {
		if r == '\n' {
			return false
		}
		if r > unicode.MaxASCII {
			nonascii++
		}
	}
	if float64(nonascii)/float64(len(post.Text)) > 0.2 {
		f.logger.Debug("rejected for non-ASCII", "text", post.Text)
		return false
	}
	if ContainsEmoji(post.Text) {
		f.logger.Debug("rejected for emoji", "text", post.Text)
		return false
	}
	return true
}
