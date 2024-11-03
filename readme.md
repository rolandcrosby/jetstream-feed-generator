# jetstream-feed-generator

Glues together [Jetstream](https://github.com/bluesky-social/jetstream) and [go-bsky-feed-generator](https://github.com/ericvolp12/go-bsky-feed-generator/) with some SQLite to consume the Bluesky firehose and serve a feed based on posts matching some criteria.

Currently (11/24) in use serving [this feed](https://bsky.app/profile/roland.cros.by/feed/composer-errors), which detects when someone types a domain by accident, fixes it, and inadvertently leaves the link attachment.
