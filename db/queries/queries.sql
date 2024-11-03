-- name: UpsertFeed :exec
insert into feeds (feed_name)
values (?)
on conflict do nothing;

-- name: GetFeed :one
select *
from feeds
where feed_name = ?;

-- name: UpdateFeedCursor :exec
update feeds
set latest_cursor = ?
where feed_name = ?;

-- name: UpsertFeedPost :exec
insert
into feed_posts (feed_name, time_us, did, rkey)
values (?, ?, ?, ?)
on conflict (feed_name, did, rkey) do nothing;

-- name: GetFeedPosts :many
select *
from feed_posts
where feed_name = ?
  and time_us < ?
order by time_us desc
limit ?;
