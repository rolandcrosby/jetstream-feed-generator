create table if not exists feeds
(
    feed_name     text primary key,
    latest_cursor integer
);

create table if not exists feed_posts
(
    feed_name text    not null,
    time_us   integer not null,
    did       text    not null,
    rkey      text    not null
);

create unique index if not exists feed_posts_unique_by_feed on feed_posts (feed_name, did, rkey);
create index if not exists feed_posts_by_time on feed_posts (feed_name, time_us);
