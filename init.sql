CREATE DATABASE formula;
\c formula;

CREATE TABLE messages (
  id SERIAL PRIMARY KEY,
  message TEXT UNIQUE NOT NULL
);

CREATE TABLE canlog (
  time TIMESTAMPTZ NOT NULL,
  message_id INTEGER NOT NULL REFERENCES messages(id),
  value REAL NULL
);

SELECT create_hypertable('canlog', 'time');

CREATE INDEX on canlog (message_id, time DESC);
