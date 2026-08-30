-- Runs once, automatically, the first time the postgres container initializes
-- its data volume (docker-entrypoint-initdb.d convention). Each service gets
-- its own database — database-per-service, even though they share one
-- Postgres instance for simplicity in this compose stack.
CREATE DATABASE order_db;
CREATE DATABASE inventory_db;
CREATE DATABASE payment_db;
CREATE DATABASE notification_db;
