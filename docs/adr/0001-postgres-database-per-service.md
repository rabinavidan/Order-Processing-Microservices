# ADR 0001: Postgres, database-per-service, on one shared instance

**Status:** Accepted

## Context

Before M3, `order_service` had no persisted state at all (it only published to Kafka), and `inventory_service`/`payment_service`/`notification_service` kept their state — stock levels, the M2 idempotency dedup set — in plain in-process `dict`/`set` objects. Restarting a service silently reset it to defaults and forgot every duplicate it had ever seen. M3's goal is state that survives a restart.

The two realistic options for a project this size were:

- **SQLite**, one file per service, persisted via a Docker volume.
- **Postgres**, one server, with each service owning its own database.

## Decision

Postgres, with **database-per-service**: one `postgres` container in `docker-compose.yml` (see [`infra/postgres/init-databases.sql`](../../infra/postgres/init-databases.sql)), but `order_db`, `inventory_db`, `payment_db`, and `notification_db` are 4 separate databases inside it — no service can query another's tables, so the service boundary stays real at the data layer, not just in the code. Each service talks to Postgres through SQLAlchemy 2.0 (`db.py`) and owns its own Alembic migration history (`alembic/versions/`).

## Why not SQLite

SQLite would have been simpler to stand up (no extra container, no driver, no connection string wiring) and would have satisfied M3's literal "survives a restart" requirement just as well. It was rejected here because:

- **Concurrency.** `order_service` is a FastAPI app that can receive concurrent requests; SQLite's single-writer file lock is a real limitation under any meaningful load, where Postgres just isn't.
- **Interview signal.** A stack that runs Postgres in compose, with real migrations and a database-per-service boundary, reads as the thing teams actually run in production. SQLite reads as a demo shortcut — accurate for what this project needs functionally, but it undersells the "production thinking" this milestone is explicitly trying to demonstrate.
- **Alembic exercises the same code path either way.** Since the schema is simple (no exotic Postgres-only types), writing the migrations against SQLAlchemy's dialect-agnostic `op.create_table()` means the same migration files apply cleanly to both engines — confirmed by hand (`alembic upgrade head` against a local Postgres container) and by the unit test suite (`tests/test_migrations.py` in each service runs the real migration against a throwaway SQLite file, and the e2e test runs it against a real Postgres testcontainer) — so choosing Postgres cost nothing in migration-authoring complexity.

## Consequences

- Local `pytest` runs still use **in-memory SQLite** (via `sqlalchemy.pool.StaticPool`) for speed and zero external dependencies — unit tests bypass Alembic entirely and call `Base.metadata.create_all()` directly. This means the unit tests validate application logic against SQLite while production runs Postgres; the migration files themselves are what's proven against both engines (see above), which is the part that would actually differ between dialects.
- Running the full stack locally (`docker-compose up`) now requires Postgres to become healthy before any service starts (`depends_on: postgres: condition: service_healthy`), same pattern already used for Kafka.
- `psycopg2-binary` is an added dependency in every service's `requirements.txt`; it ships prebuilt wheels, so this didn't require any new system packages in the Dockerfiles.
