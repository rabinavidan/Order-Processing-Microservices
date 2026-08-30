from sqlalchemy import create_engine, inspect, text

from main import run_migrations


def test_migrations_create_expected_tables(tmp_path):
    db_path = tmp_path / "migration_test.db"
    database_url = f"sqlite:///{db_path}"

    run_migrations(database_url)

    engine = create_engine(database_url)
    tables = set(inspect(engine).get_table_names())
    assert {"stock", "processed_messages", "outbox_events", "alembic_version"} <= tables


def test_migrations_seed_default_stock(tmp_path):
    db_path = tmp_path / "migration_test.db"
    database_url = f"sqlite:///{db_path}"

    run_migrations(database_url)

    engine = create_engine(database_url)
    with engine.connect() as conn:
        rows = dict(conn.execute(text("SELECT product, quantity FROM stock")).all())
    assert rows == {"laptop": 10, "phone": 5, "keyboard": 20}


def test_migrations_are_idempotent(tmp_path):
    db_path = tmp_path / "migration_test.db"
    database_url = f"sqlite:///{db_path}"

    run_migrations(database_url)
    run_migrations(database_url)  # should no-op cleanly, not raise
