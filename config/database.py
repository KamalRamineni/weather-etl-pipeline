import logging
import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Manages database connections and connection pooling via SQLAlchemy."""

    def __init__(self):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.database = os.getenv('DB_NAME', 'weather_db')
        self.user = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', '')

        encoded_user = quote_plus(self.user)
        encoded_password = quote_plus(self.password)
        self.connection_string = (
            f"postgresql://{encoded_user}:{encoded_password}@"
            f"{self.host}:{self.port}/{self.database}"
        )

        self.engine = create_engine(
            self.connection_string,
            echo=False,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            connect_args={"sslmode": "require"},
        )

        self.SessionLocal = sessionmaker(bind=self.engine)

    def get_connection(self):
        return self.engine.connect()

    def get_session(self):
        return self.SessionLocal()

    def test_connection(self):
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            return True
        except Exception as e:
            logger.error("Database connection failed: %s", e)
            return False

    def execute_sql_file(self, file_path):
        try:
            with open(file_path, 'r') as f:
                sql_commands = f.read()

            # Strip full-line comments before splitting on semicolons
            cleaned = '\n'.join(
                line for line in sql_commands.splitlines()
                if not line.strip().startswith('--')
            )

            with self.engine.connect() as conn:
                with conn.begin():
                    for command in cleaned.split(';'):
                        command = command.strip()
                        if command:
                            conn.execute(text(command))

            logger.info("Executed SQL file: %s", file_path)
        except Exception as e:
            logger.error("Error executing SQL file %s: %s", file_path, e)
            raise

    def close(self):
        self.engine.dispose()
        logger.info("Database connections closed")
