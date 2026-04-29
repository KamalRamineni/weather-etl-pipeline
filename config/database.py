"""
Database Configuration Module

This module handles database connections using SQLAlchemy.
SQLAlchemy is an ORM (Object Relational Mapper) that provides:
- Database abstraction (works with PostgreSQL, MySQL, SQLite, etc.)
- Connection pooling (reuses connections for better performance)
- SQL generation (can write Python instead of raw SQL)
"""

import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables from .env file
# This keeps sensitive info (passwords, API keys) out of code
load_dotenv()


class DatabaseConfig:
    """
    Manages database connections and operations.
    
    Key concepts:
    - Engine: Manages connections to the database
    - Session: Represents a "conversation" with the database
    - Connection pooling: Reuses connections instead of creating new ones
    """
    
    def __init__(self):
        """Initialize database connection parameters from environment variables"""
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.database = os.getenv('DB_NAME', 'weather_db')
        self.user = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', '')
        
        # Create connection string
        # Format: postgresql://username:password@host:port/database
        encoded_user = quote_plus(self.user)
        encoded_password = quote_plus(self.password)
        self.connection_string = (
            f"postgresql://{encoded_user}:{encoded_password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
        
        # Create SQLAlchemy engine
        # echo=True will print all SQL queries (useful for learning!)
        self.engine = create_engine(
            self.connection_string,
            echo=False,  # Set to True to see SQL queries
            pool_pre_ping=True,  # Verify connections before using them
            pool_size=5,  # Number of connections to keep open
            max_overflow=10  # Max additional connections when pool is full
        )
        
        # Create session factory
        self.SessionLocal = sessionmaker(bind=self.engine)
    
    def get_connection(self):
        """
        Get a raw database connection.
        Use this for executing SQL directly.
        
        Returns:
            SQLAlchemy connection object
        """
        return self.engine.connect()
    
    def get_session(self):
        """
        Get a database session.
        Use this for ORM operations (more advanced).
        
        Returns:
            SQLAlchemy session object
        """
        return self.SessionLocal()
    
    def test_connection(self):
        """
        Test database connection.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                # Execute a simple query to test connection
                result = conn.execute(text("SELECT 1"))
                print("✅ Database connection successful!")
                return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def execute_sql_file(self, file_path):
        """
        Execute SQL commands from a file.
        Useful for running schema creation scripts.
        
        Args:
            file_path (str): Path to SQL file
        """
        try:
            with open(file_path, 'r') as file:
                sql_commands = file.read()

            # Remove full-line SQL comments before splitting commands.
            # Otherwise chunks that start with comments can cause valid SQL
            # statements in the same chunk to be skipped.
            cleaned_commands = []
            for line in sql_commands.splitlines():
                stripped = line.strip()
                if stripped.startswith('--'):
                    continue
                cleaned_commands.append(line)
            sql_commands = '\n'.join(cleaned_commands)
            
            with self.engine.connect() as conn:
                # Split by semicolon and execute each statement
                # SQLAlchemy requires explicit transaction management
                with conn.begin():
                    for command in sql_commands.split(';'):
                        command = command.strip()
                        if command and not command.startswith('--'):
                            conn.execute(text(command))
            
            print(f"✅ Successfully executed SQL file: {file_path}")
        except Exception as e:
            print(f"❌ Error executing SQL file: {e}")
            raise
    
    def close(self):
        """Close all database connections"""
        self.engine.dispose()
        print("Database connections closed.")


# Example usage:
if __name__ == "__main__":
    # This code runs only when you execute this file directly
    # Not when you import it as a module
    
    db = DatabaseConfig()
    
    # Test connection
    if db.test_connection():
        print("\n🎉 Database is ready to use!")
        
        # Show connection details (without password)
        print(f"\nConnection details:")
        print(f"  Host: {db.host}")
        print(f"  Port: {db.port}")
        print(f"  Database: {db.database}")
        print(f"  User: {db.user}")
