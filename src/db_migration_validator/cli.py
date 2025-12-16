"""
CLI entry point for Database Migration Validator.

This module:
- Reads environment variables
- Builds source and target database configurations
- Creates connectors using ConnectorFactory
- Establishes database connections

This is the starting point of the application.
"""

import os
from dotenv import load_dotenv

from db_migration_validator.connectors.factory import ConnectorFactory


def build_db_config(prefix: str) -> dict:
    """
    Build database configuration dictionary from environment variables.

    Args:
        prefix (str): Either 'SOURCE' or 'TARGET'

    Returns:
        dict: Database connection configuration
    """
    return {
        "host": os.getenv(f"{prefix}_HOST"),
        "port": int(os.getenv(f"{prefix}_PORT")),
        "db": os.getenv(f"{prefix}_DB"),
        "user": os.getenv(f"{prefix}_USER"),
        "password": os.getenv(f"{prefix}_PASSWORD"),
    }


def main():
    """
    Main CLI execution function.
    """
    # Load variables from .env file
    load_dotenv()

    # Read database types
    source_db_type = os.getenv("SOURCE_DB_TYPE")
    target_db_type = os.getenv("TARGET_DB_TYPE")

    if not source_db_type or not target_db_type:
        raise ValueError("SOURCE_DB_TYPE and TARGET_DB_TYPE must be set in .env")

    # Build connection configs
    source_config = build_db_config("SOURCE")
    target_config = build_db_config("TARGET")

    # Create connectors using factory
    source_connector = ConnectorFactory.create(source_db_type, source_config)
    target_connector = ConnectorFactory.create(target_db_type, target_config)

    # Connect to databases
    print(f"Connecting to SOURCE database ({source_db_type})...")
    source_connector.connect()
    print("Source connection successful.")

    print(f"Connecting to TARGET database ({target_db_type})...")
    target_connector.connect()
    print("Target connection successful.")

    # Simple smoke test: fetch schemas
    source_schemas = source_connector.list_schemas()
    target_schemas = target_connector.list_schemas()

    print(f"\nSOURCE schemas (sample): {source_schemas[:5]}")
    print(f"TARGET schemas (sample): {target_schemas[:5]}")

    # Close connections cleanly
    source_connector.close()
    target_connector.close()

    print("\nCLI execution completed successfully.")


if __name__ == "__main__":
    main()
