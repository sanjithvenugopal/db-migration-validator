"""
BaseConnector

This module defines the abstract base class for all database connectors.
Every supported database (Oracle, Postgres, Redshift, etc.) must implement
this interface.

This ensures:
- Consistent behavior across databases
- Easy extensibility
- Clean separation of concerns
"""

from abc import ABC, abstractmethod


class BaseConnector(ABC):
    """
    Abstract base class for database connectors.
    All database-specific connectors must inherit from this class
    and implement its methods.
    """

    def __init__(self, config: dict):
        """
        Initialize connector with configuration.

        Args:
            config (dict): Database connection configuration
                           (host, port, db, user, password, etc.)
        """
        self.config = config
        self.connection = None

    @abstractmethod
    def connect(self):
        """
        Establish a connection to the database.
        Must set self.connection.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Close the database connection safely.
        """
        pass

    @abstractmethod
    def list_schemas(self):
        """
        Return a list of schemas available in the database.

        Returns:
            List[str]
        """
        pass

    @abstractmethod
    def list_tables(self, schema: str):
        """
        Return a list of tables for a given schema.

        Args:
            schema (str): Schema name

        Returns:
            List[str]
        """
        pass

    @abstractmethod
    def get_row_count(self, schema: str, table: str):
        """
        Return total row count for a table.

        Args:
            schema (str): Schema name
            table (str): Table name

        Returns:
            int
        """
        pass

    @abstractmethod
    def get_columns(self, schema: str, table: str):
        """
        Return column metadata for a table.

        Each column must be normalized into this structure:
        {
            "column_name": str,
            "data_type": str,
            "length": int | None,
            "precision": int | None,
            "scale": int | None
        }

        Args:
            schema (str): Schema name
            table (str): Table name

        Returns:
            List[dict]
        """
        pass

    @abstractmethod
    def get_constraints(self, schema: str, table: str):
        """
        Return constraints for a table (PK, FK, UNIQUE).

        Normalized format:
        {
            "constraint_name": str,
            "constraint_type": str   # PK / FK / UNIQUE
        }

        Returns:
            List[dict]
        """
        pass

    @abstractmethod
    def get_procedures(self):
        """
        Return stored procedures available in the database.

        Returns:
            List[str]
        """
        pass

    @abstractmethod
    def get_functions(self):
        """
        Return functions available in the database.

        Returns:
            List[str]
        """
        pass

    @abstractmethod
    def get_triggers(self):
        """
        Return triggers available in the database.

        Returns:
            List[str]
        """
        pass
