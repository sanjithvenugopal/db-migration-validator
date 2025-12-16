"""
ConnectorFactory

Responsible for returning the correct database connector
based on the database type provided in configuration.
"""

from .oracle import OracleConnector
from .redshift import RedshiftConnector


class ConnectorFactory:
    """
    Factory class to create database connectors.
    """

    @staticmethod
    def create(db_type: str, config: dict):
        """
        Create and return a database connector instance.

        Args:
            db_type (str): Type of database (oracle, redshift, etc.)
            config (dict): Connection configuration

        Returns:
            BaseConnector instance
        """
        db_type = db_type.lower()

        if db_type == "oracle":
            return OracleConnector(config)

        if db_type == "redshift":
            return RedshiftConnector(config)

        # If db_type is not supported
        raise ValueError(f"Unsupported database type: {db_type}")
