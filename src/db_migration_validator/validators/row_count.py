"""
RowCountValidator

Validates row counts between source and target databases.
"""

from typing import Dict
from db_migration_validator.connectors.base import BaseConnector


class RowCountValidator:
    """
    Validator to compare row counts for a table between two databases.
    """

    @staticmethod
    def validate(
        source: BaseConnector,
        target: BaseConnector,
        schema: str,
        table: str
    ) -> Dict:
        """
        Validate row count for a given table.

        Args:
            source (BaseConnector): Source database connector
            target (BaseConnector): Target database connector
            schema (str): Schema name
            table (str): Table name

        Returns:
            dict: Validation result
        """
        # Fetch row count from source database
        source_count = source.get_row_count(schema, table)

        # Fetch row count from target database
        target_count = target.get_row_count(schema, table)

        # Determine validation status
        status = "MATCH" if source_count == target_count else "MISMATCH"

        # Return structured result
        return {
            "schema": schema,
            "table": table,
            "check_type": "ROW_COUNT",
            "source_value": source_count,
            "target_value": target_count,
            "status": status
        }
