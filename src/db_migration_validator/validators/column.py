"""
ColumnValidator

Validates column structure between source and target tables.
Checks:
- Column existence
- Datatype
- Length
- Precision
- Scale
"""

from typing import List, Dict
from db_migration_validator.connectors.base import BaseConnector


class ColumnValidator:
    """
    Validator to compare column metadata between two databases.
    """

    @staticmethod
    def validate(
        source: BaseConnector,
        target: BaseConnector,
        schema: str,
        table: str
    ) -> List[Dict]:
        """
        Validate column structure for a table.

        Args:
            source (BaseConnector): Source database connector
            target (BaseConnector): Target database connector
            schema (str): Schema name
            table (str): Table name

        Returns:
            List[dict]: List of validation results (one per column)
        """
        results = []

        # Fetch column metadata from source and target
        source_columns = source.get_columns(schema, table)
        target_columns = target.get_columns(schema, table)

        # Convert column lists to dictionaries keyed by column name
        source_map = {col["column_name"].upper(): col for col in source_columns}
        target_map = {col["column_name"].upper(): col for col in target_columns}

        # Union of all column names
        all_columns = sorted(set(source_map.keys()) | set(target_map.keys()))

        for column_name in all_columns:
            source_col = source_map.get(column_name)
            target_col = target_map.get(column_name)

            # Case 1: Column missing in target
            if source_col and not target_col:
                results.append({
                    "schema": schema,
                    "table": table,
                    "column": column_name,
                    "check_type": "COLUMN_EXISTENCE",
                    "source_value": "PRESENT",
                    "target_value": "MISSING",
                    "status": "MISMATCH"
                })
                continue

            # Case 2: Column missing in source
            if target_col and not source_col:
                results.append({
                    "schema": schema,
                    "table": table,
                    "column": column_name,
                    "check_type": "COLUMN_EXISTENCE",
                    "source_value": "MISSING",
                    "target_value": "PRESENT",
                    "status": "MISMATCH"
                })
                continue

            # Case 3: Column exists in both → compare attributes
            checks = {
                "DATA_TYPE": (
                    source_col["data_type"],
                    target_col["data_type"]
                ),
                "LENGTH": (
                    source_col["length"],
                    target_col["length"]
                ),
                "PRECISION": (
                    source_col["precision"],
                    target_col["precision"]
                ),
                "SCALE": (
                    source_col["scale"],
                    target_col["scale"]
                )
            }

            for check_name, (src_val, tgt_val) in checks.items():
                status = "MATCH" if src_val == tgt_val else "MISMATCH"

                results.append({
                    "schema": schema,
                    "table": table,
                    "column": column_name,
                    "check_type": check_name,
                    "source_value": src_val,
                    "target_value": tgt_val,
                    "status": status
                })

        return results
