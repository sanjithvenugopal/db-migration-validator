"""
ConstraintValidator

Validates table-level constraints between source and target databases.
Checks:
- Primary Keys
- Foreign Keys
- Unique Constraints
"""

from typing import List, Dict
from db_migration_validator.connectors.base import BaseConnector


class ConstraintValidator:
    """
    Validator to compare constraints between two databases.
    """

    @staticmethod
    def validate(
        source: BaseConnector,
        target: BaseConnector,
        schema: str,
        table: str
    ) -> List[Dict]:
        """
        Validate constraints for a table.

        Args:
            source (BaseConnector): Source database connector
            target (BaseConnector): Target database connector
            schema (str): Schema name
            table (str): Table name

        Returns:
            List[dict]: Validation results
        """
        results = []

        # Fetch constraints from source and target
        source_constraints = source.get_constraints(schema, table)
        target_constraints = target.get_constraints(schema, table)

        # Convert constraints into lookup maps
        source_map = {
            c["constraint_name"].upper(): c["constraint_type"]
            for c in source_constraints
        }
        target_map = {
            c["constraint_name"].upper(): c["constraint_type"]
            for c in target_constraints
        }

        # Union of all constraint names
        all_constraints = sorted(set(source_map.keys()) | set(target_map.keys()))

        for constraint_name in all_constraints:
            src_type = source_map.get(constraint_name)
            tgt_type = target_map.get(constraint_name)

            # Constraint missing in target
            if src_type and not tgt_type:
                results.append({
                    "schema": schema,
                    "table": table,
                    "constraint": constraint_name,
                    "check_type": "CONSTRAINT_EXISTENCE",
                    "source_value": src_type,
                    "target_value": "MISSING",
                    "status": "MISMATCH"
                })
                continue

            # Constraint missing in source
            if tgt_type and not src_type:
                results.append({
                    "schema": schema,
                    "table": table,
                    "constraint": constraint_name,
                    "check_type": "CONSTRAINT_EXISTENCE",
                    "source_value": "MISSING",
                    "target_value": tgt_type,
                    "status": "MISMATCH"
                })
                continue

            # Constraint exists in both → compare type
            status = "MATCH" if src_type == tgt_type else "MISMATCH"

            results.append({
                "schema": schema,
                "table": table,
                "constraint": constraint_name,
                "check_type": "CONSTRAINT_TYPE",
                "source_value": src_type,
                "target_value": tgt_type,
                "status": status
            })

        return results
