"""
RoutineValidator

Validates database routines between source and target databases.
Covers:
- Procedures
- Functions
- Triggers
"""

from typing import List, Dict
from db_migration_validator.connectors.base import BaseConnector


class RoutineValidator:
    """
    Validator to compare procedures, functions, and triggers
    between two databases.
    """

    @staticmethod
    def _validate_object(
        source_objects: List[str],
        target_objects: List[str],
        object_type: str
    ) -> List[Dict]:
        """
        Generic routine validation logic.

        Args:
            source_objects (List[str]): Objects from source DB
            target_objects (List[str]): Objects from target DB
            object_type (str): PROCEDURE / FUNCTION / TRIGGER

        Returns:
            List[dict]: Validation results
        """
        results = []

        # Normalize object names to uppercase
        source_set = {obj.upper() for obj in source_objects}
        target_set = {obj.upper() for obj in target_objects}

        # Union of all objects
        all_objects = sorted(source_set | target_set)

        for obj in all_objects:
            if obj in source_set and obj in target_set:
                status = "MATCH"
                source_value = "PRESENT"
                target_value = "PRESENT"
            elif obj in source_set:
                status = "MISMATCH"
                source_value = "PRESENT"
                target_value = "MISSING"
            else:
                status = "MISMATCH"
                source_value = "MISSING"
                target_value = "PRESENT"

            results.append({
                "object_type": object_type,
                "object_name": obj,
                "source_value": source_value,
                "target_value": target_value,
                "status": status
            })

        return results

    @staticmethod
    def validate(
        source: BaseConnector,
        target: BaseConnector
    ) -> List[Dict]:
        """
        Validate procedures, functions, and triggers.

        Args:
            source (BaseConnector): Source database connector
            target (BaseConnector): Target database connector

        Returns:
            List[dict]: Validation results
        """
        results = []

        # Validate procedures
        results.extend(
            RoutineValidator._validate_object(
                source.get_procedures(),
                target.get_procedures(),
                "PROCEDURE"
            )
        )

        # Validate functions
        results.extend(
            RoutineValidator._validate_object(
                source.get_functions(),
                target.get_functions(),
                "FUNCTION"
            )
        )

        # Validate triggers
        results.extend(
            RoutineValidator._validate_object(
                source.get_triggers(),
                target.get_triggers(),
                "TRIGGER"
            )
        )

        return results
