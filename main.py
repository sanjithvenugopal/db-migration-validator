"""
Main Orchestrator for Database Migration Validator

This module:
- Discovers schemas and tables
- Executes all validators
- Collects results
- Writes Excel report
"""

import os
from dotenv import load_dotenv

from db_migration_validator.connectors.factory import ConnectorFactory
from db_migration_validator.validators.row_count import RowCountValidator
from db_migration_validator.validators.column import ColumnValidator
from db_migration_validator.validators.constraint import ConstraintValidator
from db_migration_validator.validators.routine import RoutineValidator
from db_migration_validator.reporting.excel_reporter import ExcelReporter


def build_db_config(prefix: str) -> dict:
    """
    Build database configuration dictionary from environment variables.
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
    Entry point for full validation execution.
    """
    # Load environment variables
    load_dotenv()

    # Read database types
    source_db_type = os.getenv("SOURCE_DB_TYPE")
    target_db_type = os.getenv("TARGET_DB_TYPE")
    output_file = os.getenv("OUTPUT_XLSX", "validation_report.xlsx")

    # Build configs
    source_config = build_db_config("SOURCE")
    target_config = build_db_config("TARGET")

    # Create connectors
    source = ConnectorFactory.create(source_db_type, source_config)
    target = ConnectorFactory.create(target_db_type, target_config)

    # Connect to databases
    source.connect()
    target.connect()

    # Initialize result containers
    row_count_results = []
    column_results = []
    constraint_results = []
    routine_results = []

    # Discover schemas (intersection only)
    source_schemas = set(source.list_schemas())
    target_schemas = set(target.list_schemas())
    common_schemas = sorted(source_schemas & target_schemas)

    for schema in common_schemas:
        # Discover tables
        source_tables = set(source.list_tables(schema))
        target_tables = set(target.list_tables(schema))
        common_tables = sorted(source_tables & target_tables)

        for table in common_tables:
            # Row count validation
            row_count_results.append(
                RowCountValidator.validate(source, target, schema, table)
            )

            # Column validation
            column_results.extend(
                ColumnValidator.validate(source, target, schema, table)
            )

            # Constraint validation
            constraint_results.extend(
                ConstraintValidator.validate(source, target, schema, table)
            )

    # Routine validation (DB-level, not table-level)
    routine_results.extend(
        RoutineValidator.validate(source, target)
    )

    # Write results to Excel
    reporter = ExcelReporter(output_file)
    reporter.write(
        row_counts=row_count_results,
        column_results=column_results,
        constraint_results=constraint_results,
        routine_results=routine_results
    )

    # Close connections
    source.close()
    target.close()

    print(f"\nValidation completed successfully.")
    print(f"Report generated: {output_file}")


if __name__ == "__main__":
    main()
