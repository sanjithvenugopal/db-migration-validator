"""
OracleConnector

Concrete implementation of BaseConnector for Oracle databases.
Uses python-oracledb (thin mode, no Instant Client required).

This class is responsible ONLY for:
- Connecting to Oracle
- Fetching metadata
- Normalizing Oracle-specific outputs
"""

import oracledb
from typing import List, Dict
from .base import BaseConnector


class OracleConnector(BaseConnector):
    """
    Oracle database connector.
    Implements all abstract methods defined in BaseConnector.
    """

    def connect(self):
        """
        Establish a connection to the Oracle database.

        Uses configuration provided during initialization.
        """
        # Build DSN string: host:port/service_name
        dsn = f"{self.config['host']}:{self.config['port']}/{self.config['db']}"

        # Create Oracle connection
        self.connection = oracledb.connect(
            user=self.config['user'],
            password=self.config['password'],
            dsn=dsn
        )

    def close(self):
        """
        Close Oracle connection safely.
        """
        if self.connection:
            self.connection.close()
            self.connection = None

    def list_schemas(self) -> List[str]:
        """
        Return list of schemas (owners) in Oracle.
        """
        cursor = self.connection.cursor()

        # Fetch all schema owners
        cursor.execute("""
            SELECT username
            FROM all_users
            ORDER BY username
        """)

        schemas = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return schemas

    def list_tables(self, schema: str) -> List[str]:
        """
        Return all tables for a given schema.
        """
        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT table_name
            FROM all_tables
            WHERE owner = :schema
            ORDER BY table_name
        """, {"schema": schema.upper()})

        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return tables

    def get_row_count(self, schema: str, table: str) -> int:
        """
        Return total row count for a table.
        """
        cursor = self.connection.cursor()

        # Fully qualified table name
        query = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
        cursor.execute(query)

        count = cursor.fetchone()[0]
        cursor.close()

        return count

    def get_columns(self, schema: str, table: str) -> List[Dict]:
        """
        Return column metadata for a table in normalized format.
        """
        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT
                column_name,
                data_type,
                data_length,
                data_precision,
                data_scale
            FROM all_tab_columns
            WHERE owner = :schema
              AND table_name = :table
            ORDER BY column_id
        """, {
            "schema": schema.upper(),
            "table": table.upper()
        })

        columns = []

        for row in cursor.fetchall():
            columns.append({
                "column_name": row[0],
                "data_type": row[1],
                "length": row[2],
                "precision": row[3],
                "scale": row[4]
            })

        cursor.close()
        return columns

    def get_constraints(self, schema: str, table: str) -> List[Dict]:
        """
        Return constraints (PK, FK, UNIQUE) for a table.
        """
        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT
                constraint_name,
                constraint_type
            FROM all_constraints
            WHERE owner = :schema
              AND table_name = :table
              AND constraint_type IN ('P', 'R', 'U')
        """, {
            "schema": schema.upper(),
            "table": table.upper()
        })

        constraints = []

        for row in cursor.fetchall():
            constraints.append({
                "constraint_name": row[0],
                "constraint_type": row[1]  # P=PK, R=FK, U=UNIQUE
            })

        cursor.close()
        return constraints

    def get_procedures(self) -> List[str]:
        """
        Return list of stored procedures in Oracle.
        """
        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT object_name
            FROM user_objects
            WHERE object_type = 'PROCEDURE'
            ORDER BY object_name
        """)

        procedures = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return procedures

    def get_functions(self) -> List[str]:
        """
        Return list of functions in Oracle.
        """
        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT object_name
            FROM user_objects
            WHERE object_type = 'FUNCTION'
            ORDER BY object_name
        """)

        functions = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return functions

    def get_triggers(self) -> List[str]:
        """
        Return list of triggers in Oracle.
        """
        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT trigger_name
            FROM user_triggers
            ORDER BY trigger_name
        """)

        triggers = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return triggers
