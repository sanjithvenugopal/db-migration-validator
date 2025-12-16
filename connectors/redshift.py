"""
RedshiftConnector

Concrete implementation of BaseConnector for Amazon Redshift.
Uses psycopg2 (PostgreSQL-compatible driver).

Responsible only for:
- Connecting to Redshift
- Fetching Redshift metadata
- Normalizing output to BaseConnector format
"""

import psycopg2
from typing import List, Dict
from .base import BaseConnector


class RedshiftConnector(BaseConnector):
    """
    Amazon Redshift database connector.
    """

    def connect(self):
        """
        Establish connection to Amazon Redshift.
        """
        self.connection = psycopg2.connect(
            host=self.config['host'],
            port=self.config['port'],
            dbname=self.config['db'],
            user=self.config['user'],
            password=self.config['password']
        )

    def close(self):
        """
        Close Redshift connection safely.
        """
        if self.connection:
            self.connection.close()
            self.connection = None

    def list_schemas(self) -> List[str]:
        """
        Return list of schemas in Redshift.
        """
        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            ORDER BY schema_name
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
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (schema,))

        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return tables

    def get_row_count(self, schema: str, table: str) -> int:
        """
        Return total row count for a table.
        """
        cursor = self.connection.cursor()

        query = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
        cursor.execute(query)

        count = cursor.fetchone()[0]
        cursor.close()

        return count

    def get_columns(self, schema: str, table: str) -> List[Dict]:
        """
        Return column metadata in normalized format.
        """
        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))

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
            FROM information_schema.table_constraints
            WHERE table_schema = %s
              AND table_name = %s
        """, (schema, table))

        constraints = []

        for row in cursor.fetchall():
            constraints.append({
                "constraint_name": row[0],
                "constraint_type": row[1]
            })

        cursor.close()
        return constraints

    def get_procedures(self) -> List[str]:
        """
        Return list of stored procedures in Redshift.
        (Redshift has limited stored procedure support.)
        """
        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT routine_name
            FROM information_schema.routines
            WHERE routine_type = 'PROCEDURE'
            ORDER BY routine_name
        """)

        procedures = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return procedures

    def get_functions(self) -> List[str]:
        """
        Return list of functions in Redshift.
        """
        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT routine_name
            FROM information_schema.routines
            WHERE routine_type = 'FUNCTION'
            ORDER BY routine_name
        """)

        functions = [row[0] for row in cursor.fetchall()]
        cursor.close()

        return functions

    def get_triggers(self) -> List[str]:
        """
        Redshift does not support triggers.
        Return empty list to satisfy BaseConnector contract.
        """
        return []
