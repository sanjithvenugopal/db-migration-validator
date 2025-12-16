"""
ExcelReporter

Responsible for writing validation results to a single Excel file
with multiple sheets.

This module has NO database logic and NO validation logic.
It only formats and writes output.
"""

import pandas as pd
from typing import List, Dict


class ExcelReporter:
    """
    Writes validation results into an Excel workbook.
    """

    def __init__(self, output_file: str):
        """
        Initialize Excel reporter.

        Args:
            output_file (str): Path to output Excel file
        """
        self.output_file = output_file

    def write(
        self,
        row_counts: List[Dict],
        column_results: List[Dict],
        constraint_results: List[Dict],
        routine_results: List[Dict]
    ):
        """
        Write all validation results to Excel.

        Args:
            row_counts (List[Dict]): Row count validation results
            column_results (List[Dict]): Column validation results
            constraint_results (List[Dict]): Constraint validation results
            routine_results (List[Dict]): Routine validation results
        """
        with pd.ExcelWriter(self.output_file, engine="openpyxl") as writer:

            # Write row count results
            if row_counts:
                pd.DataFrame(row_counts).to_excel(
                    writer,
                    sheet_name="RowCounts",
                    index=False
                )

            # Write column validation results
            if column_results:
                pd.DataFrame(column_results).to_excel(
                    writer,
                    sheet_name="Columns",
                    index=False
                )

            # Write constraint validation results
            if constraint_results:
                pd.DataFrame(constraint_results).to_excel(
                    writer,
                    sheet_name="Constraints",
                    index=False
                )

            # Write routine validation results
            if routine_results:
                pd.DataFrame(routine_results).to_excel(
                    writer,
                    sheet_name="Routines",
                    index=False
                )
