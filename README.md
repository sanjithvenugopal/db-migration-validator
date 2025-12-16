# Database Migration Validation Framework

A Python-based framework to validate database migrations across heterogeneous systems
(On-Prem to Cloud, Cloud to Cloud).

## 🚀 Problem Statement
Database migrations often result in data quality issues such as:
- Row count mismatches
- Missing or altered columns
- Datatype and precision inconsistencies
- Missing constraints or database objects

Manual validation is error-prone and time-consuming.

## ✅ Solution
This project provides an automated, extensible framework to validate:
- Schemas and tables
- Row counts
- Column names, datatypes, precision, and scale
- Primary, foreign, and unique constraints
- Procedures, functions, and triggers
- Data quality anomalies

## 🧱 Architecture
- Modular Python library
- Pluggable database connectors
- Common metadata normalization layer
- Validator-based execution
- Excel-based audit reports

## 🗄️ Supported Databases
- Oracle
- Amazon Redshift
- PostgreSQL
- SQL Server
- MySQL
- Amazon Athena
- Snowflake (planned)

## 📊 Output
- Single Excel file with multiple sheets:
  - RowCounts
  - ColumnValidation
  - Constraints
  - Procedures
  - Functions
  - Triggers
  - Summary

## 🛠️ Tech Stack
- Python
- SQL
- Pandas
- OpenPyXL
- python-oracledb
- psycopg2
- GitHub

## 📌 Use Cases
- On-prem to cloud database migration validation
- Data quality reconciliation
- Pre- and post-migration audit checks

## 🔜 Roadmap
- CLI support
- Parallel execution
- Data-level sampling validation
- CI/CD integration

---

⭐ If you find this useful, feel free to star the repository.
