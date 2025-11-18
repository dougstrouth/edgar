import duckdb
import sys
from pathlib import Path

# Add project root to sys.path to allow importing utils
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils.config_utils import AppConfig


def get_db_schema(con):
    tables = con.execute("SHOW TABLES;").fetchall()
    schema = {}
    for table in tables:
        table_name = table[0]
        columns = con.execute(f"DESCRIBE {table_name};").fetchall()
        schema[table_name] = columns
    return schema


def format_schema_as_markdown(schema):
    markdown = "# EDGAR Analytics - Data Dictionary\n\n"
    markdown += (
        "This document provides a detailed description of the database schema used in the EDGAR Analytics project. "
        "The database stores structured data parsed from SEC EDGAR bulk files and supplementary stock market data from Yahoo Finance.\n\n"
    )
    markdown += "## Database Engine\n\n*   **Type**: DuckDB\n\n"
    markdown += "## Table Definitions\n\n"

    for table_name, columns in schema.items():
        markdown += f"### `{table_name}`\n\n"
        markdown += "| Column Name | Data Type | Description |\n"
        markdown += "| :--- | :--- | :--- |\n"
        for column in columns:
            column_name, data_type, *_ = column
            markdown += f"| `{column_name}` | `{data_type}` | |\n"
        markdown += "\n"
    return markdown


if __name__ == "__main__":
    config = AppConfig(calling_script_path=Path(__file__))
    db_file = config.DB_FILE_STR
    print(f"Connecting to database: {db_file}")
    with duckdb.connect(database=db_file, read_only=True) as con:
        schema = get_db_schema(con)
    markdown_content = format_schema_as_markdown(schema)
    out = Path(__file__).resolve().parents[1] / "DATA_DICTIONARY.md"
    out.write_text(markdown_content)
    print("DATA_DICTIONARY.md has been updated.")
