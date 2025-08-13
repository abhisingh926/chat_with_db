import os
from dotenv import load_dotenv
from typing import Any
from mcp.server.fastmcp import FastMCP
import mysql.connector
import yaml
import pathlib
import sys  # Required for printing errors to stderr

load_dotenv()

# Initialize FastMCP server
mcp = FastMCP("tool")

# Load DB credentials
try:
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")

    missing_vars = [
        var for var, val in {
            "DB_HOST": DB_HOST,
            "DB_USER": DB_USER,
            "DB_PASSWORD": DB_PASSWORD,
            "DB_NAME": DB_NAME,
        }.items() if not val
    ]
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

    DB_CONFIG = {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "database": DB_NAME,
    }
except Exception as e:
    raise RuntimeError(f"[RuntimeError] Failed to configure DB: {e}")

# Load schema descriptions
def load_schema_descriptions(path: str = "schema.yaml") -> dict:
    try:
        schema_file = pathlib.Path(path)
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {path}")
        with open(schema_file, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"[SchemaLoadError] Failed to load schema file: {e}", file=sys.stderr)
        return {}

# Load once during startup
TABLE_DESCRIPTIONS = load_schema_descriptions()

@mcp.tool()
def get_table_info(module: str = "") -> dict:
    """
    Returns schema information for all or specific module.

    Args:
        module (str): Module name (e.g., "sales", "inventory"). If empty, return all.

    Returns:
        dict: Table and field descriptions.
    """
    try:
        if module:
            return TABLE_DESCRIPTIONS.get(module, {"error": f"No module named '{module}' found."})
        return TABLE_DESCRIPTIONS
    except Exception as e:
        return {"error": f"[UnexpectedError] {str(e)}"}

@mcp.tool()
def run_sql_query(query: str) -> dict:
    """
    Executes a SELECT SQL query on the MySQL database.

    Args:
        query (str): The SQL query string.

    Returns:
        dict: The fetched rows or error.
    """
    try:
        if not query.strip().lower().startswith("select"):
            return {"error": "Only SELECT queries are allowed."}

        try:
            conn = mysql.connector.connect(**DB_CONFIG)
        except mysql.connector.Error as conn_err:
            return {"error": f"[ConnectionError] Could not connect to database: {conn_err}"}

        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query)
            result = cursor.fetchall()
            return {"rows": result}
        except mysql.connector.Error as query_err:
            return {"error": f"[QueryError] Failed to execute query: {query_err}"}
        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        return {"error": f"[UnexpectedError] {str(e)}"}

if __name__ == "__main__":
    mcp.run(transport='stdio')