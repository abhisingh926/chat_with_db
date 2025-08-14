# import os
# from dotenv import load_dotenv
# from typing import Any
# from mcp.server.fastmcp import FastMCP
# import mysql.connector
# import yaml
# import pathlib
# import sys  # Required for printing errors to stderr
# from openai import OpenAI

# load_dotenv()

# # Initialize FastMCP server
# mcp = FastMCP("tool")

# # Load DB credentials
# try:
#     DB_HOST = os.getenv("DB_HOST")
#     DB_PORT = int(os.getenv("DB_PORT", 3306))
#     DB_USER = os.getenv("DB_USER")
#     DB_PASSWORD = os.getenv("DB_PASSWORD")
#     DB_NAME = os.getenv("DB_NAME")

#     missing_vars = [
#         var for var, val in {
#             "DB_HOST": DB_HOST,
#             "DB_USER": DB_USER,
#             "DB_PASSWORD": DB_PASSWORD,
#             "DB_NAME": DB_NAME,
#         }.items() if not val
#     ]
#     if missing_vars:
#         raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

#     DB_CONFIG = {
#         "host": DB_HOST,
#         "port": DB_PORT,
#         "user": DB_USER,
#         "password": DB_PASSWORD,
#         "database": DB_NAME,
#     }
# except Exception as e:
#     raise RuntimeError(f"[RuntimeError] Failed to configure DB: {e}")

# # Load schema descriptions
# def load_schema_descriptions(path: str = "schema.yaml") -> dict:
#     try:
#         schema_file = pathlib.Path(path)
#         if not schema_file.exists():
#             raise FileNotFoundError(f"Schema file not found: {path}")
#         with open(schema_file, "r") as f:
#             return yaml.safe_load(f)
#     except Exception as e:
#         print(f"[SchemaLoadError] Failed to load schema file: {e}", file=sys.stderr)
#         return {}

# # Load once during startup
# TABLE_DESCRIPTIONS = load_schema_descriptions()

# # @mcp.tool()
# # def get_table_info(module: str = "") -> dict:
# #     """
# #     Returns schema information for all or specific module.

# #     Args:
# #         module (str): Module name (e.g., "sales", "inventory"). If empty, return all.

# #     Returns:
# #         dict: Table and field descriptions.
# #     """
# #     try:
# #         if module:
# #             return TABLE_DESCRIPTIONS.get(module, {"error": f"No module named '{module}' found."})
# #         return TABLE_DESCRIPTIONS
# #     except Exception as e:
# #         return {"error": f"[UnexpectedError] {str(e)}"}


# @mcp.tool()
# def search_schema(keyword: str) -> dict:
#     """
#     Search for tables and fields in the schema by keyword.
#     """
#     results = {}
#     keyword_lower = keyword.lower()

#     for table, table_info in TABLE_DESCRIPTIONS.items():
#         if keyword_lower in table.lower() or keyword_lower in table_info.get("description", "").lower():
#             results[table] = table_info
#             continue

#         for field, field_info in table_info.get("fields", {}).items():
#             if keyword_lower in field.lower() or keyword_lower in field_info.get("description", "").lower():
#                 if table not in results:
#                     results[table] = {"fields": {}}
#                 results[table]["fields"][field] = field_info

#     return results if results else {"message": "No matches found."}

# @mcp.tool()
# def generate_sql(natural_query: str) -> dict:
#     """
#     Generate a SQL query from a natural language request using schema knowledge.
#     """
#     client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

#     prompt = f"""
#     You are an expert SQL generator. Use the following schema:
#     {yaml.dump(TABLE_DESCRIPTIONS, default_flow_style=False)}
    
#     Convert the following request into a MySQL SELECT query:
#     {natural_query}

#     Only return the SQL query, nothing else.
#     """

#     try:
#         response = client.chat.completions.create(
#             model="gpt-4o-mini",
#             messages=[{"role": "user", "content": prompt}]
#         )
#         sql = response.choices[0].message.content.strip()
#         return {"query": sql}
#     except Exception as e:
#         return {"error": str(e)}

# @mcp.tool()
# def run_sql_query(query: str) -> dict:
#     """
#     Executes a SELECT SQL query on the MySQL database.

#     Args:
#         query (str): The SQL query string.

#     Returns:
#         dict: The fetched rows or error.
#     """
#     try:
#         if not query.strip().lower().startswith("select"):
#             return {"error": "Only SELECT queries are allowed."}

#         try:
#             conn = mysql.connector.connect(**DB_CONFIG)
#         except mysql.connector.Error as conn_err:
#             return {"error": f"[ConnectionError] Could not connect to database: {conn_err}"}

#         try:
#             cursor = conn.cursor(dictionary=True)
#             cursor.execute(query)
#             result = cursor.fetchall()
#             return {"rows": result}
#         except mysql.connector.Error as query_err:
#             return {"error": f"[QueryError] Failed to execute query: {query_err}"}
#         finally:
#             cursor.close()
#             conn.close()

#     except Exception as e:
#         return {"error": f"[UnexpectedError] {str(e)}"}

# if __name__ == "__main__":
#     mcp.run(transport='stdio')





import os
import re
import sys
import yaml
import pathlib
from typing import Dict, Any, List, Tuple

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
import mysql.connector
from openai import OpenAI

# ----------------------------
# Bootstrapping
# ----------------------------
load_dotenv()
mcp = FastMCP("tool")

# ----------------------------
# Configuration
# ----------------------------
def _env_or_raise(name: str, default: Any = None, cast=None):
    val = os.getenv(name, default)
    if val is None or val == "":
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return cast(val) if cast else val

try:
    DB_HOST = _env_or_raise("DB_HOST")
    DB_PORT = _env_or_raise("DB_PORT", 3306, int)
    DB_USER = _env_or_raise("DB_USER")
    DB_PASSWORD = _env_or_raise("DB_PASSWORD")
    DB_NAME = _env_or_raise("DB_NAME")
    OPENAI_API_KEY = _env_or_raise("OPENAI_API_KEY")
    # OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    


    DB_CONFIG = {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "database": DB_NAME,
    }
except Exception as e:
    raise RuntimeError(f"[RuntimeError] Failed to configure environment: {e}")

# ----------------------------
# Schema loading
# ----------------------------
# def load_schema_descriptions(path: str = "schema.yaml") -> Dict[str, Any]:
#     try:
#         schema_file = pathlib.Path(path)
#         if not schema_file.exists():
#             raise FileNotFoundError(f"Schema file not found: {path}")
#         with open(schema_file, "r", encoding="utf-8") as f:
#             data = yaml.safe_load(f) or {}
#             if not isinstance(data, dict):
#                 raise ValueError("schema.yaml must be a mapping of table_name -> metadata")
#             return data
#     except Exception as e:
#         print(f"[SchemaLoadError] Failed to load schema file: {e}", file=sys.stderr)
#         return {}

def load_schema_descriptions(path: str = "schemas") -> dict:
    schema_dir = pathlib.Path(path)
    all_schemas = {}

    if not schema_dir.exists() or not schema_dir.is_dir():
        print(f"[SchemaLoadError] Schema directory not found: {path}", file=sys.stderr)
        return {}

    for file in schema_dir.glob("*.yaml"):
        try:
            with open(file, "r") as f:
                data = yaml.safe_load(f) or {}
                module_name = file.stem  # e.g., "sales" from "sales.yaml"
                all_schemas[module_name] = data
        except Exception as e:
            print(f"[SchemaLoadError] Failed to load {file.name}: {e}", file=sys.stderr)

    return all_schemas

TABLE_DESCRIPTIONS: Dict[str, Any] = load_schema_descriptions()
ALLOWED_TABLES: set = set(TABLE_DESCRIPTIONS.keys())

# ----------------------------
# Utilities
# ----------------------------
def extract_text_from_openai_response(resp) -> str:
    """
    Handles both the modern 'content is a list of blocks' format and older 'string'
    format from the OpenAI Python SDK.
    """
    try:
        # Newer style: list of content blocks
        blocks = resp.choices[0].message.content
        if isinstance(blocks, list):
            text = "".join(
                (b.get("text") or "")
                for b in blocks
                if isinstance(b, dict) and b.get("type") == "text"
            ).strip()
            if text:
                return text
        # Older style: direct string
        if isinstance(blocks, str):
            return blocks.strip()
    except Exception:
        pass
    # Fallback: try generic content access
    try:
        return str(resp.choices[0].message.content).strip()
    except Exception:
        return ""

def is_single_statement(query: str) -> bool:
    # Rough check to block multi-statements
    # Count semicolons; allow a single trailing semicolon
    q = query.strip()
    if not q:
        return False
    # Remove trailing semicolon if present
    if q.endswith(";"):
        q = q[:-1]
    return ";" not in q

def is_read_only_select(query: str) -> bool:
    q = query.strip().lower()
    if not q.startswith("select"):
        return False
    # Block writes/DDL keywords appearing anywhere
    forbidden = [
        "insert", "update", "delete", "drop", "alter", "create", "truncate",
        "grant", "revoke", "replace", "rename", "call", "do", "handler", "load",
        "lock", "unlock"
    ]
    return not any(f in q for f in forbidden)

def references_only_allowed_tables(query: str, allowed_tables: set) -> bool:
    """
    Very lightweight whitelist check. We simply ensure at least one allowed table appears,
    and block obvious system schemas.
    For stricter control you could parse the SQL AST with sqlglot or moz-sql-parser.
    """
    q = query.lower()
    system_schemas = ["information_schema", "mysql.", "performance_schema", "sys."]
    if any(s in q for s in system_schemas):
        return False

    # If no tables are whitelisted, allow nothing
    if not allowed_tables:
        return False

    # Require that at least one allowed table name is present
    return any(t.lower() in q for t in allowed_tables)

def ensure_limit(query: str, max_rows: int) -> str:
    """
    If the query has no LIMIT, append one. If it has a LIMIT, keep the smaller of the two.
    """
    pattern = re.compile(r"\blimit\s+(\d+)\b", re.IGNORECASE)
    match = pattern.search(query)
    if match:
        try:
            existing = int(match.group(1))
            if existing > max_rows:
                # Replace with tighter limit
                return pattern.sub(f"LIMIT {max_rows}", query)
            return query
        except Exception:
            # If parsing fails, just append a limit
            return query.rstrip(";") + f" LIMIT {max_rows}"
    else:
        return query.rstrip(";") + f" LIMIT {max_rows}"

def pick_relevant_tables(natural_query: str, max_tables: int = 6) -> List[str]:
    """
    Simple keyword scoring to pick relevant tables from TABLE_DESCRIPTIONS.
    """
    nq = (natural_query or "").lower()
    if not nq:
        return []
    scores: List[Tuple[str, int]] = []
    for table, info in TABLE_DESCRIPTIONS.items():
        score = 0
        tdesc = str(info.get("description", "")).lower()
        if table.lower() in nq:
            score += 5
        score += sum(1 for w in nq.split() if w in tdesc)

        fields = info.get("fields", {}) or {}
        for fname, finfo in fields.items():
            if fname.lower() in nq:
                score += 2
            fdesc = str((finfo or {}).get("description", "")).lower()
            score += sum(1 for w in nq.split() if w in fdesc)
        if score > 0:
            scores.append((table, score))
    scores.sort(key=lambda x: x[1], reverse=True)
    return [t for t, _ in scores[:max_tables]]

def schema_subset_yaml(tables: List[str]) -> str:
    partial = {t: TABLE_DESCRIPTIONS[t] for t in tables if t in TABLE_DESCRIPTIONS}
    return yaml.safe_dump(partial, default_flow_style=False, allow_unicode=True)

# ----------------------------
# MCP Tools
# ----------------------------
@mcp.tool()
def get_table_info(module: str = "") -> Dict[str, Any]:
    """
    Returns schema information for a specific table (module) or all tables.
    """
    try:
        if module:
            return {module: TABLE_DESCRIPTIONS.get(module, {"error": f"No table named '{module}' found."})}
        return TABLE_DESCRIPTIONS
    except Exception as e:
        return {"error": f"[UnexpectedError] {str(e)}"}

@mcp.tool()
def search_schema(keyword: str) -> Dict[str, Any]:
    """
    Search for tables and fields in the schema by keyword.
    """
    try:
        if not keyword:
            return {"error": "Keyword cannot be empty."}

        results: Dict[str, Any] = {}
        k = keyword.lower()

        for table, table_info in TABLE_DESCRIPTIONS.items():
            t_hit = k in table.lower() or k in str(table_info.get("description", "")).lower()
            fields_hit: Dict[str, Any] = {}
            for field, field_info in (table_info.get("fields", {}) or {}).items():
                if (
                    k in field.lower()
                    or k in str((field_info or {}).get("description", "")).lower()
                ):
                    fields_hit[field] = field_info

            if t_hit or fields_hit:
                results[table] = {
                    **table_info,
                    **({"fields": fields_hit} if fields_hit else {})
                }

        return results if results else {"message": "No matches found."}
    except Exception as e:
        return {"error": f"[UnexpectedError] {str(e)}"}

@mcp.tool()
def generate_sql(natural_query: str, restrict_to_tables_csv: str = "") -> Dict[str, Any]:
    """
    Generate a MySQL SELECT query from a natural-language request using schema knowledge.
    You can optionally pass a comma-separated list of tables to restrict the context.
    """
    try:
        if not natural_query or not natural_query.strip():
            return {"error": "natural_query cannot be empty."}

        # Build schema context (smaller is cheaper + more accurate)
        tables = []
        if restrict_to_tables_csv.strip():
            tables = [t.strip() for t in restrict_to_tables_csv.split(",") if t.strip()]
        if not tables:
            tables = pick_relevant_tables(natural_query) or list(ALLOWED_TABLES)[:6]

        schema_yaml = schema_subset_yaml(tables)

        prompt = f"""
You are an expert MySQL SQL generator. You MUST produce a single, read-only SELECT statement that works on MySQL.
Schema (YAML) for relevant tables:
---
{schema_yaml}
---

User request:
{natural_query}

Constraints:
- ONLY one statement.
- SELECT only (no DDL/DML).
- Prefer explicit column lists over SELECT * when possible.
- Add reasonable JOINs and WHERE filters as needed.
- Do not reference tables outside the provided schema.
- Do not include explanations, only return the SQL.
"""

        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
        sql = extract_text_from_openai_response(resp)
        # Strip code fences if present
        sql = re.sub(r"^```(?:sql)?\s*|\s*```$", "", sql.strip(), flags=re.IGNORECASE)
        if not sql:
            return {"error": "Model returned empty SQL."}
        return {"query": sql, "tables_context": tables}
    except Exception as e:
        return {"error": f"[OpenAIError] {str(e)}"}

@mcp.tool()
def run_sql_query(query: str, max_rows: int = 200) -> Dict[str, Any]:
    """
    Executes a read-only SELECT SQL query on the MySQL database with safety checks.
    """
    try:
        if not query or not query.strip():
            return {"error": "Query cannot be empty."}
        if not is_single_statement(query):
            return {"error": "Only a single statement is allowed."}
        if not is_read_only_select(query):
            return {"error": "Only read-only SELECT queries are allowed."}
        if not references_only_allowed_tables(query, ALLOWED_TABLES):
            return {"error": "Query references unauthorized tables or system schemas."}

        safe_query = ensure_limit(query, max_rows=max(1, int(max_rows)))

        try:
            conn = mysql.connector.connect(**DB_CONFIG)
        except mysql.connector.Error as conn_err:
            return {"error": f"[ConnectionError] Could not connect to database: {conn_err}"}

        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(safe_query)
            rows = cursor.fetchall()
            return {"rows": rows, "applied_query": safe_query}
        except mysql.connector.Error as query_err:
            return {"error": f"[QueryError] Failed to execute query: {query_err}", "applied_query": safe_query}
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        return {"error": f"[UnexpectedError] {str(e)}"}

@mcp.tool()
def ask_db(natural_query: str, max_rows: int = 200) -> Dict[str, Any]:
    """
    End-to-end helper: NL -> SQL -> Results.
    Picks relevant tables, generates SQL, applies safety checks, runs it, returns rows + SQL.
    """
    try:
        gen = generate_sql(natural_query)
        if "error" in gen:
            return gen
        sql = gen.get("query", "")
        if not sql:
            return {"error": "Failed to generate SQL."}

        exec_res = run_sql_query(sql, max_rows=max_rows)
        # Include the candidate SQL and the context tables for transparency
        return {
            "query": sql,
            "tables_context": gen.get("tables_context", []),
            **exec_res
        }
    except Exception as e:
        return {"error": f"[UnexpectedError] {str(e)}"}

# ----------------------------
# Entrypoint
# ----------------------------
if __name__ == "__main__":
    mcp.run(transport="stdio")