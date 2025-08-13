import pymysql
import yaml
import os
from dotenv import load_dotenv
import requests

# ------------------------
# CONFIGURATION
# ------------------------
SCHEMA_FILE = 'schema.yaml'

# Load environment variables
load_dotenv()

# DB config
try:
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

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
        "cursorclass": pymysql.cursors.DictCursor,
    }
except Exception as e:
    raise RuntimeError(f"[RuntimeError] Failed to configure DB: {e}")

# ------------------------
# AI Description Generator
# ------------------------
def generate_ai_description(prompt):
    """Generate description using OpenAI API, fallback to generic text if API key missing."""
    if not OPENAI_API_KEY:
        return f"{prompt} (auto-generated description)"

    try:
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        }
        data = {
            "model": "gpt-4o-mini",  # Fast, cost-effective model
            "messages": [
                {"role": "system", "content": "You are a helpful assistant that writes short, clear descriptions for database tables and fields."},
                {"role": "user", "content": f"Write a short description for: {prompt}"}
            ],
            "max_tokens": 50
        }

        response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=data)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"⚠️ AI description generation failed: {e}")
        return f"{prompt} (auto-generated description)"

# ------------------------
# DB Functions
# ------------------------
def get_all_tables(cursor):
    cursor.execute("SHOW TABLES;")
    key = list(cursor.fetchone().keys())[0]
    cursor.execute("SHOW TABLES;")
    return [row[key] for row in cursor.fetchall()]

def get_table_description(cursor, table_name):
    cursor.execute(f"SHOW TABLE STATUS LIKE %s;", (table_name,))
    result = cursor.fetchone()
    return result.get("Comment", "") if result else ""

def get_table_columns(cursor, table_name):
    cursor.execute(f"SHOW FULL COLUMNS FROM `{table_name}`;")
    return cursor.fetchall()

# ------------------------
# Main Schema Generation
# ------------------------
def generate_schema(connection):
    schema = {}

    with connection.cursor() as cursor:
        tables = get_all_tables(cursor)

        for table in tables:
            print(f"📦 Processing table: {table}")
            try:
                table_desc = get_table_description(cursor, table)
                if not table_desc:
                    table_desc = generate_ai_description(f"database table named '{table}'")

                columns = get_table_columns(cursor, table)

                table_data = {
                    "description": table_desc,
                    "fields": {}
                }

                for column in columns:
                    field_name = column["Field"]
                    field_type = column["Type"]
                    field_comment = column["Comment"] or ""
                    if not field_comment:
                        field_comment = generate_ai_description(
                            f"field '{field_name}' of type '{field_type}' in table '{table}'"
                        )

                    table_data["fields"][field_name] = {
                        "type": field_type,
                        "description": field_comment
                    }

                schema[table] = table_data

            except Exception as e:
                print(f"❌ Error processing table `{table}`: {e}")

    return schema

# ------------------------
# File Writer
# ------------------------
def write_schema_to_file(schema):
    with open(SCHEMA_FILE, 'w') as f:
        yaml.dump(schema, f, sort_keys=False, default_flow_style=False)
    print(f"\n✅ YAML file `{SCHEMA_FILE}` updated successfully.")

# ------------------------
# Main Runner
# ------------------------
def main():
    try:
        connection = pymysql.connect(**DB_CONFIG)
        schema = generate_schema(connection)
        write_schema_to_file(schema)
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()

if __name__ == "__main__":
    main()