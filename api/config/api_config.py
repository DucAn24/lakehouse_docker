import os

# Path for SQL files
SQL_BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "sql")


def get_available_sql_files():
    try:
        files = os.listdir(SQL_BASE_PATH)
        return [f.replace(".sql", "") for f in files if f.endswith(".sql")]
    except Exception:
        return []


def validate_sql_file_exists(sql_file_name: str) -> bool:
    return sql_file_name in get_available_sql_files()


def get_sql_file_path(sql_file_name: str) -> str:
    if not sql_file_name.endswith(".sql"):
        sql_file_name += ".sql"
    return os.path.join(SQL_BASE_PATH, sql_file_name)
