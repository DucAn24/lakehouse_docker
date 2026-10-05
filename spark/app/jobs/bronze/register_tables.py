"""Register every bronze table (common.tables) in Unity Catalog + Trino.

    spark-submit jobs/bronze/register_tables.py [--table <name>]
"""

from common.catalog import main

if __name__ == "__main__":
    main("bronze")
