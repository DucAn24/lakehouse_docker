"""Register every gold table (common.tables) in Unity Catalog + Trino.

    spark-submit jobs/gold/register_tables.py [--table <name>]
"""

from common.catalog import main

if __name__ == "__main__":
    main("gold")
