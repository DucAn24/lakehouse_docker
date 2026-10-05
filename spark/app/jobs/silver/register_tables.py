"""Register every silver table (common.tables) in Unity Catalog + Trino.

    spark-submit jobs/silver/register_tables.py [--table <name>]
"""

from common.catalog import main

if __name__ == "__main__":
    main("silver")
