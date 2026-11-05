"""Run a query and print its result as an Arrow table.

Credentials come from Application Default Credentials (ADC):

    python -m pip install "bigquery-adbc[dbapi]"
    python examples/query.py my-project "SELECT 1 AS n"
"""

import sys

from bigquery_adbc import dbapi


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit(f"usage: {sys.argv[0]} PROJECT SQL")
    project, sql = sys.argv[1:]

    with dbapi.connect(f"bigquery:///{project}", autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute(sql)
        print(cursor.fetch_arrow_table())


if __name__ == "__main__":
    main()
