"""A simple example showing the JSON Lines adapter."""
import argparse
import logging

from shillelagh.backends.apsw.db import connect

FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    connection = connect(":memory:")
    cursor = connection.cursor()

    SQL = '''SELECT * FROM "test.jsonl"'''
    print(SQL)
    for row in cursor.execute(SQL):
        print(row)
    print("==")

    SQL = """SELECT * FROM "test.jsonl" WHERE "index" > 11"""
    print(SQL)
    for row in cursor.execute(SQL):
        print(row)
    print("==")
