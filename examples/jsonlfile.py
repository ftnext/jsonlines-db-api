"""A simple example showing the JSON Lines adapter."""
from shillelagh.backends.apsw.db import connect

if __name__ == "__main__":
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
