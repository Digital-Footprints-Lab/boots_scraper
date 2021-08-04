import csv
import pandas as pd
import sqlite3
from sqlite3 import Error
import warnings

warnings.filterwarnings("ignore")

db_name = "test017.db"

product_csv = "scrape_PoC.csv"
# product_csv = "scrape_dups.csv"
more_csv = "more.csv"
product_table = "products"
product_index = "product_index"

person_csv = "person_PoC.csv"
person_table = "persons"
person_index = "person_index"


def sqlite_connect(db_name):

    #~ connect to our working DB
    connection = sqlite3.connect(db_name)
    #~ c is the cursor object: executes SQL on DB table
    cursor = connection.cursor()

    return connection, cursor


def csv_to_new_sqlite_table(cursor,
                            connection,
                            csv,
                            table_name):

    """
    csv : name/path of the incoming csv file
    table_name : name you are giving to the new table
    """

    try:
        pd.read_csv(csv).to_sql(table_name,
                                connection,
                                if_exists="fail",
                                index=False)
    except Exception as e:
        print(e, csv, "not imported.")

    record_count = len(cursor.execute(f"SELECT * FROM {table_name}").fetchall())
    print(f"{record_count} records currently in table {table_name}.")

    connection.commit()


def create_index(cursor,
                 connection,
                 table_name,
                 index_name,
                 index_column):

    """
    ARGS: sql cursor + connection,
          name of table to index,
          name you are giving to your new index,
          which column to be indexed

    RETS: nothing, commits changes to SQLite DB.
    """

    try:
        cursor.execute(f"""CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
                           ON {table_name}({index_column});""")

        connection.commit()

    except Exception as e:
        print("Index creation failed", e)


def add_csv_lines_to_table(cursor, connection, csv_file, table_name):

    """
    ARGS: sql cursor + connection,
          input csv file to be added,
          name of the table to be added to

    Reads the incoming csv into a dataframe,
    then iterates through each row, inserting the relevant fields.
    If there are duplicate on unique index column, skip and do nothing.

    RETS: nothing, commits changes to the SQLite DB.
    """

    incoming_df = pd.read_csv(csv_file)

    for index, row in incoming_df.iterrows():
        print(row["productid"])
        cursor.execute(f"""INSERT INTO {table_name}
                           (productid, name, PDP_productPrice, details, long_description)
                           VALUES
                           ("{row["productid"]}",
                           "{row["name"]}",
                           "{row["PDP_productPrice"]}",
                           "{row["details"]}",
                           "{row["long_description"]}")
                           ON CONFLICT DO NOTHING""")

    connection.commit()


def apply_transaction_to_person(person, transaction_id):

    #! TODO
    pass


def main():

    #~ connect!
    connection, cursor = sqlite_connect(db_name)

    #~ bring product csv into sqlite table
    csv_to_new_sqlite_table(cursor,
                            connection,
                            product_csv,
                            product_table)
    #~ assert index on productid column
    create_index(cursor,
                 connection,
                 product_table,
                 product_index,
                 "productid")


    add_csv_lines_to_table(cursor, connection, more_csv, product_table)

    print(cursor.execute(f"SELECT * FROM products").fetchall())

    record_count = len(cursor.execute(f"SELECT * FROM products").fetchall())
    print(f"{record_count} records currently in table products.")


    #~ bring person csv into sqlite table, apply index
    csv_to_new_sqlite_table(cursor,
                            connection,
                            person_csv,
                            person_table)
    create_index(cursor,
                 connection,
                 person_table,
                 person_index,
                 "id")

    #~ close connection to DB
    connection.close()


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")

