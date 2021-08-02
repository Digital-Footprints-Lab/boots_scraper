import csv
import pandas as pd
import sqlite3
from sqlite3 import Error
import warnings

warnings.filterwarnings("ignore")

db_name = "test001.db"

product_csv = "scrape_PoC.csv"
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


def csv_to_sqlite_table_and_add_index(cursor,
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
    except ValueError as e:
        print(e, csv, "not imported.")

    record_count = len(cursor.execute(f"SELECT * FROM {table_name}").fetchall())
    print(f"{record_count} records currently in table {table_name}.")

    #! ok, so making the table and populating it from the csv is fine,
    #! but how can we append new scraped csvs, ignoring duplicates but updating price?
    #! and adding scrape date? this function is too coarse.


def create_index(cursor, table_name, index_name, index_column):

    """
    index_name : name you are giving to your new index
    index_column : name of existing column in table to assign as index
    """

    cursor.execute(f"""CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
                  ON {table_name}({index_column});""")



def apply_transaction_to_person(person, transaction_id):

    #! TODO
    pass


def main():

    #~ connect!
    connection, cursor = sqlite_connect(db_name)

    #~ bring product csv into sqlite table, apply index
    csv_to_sqlite_table_and_add_index(cursor,
                                      connection,
                                      product_csv,
                                      product_table)
    create_index(cursor,
                 product_table,
                 product_index,
                 "productid")

    #~ bring person csv into sqlite table, apply index
    csv_to_sqlite_table_and_add_index(cursor,
                                      connection,
                                      person_csv,
                                      person_table)
    create_index(cursor,
                 person_table,
                 person_index,
                 "id")

    #~ save changes and close DB
    connection.commit()
    connection.close()


if __name__ == "__main__":

    main()


