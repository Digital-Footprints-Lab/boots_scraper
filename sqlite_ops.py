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

#~ connect to our working DB
connection = sqlite3.connect(db_name)
#~ c is the cursor object: executes SQL on DB table
c = connection.cursor()


def csv_to_sqlite_table_and_add_index(csv, table_name):

    try:
        pd.read_csv(csv).to_sql(table_name,
                                connection,
                                if_exists="fail",
                                index=False)
    except ValueError as e:
        print(e, csv, "not imported.")

    record_count = len(c.execute(f"SELECT * FROM {table_name}").fetchall())
    print(f"{record_count} records currently in table {table_name}.")


def create_index(table_name, index_name, index_column_name):

    c.execute(f"""CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
                  ON {table_name}({index_column_name});""")


#! ok, so making the table and populating it from the csv is fine,
#! but how can we append new scraped csvs, ignoring duplicates but updating price?
#! and adding scrape date?


def apply_transaction_to_person(person, transaction_id):

    #! TODO
    pass


def main():

    #~ bring product csv into sqlite table, apply index
    csv_to_sqlite_table_and_add_index(product_csv, product_table)
    create_index(product_table, product_index, "productid")

    #~ bring person csv into sqlite table, apply index
    csv_to_sqlite_table_and_add_index(person_csv, person_table)
    create_index(person_table, person_index, "id")

    #~ save changes and close DB
    connection.commit()
    connection.close()


if __name__ == "__main__":

    main()


