import sys
import argparse
import csv
import pandas as pd
import sqlite3
from sqlite3 import Error
import warnings

warnings.filterwarnings("ignore")

db_name = "test0113.db"

product_csv = "scrape_PoC.csv"
# product_csv = "scrape_dups.csv"
more_csv = "more.csv"
product_table = "products"
product_index = "product_index"

person_csv = "person_PoC.csv"
person_table = "persons"
person_index = "person_index"


def args_setup():

    parser = argparse.ArgumentParser(description="SQLite Interfacer for Transactional Epidemiology",
                                     epilog="Example: python3 sql_ops.py --update_products")
    parser.add_argument("--update_products", action="store_true",
        help="Bring in new products to DB in as csv.")
    parser.add_argument("--update_persons", action="store_true",
        help="Bring in new persons to DB as csv.)")
    parser.add_argument("--join_transactions", action="store_true",
        help="Add products in transaction_csv to individual person table (or create table if new).)")

    args = parser.parse_args()

    return parser, args


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
    ARGS: sqlite cursor + connection,
          name of table to index,
          name of incoming csv file,
          name to apply to new table.

    RETS: nothing, commits changes to SQLite DB.
    """

    try:
        pd.read_csv(csv).to_sql(table_name,
                                connection,
                                if_exists="fail",
                                index=False)
    except Exception as e:
        print(e, csv, "not imported.")

    connection.commit()


def create_index(cursor,
                 connection,
                 table_name,
                 index_name,
                 index_column):

    """
    ARGS: sqlite cursor + connection,
          name of table to index,
          name you are giving to your new index,
          which column to be indexed

    RETS: nothing, commits changes to SQLite DB.
    """

    try:
        cursor.execute(f"""CREATE UNIQUE INDEX IF NOT EXISTS
                           {index_name} ON
                           {table_name}({index_column});""")

        connection.commit()

    except Exception as e:
        print("Index creation failed", e)


def add_csv_lines_to_table(cursor, connection, csv_file, table_name):

    """
    ARGS: sqlite cursor + connection,
          input csv file to be added,
          name of the table to be added to

    Reads the incoming csv into a dataframe,
    then iterates through each row, inserting the relevant fields.
    If there are duplicate on unique index column, skip and do nothing.

    RETS: nothing, commits changes to the SQLite DB.
    """

    incoming_df = pd.read_csv(csv_file)

    #! this cannot deal with unclean data. sql hates quotes and brackets. todo.
    #! [although cleaning should be elsewhere. raw dirty, DB clean.]
    for _, row in incoming_df.iterrows():
        cursor.execute(f"""INSERT INTO {table_name}
                           (productid, name, PDP_productPrice)
                           VALUES(
                           "{row["productid"]}",
                           "{row["name"]}",
                           "{row["PDP_productPrice"]}")
                           ON CONFLICT DO NOTHING""")

    connection.commit()


def join_transaction_to_person(cursor, connection, transaction_csv):

    #~ read transaction csv into df
    transaction_df = pd.read_csv(transaction_csv)

    #~ make a transactions table for this person (if new)
    table_name = transaction_df.iloc[0]["ID"]
    print(table_name)
    #! todo create new table if exists
    #! do we have to describe the table explicitly?
    #! i'd prefer to have the output of the JOIN to describe the table columns
    # cursor.execute(f"""CREATE TABLE IF NOT EXISTS {"table_name"}""")

    for _, row in transaction_df.iterrows():
        cursor.execute(f"""SELECT *
                           FROM persons
                           INNER JOIN products
                           ON persons.bootsid = {row["ID"]}
                           AND products.productid = {row["ITEM_CODE"]}""")
        result = cursor.fetchall()
        print(result)
        #! TODO insert each join to this person's product table table
        #! TODO include the transaction details, in particular time/date

    connection.commit()


def table_record_count(cursor, table_name):

    try:
        count = len(cursor.execute(f"SELECT * FROM {table_name}").fetchall())
        print(f"{count} records currently in table {table_name}.")
    except Exception as e:
        print(e)


def main():

    parser, args = args_setup()

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(0)

    #~ connect!
    connection, cursor = sqlite_connect(db_name)

    if args.update_products:
        #~ bring product csv into sqlite table
        csv_to_new_sqlite_table(cursor,
                                connection,
                                product_csv,
                                product_table)

        #~ adding new products to table to test duplicate prevention
        add_csv_lines_to_table(cursor, connection, more_csv, product_table)

        #~ index on productid column
        create_index(cursor,
                    connection,
                    product_table,
                    product_index,
                    "productid")

    if args.update_persons:
        #~ bring person csv into sqlite table, apply index
        csv_to_new_sqlite_table(cursor,
                                connection,
                                person_csv,
                                person_table)
        create_index(cursor,
                    connection,
                    person_table,
                    person_index,
                    "alspacid")

    transaction_csv = "boots_transaction_PoC.csv"
    if args.join_transactions:
        #~ take a csv of transactions,
        #~ pull person and product details from tables, join on
        #~ boots id and product id <->
        join_transaction_to_person(cursor,
                                   connection,
                                   transaction_csv)

    #~ debug count records
    table_record_count(cursor, product_table)
    table_record_count(cursor, person_table)

    #~ close connection to DB
    connection.close()


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")

