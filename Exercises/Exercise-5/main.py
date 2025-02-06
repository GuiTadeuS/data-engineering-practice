import os
import psycopg2
from enum import Enum


class SQLQueries(Enum):
    CREATE_ACCOUNT_TABLE = """
    create table if not exists account (
        customer_id integer primary key,
        first_name text,
        last_name text,
        address_1 text,
        address_2 text,
        city text,
        state text,
        zip_code text,
        join_date date
    );
    """

    CREATE_PRODUCT_TABLE = """
    create table if not exists product (
        product_id integer primary key,
        product_code integer,
        product_description text
    );

    create unique index idx_product_product_code ON product (product_code);
    """

    CREATE_TRANSACTION_TABLE = """
    create table if not exists transaction (
        transaction_id text primary key,
        transaction_date date,
        product_id integer REFERENCES product(product_id),
        product_code text,
        product_description text,
        quantity integer,
        account_id integer REFERENCES account(customer_id)
    );

    create unique index idx_transaction_product_id ON transaction (product_id);
    create unique index idx_transaction_product_code ON transaction (product_code);
    create unique index idx_transaction_account_id ON transaction (account_id);
    """


data_files = {
    "account": "accounts.csv",
    "product": "products.csv",
    "transaction": "transactions.csv"
    }


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pwd = "postgres"
    with psycopg2.connect(host=host, database=database, user=user, password=pwd) as conn:
        with conn.cursor() as cur:

            cur.execute(SQLQueries.CREATE_ACCOUNT_TABLE.value)
            cur.execute(SQLQueries.CREATE_PRODUCT_TABLE.value)
            cur.execute(SQLQueries.CREATE_TRANSACTION_TABLE.value)

            print("tables created")

            for table, filename in data_files.items():
                csv_path = os.path.join('data', filename)
                with open(csv_path, 'r') as csv_file:
                    cur.copy_expert(
                        sql=f"COPY {table} FROM STDIN WITH CSV HEADER",
                        file=csv_file
                    )
                    print(f"data inserted into {table}")

            print("process finished")


if __name__ == "__main__":
    main()
