import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

load_dotenv()

# db connection function
def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("HOST"),
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            database=os.getenv("DATABASE"),
            port=os.getenv("DB_PORT", "3306")
        )
        if connection.is_connected():
            print("✅Connected to MySQL.")
            return connection
    except Error as e:
        print("❌Connection error:", e)
        return None

 # create table function
def create_table(connection, table_name, create_table_query):
    with connection.cursor() as cursor:
        cursor.execute(create_table_query)
        connection.commit()
        print(f"Table '{table_name}' ready.")

# insert batch function
def insert_batch(connection, table_name, df, insert_query, batch_size=5000):
    data_tuples = [tuple(row) for row in df.to_numpy()]
    with connection.cursor() as cursor:
        for i in range(0, len(data_tuples), batch_size):
            cursor.executemany(insert_query, data_tuples[i:i + batch_size])
            connection.commit()
            print(f"✅Inserted {i + batch_size} rows into the {table_name} table.")
        print(f"✅Data successfully inserted into the {table_name} table.")
