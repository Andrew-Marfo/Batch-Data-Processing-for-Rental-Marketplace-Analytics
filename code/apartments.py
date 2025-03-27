import pandas as pd
from db_connection_utils.db_conn_utils import connect_to_database, create_table, insert_batch


# Prepare the dataframe
csv_path = "data/data/apartments.csv"
df = pd.read_csv(csv_path)

# replacing NaN values with None
df = df.where(pd.notnull(df), None)

create_table_query = """
CREATE TABLE IF NOT EXISTS apartments (
    id BIGINT PRIMARY KEY,
    title VARCHAR(255),
    source VARCHAR(100),
    price FLOAT,
    currency VARCHAR(10),
    listing_created_on VARCHAR(10),
    is_active BOOLEAN,
    last_modified_timestamp VARCHAR(10)
);
"""

insert_query = """
    INSERT INTO apartments (
        id, title, source, price, currency, listing_created_on, is_active, last_modified_timestamp
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        title=VALUES(title),
        source=VALUES(source),
        price=VALUES(price),
        currency=VALUES(currency),
        listing_created_on=VALUES(listing_created_on),
        is_active=VALUES(is_active),
        last_modified_timestamp=VALUES(last_modified_timestamp);
"""

def main():
    connection = connect_to_database()
    if connection:
        try:
            create_table(connection, "apartments", create_table_query)
            insert_batch(connection, "apartments", df, insert_query)
        except Exception as e:
            print("❌Error:", e)
        finally:
            connection.close()
            print("✅Connection closed.")

if __name__ == "__main__":
    main()