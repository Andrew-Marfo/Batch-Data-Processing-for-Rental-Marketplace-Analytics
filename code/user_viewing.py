import pandas as pd
from db_connection_utils.db_conn_utils import connect_to_database, create_table, insert_batch

# Prepare the dataframes
df = pd.read_csv('./data/data/user_viewing.csv')

# replacing NaN values with None
df = df.where(pd.notnull(df), None)

create_table_query = """
CREATE TABLE IF NOT EXISTS user_viewing (
    user_id BIGINT,
    apartment_id BIGINT,
    viewed_at VARCHAR(50),
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(50),
    PRIMARY KEY (user_id, apartment_id)
);
"""

insert_query = """
    INSERT INTO user_viewing (
            user_id, apartment_id, viewed_at, is_wishlisted, call_to_action
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            user_id=VALUES(user_id),
            apartment_id=VALUES(apartment_id),
            viewed_at = VALUES(viewed_at),
            is_wishlisted = VALUES(is_wishlisted),
            call_to_action = VALUES(call_to_action);
"""

def main():
    connection = connect_to_database()
    if connection:
        try:
            create_table(connection, "user_viewing", create_table_query)
            insert_batch(connection, "user_viewing", df, insert_query)
        except Exception as e:
            print("❌Error:", e)
        finally:
            connection.close()
            print("✅Connection closed.")

if __name__ == "__main__":
    main()