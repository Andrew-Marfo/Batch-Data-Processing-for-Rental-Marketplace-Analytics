import pandas as pd
from db_connection_utils.db_conn_utils import connect_to_database, create_table, insert_batch


# Prepare the dataframe
csv_path = "./data/data/bookings.csv"
df = pd.read_csv(csv_path)

# replacing NaN values with None
# df = df.where(pd.notnull(df), None)

create_table_query = """
CREATE TABLE IF NOT EXISTS bookings (
    booking_id BIGINT PRIMARY KEY,
    user_id BIGINT,
    apartment_id BIGINT,
    booking_date VaRCHAR(50),
    checkin_date VaRCHAR(50),
    checkout_date VaRCHAR(50),
    total_price FLOAT,
    currency VARCHAR(10),
    booking_status VARCHAR(50)
);
"""

insert_query = """
    INSERT INTO bookings (
        booking_id, user_id, apartment_id, booking_date, checkin_date, checkout_date, total_price, currency, booking_status
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        user_id=VALUES(user_id),
        apartment_id=VALUES(apartment_id),
        booking_date=VALUES(booking_date),
        checkin_date=VALUES(checkin_date),
        checkout_date=VALUES(checkout_date),
        total_price=VALUES(total_price),
        currency=VALUES(currency),
        booking_status=VALUES(booking_status);
"""

def main():
    connection = connect_to_database()
    if connection:
        try:
            create_table(connection, "bookings", create_table_query)
            insert_batch(connection, "bookings", df, insert_query)
        except Exception as e:
            print("❌Error:", e)
        finally:
            connection.close()
            print("✅Connection closed.")

if __name__ == "__main__":
    main()