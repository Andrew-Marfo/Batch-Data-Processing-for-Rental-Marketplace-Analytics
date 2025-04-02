import pandas as pd
from db_connection_utils.db_conn_utils import connect_to_database, create_table, insert_batch


# Prepare the dataframe
csv_path = "./data/data/apartment_attributes.csv"
df = pd.read_csv(csv_path)

# replacing NaN values with None
df = df.where(pd.notnull(df), None)

create_table_query = """
CREATE TABLE IF NOT EXISTS apartments_attributes (
    id BIGINT PRIMARY KEY,
    category VARCHAR(100),
    body TEXT,
    amenities TEXT,
    bathrooms INTEGER,
    bedrooms INTEGER,
    fee FLOAT,
    has_photo BOOLEAN,
    pets_allowed VARCHAR(50),
    price_display VARCHAR(50),
    price_type VARCHAR(50),
    square_feet INTEGER,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT
);
"""

insert_query = """
    INSERT INTO apartments_attributes (
        id, category, body, amenities, bathrooms, bedrooms, fee, has_photo,
        pets_allowed, price_display, price_type, square_feet, address,
        cityname, state, latitude, longitude
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        category=VALUES(category),
        body=VALUES(body),
        amenities=VALUES(amenities),
        bathrooms=VALUES(bathrooms),
        bedrooms=VALUES(bedrooms),
        fee=VALUES(fee),
        has_photo=VALUES(has_photo),
        pets_allowed=VALUES(pets_allowed),
        price_display=VALUES(price_display),
        price_type=VALUES(price_type),
        square_feet=VALUES(square_feet),
        address=VALUES(address),
        cityname=VALUES(cityname),
        state=VALUES(state),
        latitude=VALUES(latitude),
        longitude=VALUES(longitude);
"""

def main():
    connection = connect_to_database()
    if connection:
        try:
            create_table(connection, "apartments_attributes", create_table_query)
            insert_batch(connection, "apartments_attributes", df, insert_query)
        except Exception as e:
            print("❌Error:", e)
        finally:
            connection.close()
            print("✅Connection closed.")

if __name__ == "__main__":
    main()