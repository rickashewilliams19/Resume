import requests
import schedule
import time
import pandas as pd
from datetime import datetime
import psycopg2

access_token = " " # Add the access token generated from get user details python file

# Dates to fetch data for (assuming the same month and year)
expiry_dates = ["2025-01-09", "2025-01-16", "2025-01-23", "2025-01-30"]

# PostgreSQL connection details
DB_NAME = " "
DB_USER = " "
DB_PASSWORD = " "
DB_HOST = " "
DB_PORT = " "

def fetch_option_chain_data():
    """Fetch option chain data for multiple expiry dates and save to PostgreSQL."""
    # Log the time the function is triggered
    trigger_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n>>> fetch_option_chain_data triggered at {trigger_time}")

    # We'll use the same timestamp for all rows in this fetch
    fetch_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    all_new_rows = []

    for expiry_date in expiry_dates:
        url = "https://api.upstox.com/v2/option/chain"
        params = {
            'instrument_key': 'NSE_INDEX|Nifty 50',
            'expiry_date': expiry_date
        }
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        # Make the GET request
        response = requests.get(url, headers=headers, params=params)

        # If the response is not OK, log and skip
        if response.status_code != 200:
            print(f"Failed to fetch data for expiry {expiry_date}: {response.status_code}")
            continue

        data = response.json()
        option_chain_data = data.get('data', [])

        # Build rows
        new_rows = []
        for item in option_chain_data:
            strike_price = item.get('strike_price')
            call_iv = item.get('call_options', {})\
                          .get('option_greeks', {})\
                          .get('iv', None)
            put_iv = item.get('put_options', {})\
                         .get('option_greeks', {})\
                         .get('iv', None)

            new_rows.append({
                "Timestamp": fetch_timestamp,
                "Expiry": expiry_date,
                "Strike": strike_price,
                "Call IV": call_iv,
                "Put IV": put_iv
            })
        
        all_new_rows.extend(new_rows)

    # Convert all new rows to a DataFrame
    if all_new_rows:
        df_new = pd.DataFrame(all_new_rows)
        print(f"Inserting {len(df_new)} rows for timestamp {fetch_timestamp}")
        save_to_postgres(df_new)
    else:
        print("No new data to insert for this run.")

def save_to_postgres(df):
    """Insert the given DataFrame rows into PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Create the table if it does not exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS option_chain (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            expiry_date DATE,
            call_iv DOUBLE PRECISION,
            strike DOUBLE PRECISION,
            put_iv DOUBLE PRECISION
        );
        """
        cursor.execute(create_table_query)

        # Insert query
        insert_query = """
        INSERT INTO option_chain 
        (timestamp, expiry_date, call_iv, strike, put_iv)
        VALUES (%s, %s, %s, %s, %s);
        """

        for _, row in df.iterrows():
            cursor.execute(
                insert_query,
                (
                    row["Timestamp"],
                    row["Expiry"],
                    row["Call IV"],
                    row["Strike"],
                    row["Put IV"]
                )
            )

        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully saved to PostgreSQL.")

    except Exception as e:
        print(f"Error saving to PostgreSQL: {e}")

# Schedule the job once every minute
schedule.every(1).hour.do(fetch_option_chain_data)

if __name__ == "__main__":
    # Optionally do one immediate fetch at startup:
    fetch_option_chain_data()

    print("\n>>> Starting the schedule loop...")
    while True:
        # This will check every second if it's time to run the job
        schedule.run_pending()
        time.sleep(1)