# Created by: @pusreenath https://github.com/pusreenath on 6th May 2023

import psycopg2
import pandas as pd
import requests

def retrieve_data():
    # Connect to the database
    conn = psycopg2.connect(host="localhost", user="postgres", password="postgres@123", port=5432, database="phonepe_pulse")
    curr = conn.cursor()
    # Fetch the data from the database
    query1 = """select * from trdata_vw"""
    query2 = """select * from userdata_vw"""
    # Create a DataFrame with aggregated state data
    df1 = pd.read_sql(query1, conn)
    df2 = pd.read_sql(query2, conn)
    df = pd.concat([df1,df2])
    # Group the data by state and fetch geolocation for each unique state
    unique_states = df["state"].unique()
    geolocation_data = [geocode_location(state) for state in unique_states]
    geolocation_df = pd.DataFrame(geolocation_data, columns=["state_latitude", "state_longitude"])
    geolocation_df["state"] = unique_states
    # Merge the geolocation data back into the original DataFrame
    df = df.merge(geolocation_df, on="state", how="left")
    # Fetch geolocation data for districts in the selected state, year, and quarter
    districts = df["district"].unique()
    district_geolocation_data = [geocode_location(district) for district in districts]
    district_geolocation_df = pd.DataFrame(district_geolocation_data, columns=["district_latitude", "district_longitude"])
    district_geolocation_df["district"] = districts
    # Merge the district geolocation data back into the DataFrame
    df = df.merge(district_geolocation_df, on="district", how="left")
    insert_values(df,"merged_geolocation_data",conn,curr)
    return df

# Function to fetch geolocation data for locations
def geocode_location(location):
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": location, "format": "json", "limit": 1}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    if len(data) > 0:
        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])
        return lat, lon
    else:
        return None, None

def insert_values(df,table_name,conn,curr):
    columns = df.columns.tolist()
    placeholders = ['%s' if df[col].dtype.kind not in 'fi' else '%s::{}'.format('float' if df[col].dtype.kind == 'f' else 'int') for col in columns]
    insert_query = 'insert into {} ({}) values ({})'.format(table_name, ','.join(columns), ','.join(placeholders))
    # Roll back the current transaction
    conn.rollback()
    curr.execute(f'truncate table {table_name}')
    for row in df.itertuples(index=False):
        curr.execute(insert_query, tuple(row))
        conn.commit()

if __name__ == "__main__":
    # Retrieve and process the data
    retrieve_data()