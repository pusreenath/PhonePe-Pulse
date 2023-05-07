# Created by: @pusreenath https://github.com/pusreenath on 6th May 2023

import streamlit as st
import psycopg2
import pandas as pd
import requests
import plotly.express as px

def retrieve_data():
    # Connect to the database
    conn = psycopg2.connect(host="localhost", user="postgres", password="postgres@123", port=5432, database="phonepe_pulse")

    # Fetch the data from the database
    query = """select * from trdata_vw"""

    # Create a DataFrame with aggregated state data
    df = pd.read_sql(query, conn)

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


# Main function to run the Streamlit app
def main():
    # Retrieve and process the data
    df = st.cache_data(retrieve_data)()

    # Create the dropdowns for state, year, quarter, and district
    selected_state = st.selectbox("Select State", ["All"] + list(df["state"].unique()))
    selected_transactioncategory = st.selectbox("Select Transaction Category", ["All"] + list(df["transactioncategory"].unique()))
    selected_year = st.selectbox("Select Year", ["All"] + list(df["year"].unique()))
    selected_quarter = st.selectbox("Select Quarter", ["All"] + list(df["quarter"].unique()))
    selected_district = st.selectbox("Select District", ["All"] + list(df["district"].unique()))
    selected_tu = st.selectbox("Select Transaction or Users", ["All"] + list(df["transaction_or_users"].unique()))

    # Filter the DataFrame based on the selected values
    filtered_df = df.copy()

    if selected_state != "All":
        filtered_df = filtered_df[filtered_df["state"] == selected_state]

        if selected_district != "All":
            filtered_df = filtered_df[filtered_df["district"] == selected_district]

    if selected_year != "All":
        filtered_df = filtered_df[filtered_df["year"] == selected_year]

    if selected_quarter != "All":
        filtered_df = filtered_df[filtered_df["quarter"] == selected_quarter]

    if selected_transactioncategory != "All":
        filtered_df = filtered_df[filtered_df["transactioncategory"] == selected_transactioncategory]
    
    if selected_tu != "All":
        filtered_df = filtered_df[filtered_df["transaction_or_users"] == selected_tu]

    # Plot the data on a map using Plotly
    fig = px.scatter_mapbox(
        filtered_df,
        lat="state_latitude" if selected_state == "All" else "district_latitude",
        lon="state_longitude" if selected_state == "All" else "district_longitude",
        size="totaltransactionamount",
        hover_name="state" if selected_state == "All" else "district",
        hover_data=["district", "year", "quarter", "totaltransactionamount", "transactioncategory", "totaltransactioncount"],
        color_discrete_sequence=["pink" if selected_state == "All" else "blue"],
        zoom=3,
        height=600,
    )

    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox=dict(
            center={"lat": 24, "lon": 78},
            zoom=3,
        ),
    )

    # Render the Plotly figure in Streamlit
    st.plotly_chart(fig)

if __name__ == "__main__":
    main()