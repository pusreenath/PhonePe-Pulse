# plotly streamlit map
# Created by: @pusreenath https://github.com/pusreenath on 6th May 2023

# Import required libraries
import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def retrieve_data():
    # Connect to the database
    conn = psycopg2.connect(host="localhost", user="postgres", password="postgres@123", port=5432, database="phonepe_pulse")
    curr = conn.cursor()
    # Fetch the data from the database
    query1 = """select * from merged_geolocation_data"""
    # Create a DataFrame with aggregated state data
    df = pd.read_sql(query1, conn)
    return df

# Main function to run the Streamlit app
def main():
    # Retrieve and process the data
    df = st.cache_data(retrieve_data)()
    
    # Filter the DataFrame based on the selected values
    filtered_df = df.copy()

    # Selectbox for choosing Transaction or Users
    selected_tu = st.selectbox("Select Transaction or Users", list(df["transaction_or_users"].unique()))
    
    # Filter the DataFrame based on selected transaction or users
    filtered_df = filtered_df[filtered_df["transaction_or_users"] == selected_tu]

    # Create the dropdowns for state, year, quarter, and district
    selected_state = st.selectbox("Select State", ["All"] + list(df["state"].unique()))
    selected_year = st.selectbox("Select Year", ["All"] + list(df["year"].unique()))
    selected_quarter = st.selectbox("Select Quarter", ["All"] + list(df["quarter"].unique()))

    if selected_state != "All":
        filtered_df = filtered_df[filtered_df["state"] == selected_state]
        selected_district = st.selectbox("Select District", ["All"] + list(filtered_df["district"].unique()))
        if selected_district != "All":
            filtered_df = filtered_df[filtered_df["district"] == selected_district]

    if selected_year != "All":
        filtered_df = filtered_df[filtered_df["year"] == selected_year]

    if selected_quarter != "All":
        filtered_df = filtered_df[filtered_df["quarter"] == selected_quarter]

    # Check if Transaction Category selectbox should be shown
    if selected_tu != "transaction":
        show_transactioncategory=False
    else:
        show_transactioncategory=True
    
    if show_transactioncategory:
        # Filter the DataFrame based on selected Transaction Category
        selected_transactioncategory = st.selectbox("Select Transaction Category", ["All"] + list(filtered_df["transactioncategory"].unique()))
        if selected_transactioncategory != "All":
            filtered_df = filtered_df[filtered_df["transactioncategory"] == selected_transactioncategory]

        # Plot the data on a map using Plotly
        fig = px.scatter_mapbox(
            filtered_df,
            lat="state_latitude" if selected_state == "All" else "district_latitude",
            lon="state_longitude" if selected_state == "All" else "district_longitude",
            size="totaltransactionamount",
            hover_name="state" if selected_state == "All" else "district",
            hover_data=["district", "year", "quarter", "totaltransactionamount", "transactioncategory", "totaltransactioncount","totaltransactioncount_district","totaltransactionamount_district"],
            color_discrete_sequence=["pink" if selected_state == "All" else "blue"],
            zoom=3,
            height=600,
        )

    if not show_transactioncategory:
        selected_brand = st.selectbox("Select Brand", ["All"] + list(filtered_df["brand"].unique()))
        if selected_brand != "All":
            filtered_df = filtered_df[filtered_df["brand"] == selected_brand]
        # Plot the data on a map using Plotly
        fig = px.scatter_mapbox(
            filtered_df,
            lat="state_latitude" if selected_state == "All" else "district_latitude",
            lon="state_longitude" if selected_state == "All" else "district_longitude",
            size="totalaggregisteredusers",
            hover_name="state" if selected_state == "All" else "district",
            hover_data=["district", "year", "quarter", "brand", "totaldevicecount", "totalaggregisteredusers","totalaggappopens","totalregisteredusers_district"],
            color_discrete_sequence=["pink" if selected_state == "All" else "blue"],
            zoom=3,
            height=600,
        )

    # Update the layout for the map
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox=dict(
            center={"lat": 24, "lon": 78},
            zoom=3,
        ),
    )

    # Render the Plotly figure in Streamlit
    st.plotly_chart(fig)

    if not show_transactioncategory:
        # Show top 10 brands
        if st.checkbox("Show Top 10 Brands"):
            top10_brands = filtered_df[['brand','totalaggregisteredusers']].groupby(by='brand').sum().sort_values(by='totalaggregisteredusers', ascending=False).head(10)
            st.write("Top 10 Brands:")
            st.write("This is based upon Total Registered Users")
            st.write(top10_brands)
            st.bar_chart(top10_brands)

        # Show top 10 states
        if st.checkbox("Show Top 10 States"):
            top10_states = filtered_df[['state','totalaggregisteredusers']].groupby(by='state').sum().sort_values(by='totalaggregisteredusers', ascending=False).head(10)
            st.write("Top 10 States:")
            st.write("This is based upon Total Registered Users")
            st.write(top10_states)
            st.bar_chart(top10_states)

        # Show top 10 districts
        if st.checkbox("Show Top 10 Districts"):
            top10_districts = filtered_df[['district','totalaggregisteredusers']].groupby(by='district').sum().sort_values(by='totalaggregisteredusers', ascending=False).head(10)
            st.write("Top 10 Districts:")
            st.write("This is based upon Total Registered Users")
            st.write(top10_districts)
            st.bar_chart(top10_districts)

    if show_transactioncategory:
        # Show top 5 transactions
        if st.checkbox("Show Top 5 Transactions"):
            top10_transactions = filtered_df[['transactioncategory','totaltransactionamount']].groupby(by='transactioncategory').sum().sort_values(by='totaltransactionamount', ascending=False).head(5)
            st.write("Top 5 Transactions Cateogry:")
            st.write("This is based upon Total Transaction Amount")
            st.write(top10_transactions)
            fig = go.Figure(data=[go.Bar(x=top10_transactions.index, y=top10_transactions['totaltransactionamount'])])
            st.plotly_chart(fig)

        # Show top 10 states
        if st.checkbox("Show Top 10 States"):
            top10_states = filtered_df[['state','totaltransactionamount']].groupby(by='state').sum().sort_values(by='totaltransactionamount', ascending=False).head(10)
            st.write("Top 10 States:")
            st.write("This is based upon Total Transaction Amount")
            st.write(top10_states)
            st.bar_chart(top10_states)

        # Show top 10 districts
        if st.checkbox("Show Top 10 Districts"):
            top10_districts = filtered_df[['district','totaltransactionamount']].groupby(by='district').sum().sort_values(by='totaltransactionamount', ascending=False).head(10)
            st.write("Top 10 Districts:")
            st.write("This is based upon Total Transaction Amount")
            st.write(top10_districts)
            st.bar_chart(top10_districts)

if __name__ == "__main__":
    main()