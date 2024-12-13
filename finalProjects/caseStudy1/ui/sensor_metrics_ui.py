import streamlit as st
import pandas as pd
import requests

def fetch_aggregated_data(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data: {e}")
        return []

# Main app
def main():
    st.title("Sensor Metrics Viewer")

    search_col1, search_col2 = st.columns([3, 1])
    with search_col1:
        sensor_id = st.text_input("Search by Sensor ID", placeholder="Enter Sensor ID here")
    with search_col2:
        search_button = st.button("Search", disabled=not bool(sensor_id.strip()))

    refresh_button = st.button("Refresh Data")

    table_placeholder = st.empty()

    base_api_url = "http://0.0.0.0:8080/api/aggregated-data"

    if refresh_button:
        data = fetch_aggregated_data(base_api_url)
        if data:
            df = pd.DataFrame(data)
            table_placeholder.table(df)
        else:
            table_placeholder.write("No data available.")

    if search_button and sensor_id.strip():
        api_url = f"{base_api_url}/{sensor_id.strip()}"
        data = fetch_aggregated_data(api_url)
        if data:
            df = pd.DataFrame(data)
            table_placeholder.table(df)
        else:
            table_placeholder.write("No data found for the given Sensor ID.")

if __name__ == "__main__":
    main()
