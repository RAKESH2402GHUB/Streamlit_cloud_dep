import streamlit as st
import pandas as pd
import snowflake.connector

# Snowflake connection function
def get_snowflake_connection():
    return snowflake.connector.connect(
        user="RCHITIKIREDDY",
        password="Welcome@2024",
        account="sob76570.us-east-1",
        warehouse="COMPUTE_WH",
        database="STREAMLIT_DB",
        schema="STREAMLIT_SCHEMA"
    )

# Function to upload data to Snowflake
def upload_to_snowflake(file, table_name):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    
    # Read file into DataFrame
    df = pd.read_csv(file)
    
    # Get column names from DataFrame
    columns = list(df.columns)
    column_list = ",".join(columns)
    
    try:
        # Step 1: Truncate the table to remove any existing data
        truncate_query = f"TRUNCATE TABLE {table_name}"
        cur.execute(truncate_query)
        
        # Step 2: Upload the data row by row
        for index, row in df.iterrows():
            values = tuple(row)  # row is already cleaned up in handle_nan
            print(f"Inserting row: {values}")  # Debugging: Print the values before inserting
            
            placeholders = ",".join(["%s"] * len(values))
            insert_query = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"
            print(f"Insert query: {insert_query}")  # Debugging: Print the insert query
            
            cur.execute(insert_query, values)
        
        # Commit the changes
        conn.commit()
        st.success("File uploaded and data inserted successfully!")
    except Exception as e:
        st.error(f"Error: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# Streamlit UI
st.title("Upload File to Snowflake")

# File uploader component
uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

# Table name input
table_name = st.text_input("Enter the Snowflake Table Name", value="YOUR_TABLE_NAME")

# Trigger file upload when the button is pressed
if uploaded_file is not None and st.button("Upload to Snowflake"):
    upload_to_snowflake(uploaded_file, table_name)
