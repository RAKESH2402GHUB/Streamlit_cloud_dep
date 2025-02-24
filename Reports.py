import streamlit as st
import pandas as pd
import snowflake.connector
import auth

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

st.title('Machine Details Reports')
#auth.check_user_and_password()

@st.cache_data(show_spinner="Loading the Reports...")

def get_database():
    conn= get_snowflake_connection()
    cur = conn.cursor()
    cur.execute(f'show databases')
    databases = [row[1] for row in cur.fetchall()]  # Get the second column (database names)
    cur.close()
    return databases

def get_schema(database):
    conn= get_snowflake_connection()
    cur = conn.cursor()
    cur.execute(f"SHOW SCHEMAS IN DATABASE {database}")
    schemas = [row[1] for row in cur.fetchall()]  # Get the second column (database names)
    cur.close()
    return schemas

def get_tables(database, schema):
    conn= get_snowflake_connection()
    cur = conn.cursor()
    cur.execute(f"SHOW TABLES IN {database}.{schema}")
    tables = [row[1] for row in cur.fetchall()]  # Get the second column (database names)
    cur.close()
    return tables

def select_from_table(database, schema, table):
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {database}.{schema}.{table}")
        
        rows = cur.fetchall()
        
        if not rows:
            st.warning("No data found in the selected table.")
            return pd.DataFrame()  # Returning an empty DataFrame
        
        # Get column names from the cursor description
        columns = [desc[0] for desc in cur.description]
        
        cur.close()
        
        # Return the data as a pandas DataFrame
        data = pd.DataFrame(rows, columns=columns)
        
        return data
    except Exception as e:
        st.error(f"Error fetching data from {database}.{schema}.{table}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


databases = get_database()

st.sidebar.title("Select Data Base , Schema and Table for reports")
selected_db = st.sidebar.selectbox("Choose a Database", databases)


if selected_db:
    schemas = get_schema(selected_db)
    selected_sc = st.sidebar.selectbox("Choose a Schema", schemas)

    if selected_sc:
        tables = get_tables(selected_db, selected_sc)
        selected_tb = st.sidebar.selectbox("Choose a table", tables)

        if selected_tb:
            my_data = select_from_table(selected_db, selected_sc, selected_tb)
            st.write(my_data)
