import pandas as pd
import sqlalchemy
import datetime
import snowflake.connector
import redshift_connector

# Replace with your actual credentials and query details
denodo_jdbc_url = 'jdbc:denodo://your_denodo_host:port/database_name'
denodo_username = 'your_username'
denodo_password = 'your_password'
redshift_credentials = {
    "host": "your_redshift_cluster",
    "port": 5439,
    "database": "your_database",
    "user": "your_username",
    "password": "your_password"
}
snowflake_credentials = {
    "user": "your_username",
    "password": "your_password",
    "account": "your_account",
    "warehouse": "your_warehouse",
    "database": "your_database",
    "schema": "your_schema"
}

# Connect to Denodo Database
engine = sqlalchemy.create_engine(denodo_jdbc_url, connect_args={
    "user": denodo_username,
    "password": denodo_password
})

# Query to fetch last 4 days data from two tables and perform aggregation
four_days_ago = datetime.date.today() - datetime.timedelta(days=4)
query = f"""
SELECT *
FROM table1
JOIN table2 ON table1.key = table2.key
WHERE table1.date_field >= '{four_days_ago}' AND table2.date_field >= '{four_days_ago}'
"""

# Fetch data
df = pd.read_sql(query, engine)

# Perform any necessary aggregation or transformations on df here

# Connect to Snowflake and perform upsert
ctx = snowflake.connector.connect(**snowflake_credentials)
cs = ctx.cursor()
try:
    # Create staging table in Snowflake
    cs.execute("CREATE TEMPORARY TABLE staging_table (column1 datatype, column2 datatype, ...)")
    
    # Load data into staging table
    for index, row in df.iterrows():
        cs.execute("INSERT INTO staging_table VALUES (%s, %s, ...)", (row['column1'], row['column2'], ...))

    # Perform MERGE operation
    cs.execute("""
    MERGE INTO your_table AS target
    USING staging_table AS source
    ON target.key_column = source.key_column
    WHEN MATCHED THEN
        UPDATE SET target.column1 = source.column1, target.column2 = source.column2, ...
    WHEN NOT MATCHED THEN
        INSERT (column1, column2, ...) VALUES (source.column1, source.column2, ...)
    """)
finally:
    cs.close()
    ctx.close()

# Connect to Redshift and perform upsert
with redshift_connector.connect(**redshift_credentials) as conn:
    with conn.cursor() as cursor:
        # Create staging table in Redshift
        cursor.execute("CREATE TEMPORARY TABLE staging_table (column1 datatype, column2 datatype, ...)")
        
        # Load data into staging table
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO staging_table VALUES (%s, %s, ...)", (row['column1'], row['column2'], ...))

        # Update existing records
        cursor.execute("""
        UPDATE your_table
        SET column1 = staging.column1, column2 = staging.column2, ...
        FROM staging_table staging
        WHERE your_table.key_column = staging.key_column
        """)

        # Insert new records
        cursor.execute("""
        INSERT INTO your_table (column1, column2, ...)
        SELECT staging.column1, staging.column2, ...
        FROM staging_table staging
        LEFT JOIN your_table ON staging.key_column = your_table.key_column
        WHERE your_table.key_column IS NULL
        """)
