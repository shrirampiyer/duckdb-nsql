import pandas as pd
import psycopg2
from psycopg2 import sql

# Define PostgreSQL connection parameters
DB_NAME = "duck_db_poc"
DB_USER = "postgres"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"

# Define CSV file path
csv_file = "C:/Users/shrir/OneDrive/Desktop/duckdb/diagnosis_data.csv"

# Read the CSV file into a pandas dataframe
df = pd.read_csv(csv_file)

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

# Create a cursor object
cur = conn.cursor()

# Define table name
table_name = "diagnosis"




for index, row in df.iterrows():
    insert_query = sql.SQL("""
        INSERT INTO {} (DiagnosisID, DiagnosisCode, DiagnosisDescription, SpecialistName, TreatmentPlan, HospitalName, LabTestRequired, LabTestDate, MedicationPrescribed, PrescriptionDate, MemberID)
        VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """).format(sql.Identifier(table_name))

    
    cur.execute(insert_query, tuple(row))

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()