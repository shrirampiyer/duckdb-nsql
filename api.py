import ollama
import psycopg2
import db_config

user_prompt = input("what would you like to query?\n")
conn = psycopg2.connect(
    dbname=db_config.db_name,
    user=db_config.db_user,
    password=db_config.db_password,
    host=db_config.db_host,
    port=db_config.db_port)

def get_query(user_prompt):
    r = ollama.generate(
        model='duckdb-nsql',
        system='''Here is the database schema that the SQL query will run on:
        CREATE TABLE Members (
        MemberID INT PRIMARY KEY,
        MemberName VARCHAR(255),
        MemberAddress VARCHAR(255),
        Age INT,
        InsuranceProvider VARCHAR(255),
        PolicyNumber VARCHAR(50),
        EnrollmentDate DATE,
        PrimaryCarePhysician VARCHAR(255));,
        CREATE TABLE Diagnosis (
        DiagnosisID SERIAL PRIMARY KEY,
        DiagnosisCode VARCHAR(50),
        DiagnosisDescription TEXT,
        SpecialistName VARCHAR(255),
        TreatmentPlan TEXT,
        HospitalName VARCHAR(255),
        LabTestRequired BOOLEAN,
        LabTestDate DATE,
        MedicationPrescribed VARCHAR(255),
        PrescriptionDate DATE,
        MemberID INTEGER,
        FOREIGN KEY (MemberID) REFERENCES Members(MemberID));''',
        prompt=f'{user_prompt}',
    )
    return r['response']
cur = conn.cursor()
query = get_query(user_prompt)
cur.execute(query)

rows = cur.fetchall()
print(query)
for row in rows:
    print(row)

cur.close()
conn.close()
