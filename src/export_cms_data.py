import csv
import mysql.connector

# MySQL connection details
print("Connecting to MySQL...")
conn = mysql.connector.connect(
    host='localhost',
    user='YOUR_USERNAME',
    password='YOUR_PASSWORD',
    database='cms'
)
print("Connected.")

cursor = conn.cursor()

# Output CSV path
output_file = 'PATH_TOWARDS_OUTPUT_CSV'

# SQL Query (flattened join)
query = """
SELECT 
    h.hospital_id,
    h.hospital_name,
    h.address,
    h.city,
    h.state,
    h.zip_code,
    h.county,
    h.hospital_type,
    h.hospital_ownership,

    -- Readmission
    r.measure_name AS readmission_measure,
    r.excess_readmission_ratio,
    r.predicted_readmission_rate,
    r.expected_readmission_rate,
    r.num_readmissions,
    r.start_date AS readmission_start_date,
    r.end_date AS readmission_end_date,

    -- Unplanned Visit
    u.measure_name AS unplanned_visit_measure,
    u.score AS unplanned_visit_score,
    u.num_patients AS unplanned_num_patients,
    u.num_patients_returned AS unplanned_num_patients_returned,
    u.start_date AS unplanned_start_date,
    u.end_date AS unplanned_end_date,

    -- Timely & Effective Care
    t.measure_name AS timely_measure,
    t.score AS timely_score,
    t.sample_size AS timely_sample_size,
    t.start_date AS timely_start_date,
    t.end_date AS timely_end_date

FROM hospitals h

-- Readmission
LEFT JOIN readmission_metrics r 
    ON (
        r.hospital_id = h.hospital_id OR r.facility_name = h.hospital_name
    )

-- Unplanned Visits
LEFT JOIN unplanned_visits u 
    ON (
        u.hospital_id = h.hospital_id OR u.facility_name = h.hospital_name
    )

-- Timely and Effective Care
LEFT JOIN timely_effective_care t 
    ON (
        t.hospital_id = h.hospital_id OR t.facility_name = h.hospital_name
    )

WHERE h.state = 'CA';

"""

# Execute the query
print("Running query...")
cursor.execute(query)
print("Query executed.")

# Get column names
print("Fetching column names...")
columns = [desc[0] for desc in cursor.description]
print("Columns fetched:", columns)

print("Writing to CSV...")
with open(output_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(columns)
    
    row_count = 0
    for row in cursor:
        writer.writerow(row)
        row_count += 1
        if row_count % 1000 == 0:
            print(f"{row_count} rows written...")

print(f"✅ Finished! {row_count} rows exported to {output_file}")

# Cleanup
cursor.close()
conn.close()






