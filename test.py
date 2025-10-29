import pandas as pd
import mysql.connector

# Load Excel
df = pd.read_excel("dbms_main_data.xlsx")

# Clean spaces from column names
df.columns = df.columns.str.strip()

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mithra1536",
    database="dbms"
)
cursor = conn.cursor()

# Function to handle empty or NaN values
def format_cutoff(value):
    if pd.isna(value) or value == '' or value is None:
        return '--'
    return str(value)

# Iterate through rows and insert
for _, row in df.iterrows():
    college_name = row["College Name"]
    branch_name = row["Branch Name"]
    category = row["Category"]
    
    # Find unique_key
    cursor.execute("""
        SELECT b.unique_key
        FROM branch b
        JOIN college c ON b.college_id = c.college_id
        WHERE c.college_name = %s AND b.branch_name = %s
    """, (college_name, branch_name))
    
    result = cursor.fetchone()
    if result:
        unique_key = str(result[0])  # Convert to string since unique_key is VARCHAR
        insert_query = """
        INSERT INTO cutoff (
            unique_key, category,
            cutoff_2022_R1, cutoff_2022_R2, cutoff_2022_R3,
            cutoff_2023_R1, cutoff_2023_R2, cutoff_2023_R3,
            cutoff_2024_R1, cutoff_2024_R2, cutoff_2024_R3,
            cutoff_2025_R1, cutoff_2025_R2, cutoff_2025_R3
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            cutoff_2022_R1=VALUES(cutoff_2022_R1), cutoff_2022_R2=VALUES(cutoff_2022_R2), cutoff_2022_R3=VALUES(cutoff_2022_R3),
            cutoff_2023_R1=VALUES(cutoff_2023_R1), cutoff_2023_R2=VALUES(cutoff_2023_R2), cutoff_2023_R3=VALUES(cutoff_2023_R3),
            cutoff_2024_R1=VALUES(cutoff_2024_R1), cutoff_2024_R2=VALUES(cutoff_2024_R2), cutoff_2024_R3=VALUES(cutoff_2024_R3),
            cutoff_2025_R1=VALUES(cutoff_2025_R1), cutoff_2025_R2=VALUES(cutoff_2025_R2), cutoff_2025_R3=VALUES(cutoff_2025_R3);
        """
        
        # Format all cutoff values
        values = (
            unique_key, category,
            format_cutoff(row.get("cutoff_2022_R1")), format_cutoff(row.get("cutoff_2022_R2")), format_cutoff(row.get("cutoff_2022_R3")),
            format_cutoff(row.get("cutoff_2023_R1")), format_cutoff(row.get("cutoff_2023_R2")), format_cutoff(row.get("cutoff_2023_R3")),
            format_cutoff(row.get("cutoff_2024_R1")), format_cutoff(row.get("cutoff_2024_R2")), format_cutoff(row.get("cutoff_2024_R3")),
            format_cutoff(row.get("cutoff_2025_R1")), format_cutoff(row.get("cutoff_2025_R2")), format_cutoff(row.get("cutoff_2025_R3"))
        )
        cursor.execute(insert_query, values)
conn.commit()
conn.close()
