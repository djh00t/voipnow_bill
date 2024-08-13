#!/data/opt/miniconda3/envs/e164bill/bin/python

###
### csvgen.py - Generate CSV files with outbound call data for each reseller
###
import mysql.connector
import csv
import os
from datetime import datetime, timedelta

# Function to get MySQL credentials
def get_mysql_credentials():
    with open('/etc/voipnow/.sqldb', 'r') as file:
        data = file.read().strip()
        parts = data.split(':')
        return parts[1], parts[2]  # username, password

# Function to get the last month and year
def get_last_month():
    today = datetime.today()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    return last_month.year, last_month.month

# Establish MySQL connection
username, password = get_mysql_credentials()
db_connection = mysql.connector.connect(
    host="localhost",
    user=username,
    password=password,
    database="voipnow"
)

cursor = db_connection.cursor(dictionary=True)

# Define the query
query = """
SELECT
    call_history.client_reseller_id AS reseller_id,
    reseller.company AS reseller_name, 
    call_history.client_client_id AS client_id,
    call_history.start, 
    call_history.extension_number AS source,  
    call_history.partyid AS destination,
    call_history.duration, 
    call_history.costres AS reseller_cost,
    call_history.costcl AS client_cost,
    SUBSTRING_INDEX(call_history.caller_info, ':', 1) AS caller_ip,
    call_history.callid,
    call_history.hangupcause
FROM call_history
JOIN client AS reseller ON call_history.client_reseller_id = reseller.id
JOIN client AS client ON call_history.client_client_id = client.id
WHERE
    call_history.disposion = 'ANSWERED'
    AND (
        (call_history.flow = 'in' AND call_history.costadmin > 0 AND call_history.costres > 0)
        OR
        (call_history.flow = 'out')
    )
    AND call_history.calltype != 'local'
    AND call_history.start BETWEEN DATE_SUB(LAST_DAY(NOW() - INTERVAL 1 MONTH), INTERVAL DAY(LAST_DAY(NOW() - INTERVAL 1 MONTH)) - 1 DAY)
                   AND LAST_DAY(NOW() - INTERVAL 1 MONTH)
ORDER BY 
    reseller_name,
    client_name,
    call_history.extension_number,
    call_history.start ASC
"""

# Execute the query
cursor.execute(query)
rows = cursor.fetchall()

# Get last month and year
year, month = get_last_month()
year_month_str = f"{year}{month:02d}"

# Process data per reseller and client
resellers_data = {}
for row in rows:
    reseller_name = row['reseller_name']
    client_name = row['client_name']
    if reseller_name not in resellers_data:
        resellers_data[reseller_name] = {}
    if client_name not in resellers_data[reseller_name]:
        resellers_data[reseller_name][client_name] = []
    resellers_data[reseller_name][client_name].append(row)

# Generate CSV files
for reseller_name, clients in resellers_data.items():
    filename = f"{year_month_str}_{reseller_name.replace(' ', '_')}_OUTBOUND_CALLS.csv"
    with open(filename, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        
        # Write reseller summary
        total_reseller_cost = sum(call['reseller_cost'] for client_calls in clients.values() for call in client_calls)
        total_client_cost = sum(call['client_cost'] for client_calls in clients.values() for call in client_calls)
        total_duration = sum(call['duration'] for client_calls in clients.values() for call in client_calls)
        total_hours = total_duration // 3600
        total_minutes = (total_duration % 3600) // 60
        total_seconds = total_duration % 60
        
        csvwriter.writerow([f"Company Name: {reseller_name}"])
        reseller_id = next(iter(clients.values()))[0]['reseller_id']  # Get the reseller_id from the first client's first call
        csvwriter.writerow([f"Reseller ID: {reseller_id}"])
        csvwriter.writerow([f"Total Call Time: {total_hours} hours, {total_minutes} minutes, {total_seconds} seconds"])
        csvwriter.writerow([f"Total Reseller Cost: ${total_reseller_cost:.2f}"])
        csvwriter.writerow([f"Total Client Cost: ${total_client_cost:.2f}"])
        csvwriter.writerow([])  # Blank line between sections
        
        # Write data grouped by client and extension
        for client_name, calls in clients.items():
            client_total_duration = sum(call['duration'] for call in calls)
            client_total_reseller_cost = sum(call['reseller_cost'] for call in calls)
            client_total_client_cost = sum(call['client_cost'] for call in calls)
            client_hours = client_total_duration // 3600
            client_minutes = (client_total_duration % 3600) // 60
            client_seconds = client_total_duration % 60

            csvwriter.writerow([])  # Blank line between sections
            csvwriter.writerow([])  # Blank line between sections
            csvwriter.writerow([f"Client Name: {client_name}"])
            client_id = calls[0]['client_id']  # Get the client_id from the first call
            csvwriter.writerow([f"Client ID: {client_id}"])
            csvwriter.writerow([f"Total Call Time: {client_hours} hours, {client_minutes} minutes, {client_seconds} seconds"])
            csvwriter.writerow([f"Total Reseller Cost: ${client_total_reseller_cost:.2f}"])
            csvwriter.writerow([f"Total Client Cost: ${client_total_client_cost:.2f}"])
            csvwriter.writerow([])  # Blank line between client sections

            current_extension = None

            for call in calls:
                extension = call['source']
                if current_extension != extension:
                    if current_extension is not None:
                        csvwriter.writerow([])  # Blank line between extensions

                    # Calculate totals for the current extension
                    extension_calls = [c for c in calls if c['source'] == extension]
                    extension_total_duration = sum(c['duration'] for c in extension_calls)
                    extension_total_reseller_cost = sum(c['reseller_cost'] for c in extension_calls)
                    extension_total_client_cost = sum(c['client_cost'] for c in extension_calls)
                    extension_hours = extension_total_duration // 3600
                    extension_minutes = (extension_total_duration % 3600) // 60
                    extension_seconds = extension_total_duration % 60

                    current_extension = extension
                    csvwriter.writerow([f"Extension: {extension}"])
                    csvwriter.writerow([f"Total Call Time: {extension_hours} hours, {extension_minutes} minutes, {extension_seconds} seconds"])
                    csvwriter.writerow([f"Total Reseller Cost: ${extension_total_reseller_cost:.2f}"])
                    csvwriter.writerow([f"Total Client Cost: ${extension_total_client_cost:.2f}"])
                    csvwriter.writerow(["Start", "Source", "Destination", "Duration", "Reseller Cost", "Client Cost", "Caller IP", "Call ID", "Hangup Cause"])

                csvwriter.writerow([
                    call['start'],
                    extension,
                    call['destination'],
                    call['duration'],
                    call['reseller_cost'],
                    call['client_cost'],
                    call['caller_ip'],
                    call['callid'],
                    call['hangupcause']
                ])

# Close the database connection
cursor.close()
db_connection.close()
