#!/data/opt/miniconda3/envs/e164bill/bin/python
"""
E164 Billing Report Generator.

This module generates detailed billing reports for E164 VoIP services. It processes call history data,
calculates costs, and generates reports grouped by reseller and client. The module handles:
- Call history processing
- Cost calculations
- DID and extension counting
- CSV report generation
- Billing summaries

The reports include:
- Total call durations
- Client billables
- Reseller costs
- DID counts
- Extension counts
- Detailed call records (CDRs)

Dependencies:
    - mysql.connector
    - csv
    - argparse
    - datetime
"""

import mysql.connector
import csv
import argparse
from datetime import datetime, timedelta


def get_mysql_credentials():
    """
    Retrieve MySQL credentials from configuration file.
    
    Returns:
        tuple[str, str]: Username and password for database connection
        
    Note:
        Expects credentials in /etc/voipnow/.sqldb in format: sql:username:password
    """
    with open("/etc/voipnow/.sqldb", "r") as file:
        data = file.read().strip()
        parts = data.split(":")
        return parts[1], parts[2]


def get_last_month():
    """
    Determine the year and month for report generation.
    
    Returns:
        tuple[int, int]: Year and month for report
        
    Note:
        Uses command line arguments if provided, otherwise defaults to previous month
    """
    if args.year and args.month:
        return args.year, args.month
    today = datetime.today()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    return last_month.year, last_month.month

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Generate E164 billing CSV reports.")
parser.add_argument("-y", "--year", type=int, help="Year for the report")
parser.add_argument("-m", "--month", type=int, help="Month for the report")
args = parser.parse_args()

# Initialize database connection
username, password = get_mysql_credentials()
db_connection = mysql.connector.connect(
    host="localhost", user=username, password=password, database="voipnow"
)

cursor_main = db_connection.cursor(dictionary=True)

# Fetch DID counts for resellers
# This query aggregates total DIDs assigned to each reseller
cursor_main.execute(
    """
    SELECT
        reseller.id AS reseller_id,
        COUNT(*) AS did_count
    FROM
        voipnow.channel_did AS did
    JOIN
        voipnow.client AS reseller ON did.reseller_id = reseller.id
    WHERE
        reseller.level = 10  -- Reseller level
    GROUP BY
        reseller.id
"""
)
reseller_did_counts = {
    row["reseller_id"]: row["did_count"] for row in cursor_main.fetchall()
}

# Fetch DID counts for clients
# This query aggregates total DIDs assigned to each client
cursor_main.execute(
    """
    SELECT
        client.id AS client_id,
        COUNT(*) AS did_count
    FROM
        voipnow.channel_did AS did
    JOIN
        voipnow.client AS client ON did.client_id = client.id
    GROUP BY
        client.id
"""
)
client_did_counts = {
    row["client_id"]: row["did_count"] for row in cursor_main.fetchall()
}

# Fetch extension counts for resellers
# This complex query handles the hierarchical relationship between resellers and their clients
cursor_main.execute(
    """
    SELECT
        reseller.id AS reseller_id,
        COUNT(DISTINCT extension.extended_number) AS extension_count
    FROM
        voipnow.extension AS extension
    LEFT JOIN
        voipnow.client AS client ON extension.client_id = CAST(client.id AS UNSIGNED)
    LEFT JOIN
        voipnow.client AS parent_client ON client.parent_client_id = parent_client.id AND client.level = 100
    JOIN
        voipnow.client AS reseller ON
            (client.level = 50 AND LPAD(client.parent_client_id, 4, '0') = LPAD(reseller.id, 4, '0'))
            OR
            (client.level = 100 AND LPAD(parent_client.parent_client_id, 4, '0') = LPAD(reseller.id, 4, '0'))
    WHERE
        reseller.level = 10  -- Reseller level
    GROUP BY
        reseller.id
"""
)
reseller_extension_counts = {
    row["reseller_id"]: row["extension_count"] for row in cursor_main.fetchall()
}

# Fetch extension counts for clients
# This query handles both direct clients and clients under parent organizations
cursor_main.execute(
    """
    SELECT
        COALESCE(parent_client.id, client.id) AS client_id,
        COUNT(DISTINCT extension.extended_number) AS extension_count
    FROM
        voipnow.extension AS extension
    LEFT JOIN
        voipnow.client AS client ON extension.client_id = CAST(client.id AS UNSIGNED)
    LEFT JOIN
        voipnow.client AS parent_client ON client.parent_client_id = parent_client.id AND client.level = 100
    WHERE
        client.level IN (50, 100)  -- Client or User level
    GROUP BY
        COALESCE(parent_client.id, client.id)
"""
)
client_extension_counts = {
    row["client_id"]: row["extension_count"] for row in cursor_main.fetchall()
}

# Main query for call history data
# This query retrieves all billable calls with their associated metadata
query = """
SELECT
    call_history.client_reseller_id AS reseller_id,
    reseller.company AS reseller_name,
    call_history.client_client_id AS client_id,
    client.company AS client_name,
    call_history.flow AS direction,
    REPLACE(
        CASE
            WHEN LOWER(call_history.billingplan) LIKE '% - inbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), ' - inbound', '')), '&', 'AND'), ' ', ''))
            WHEN LOWER(call_history.billingplan) LIKE '% - outbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), ' - outbound', '')), '&', 'AND'), ' ', ''))
            WHEN LOWER(call_history.billingplan) LIKE '%inbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), 'inbound', '')), '&', 'AND'), ' ', ''))
            WHEN LOWER(call_history.billingplan) LIKE '%outbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), 'outbound', '')), '&', 'AND'), ' ', ''))
            ELSE UPPER(REPLACE(REPLACE(call_history.billingplan, '&', 'AND'), ' ', ''))
        END, ' ', '') AS base_plan,
    CASE
        WHEN call_history.flow = 'out' THEN CONCAT(
            REPLACE(
                CASE
                    WHEN LOWER(call_history.billingplan) LIKE '% - inbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), ' - inbound', '')), '&', 'AND'), ' ', ''))
                    WHEN LOWER(call_history.billingplan) LIKE '% - outbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), ' - outbound', '')), '&', 'AND'), ' ', ''))
                    WHEN LOWER(call_history.billingplan) LIKE '%inbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), 'inbound', '')), '&', 'AND'), ' ', ''))
                    WHEN LOWER(call_history.billingplan) LIKE '%outbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), 'outbound', '')), '&', 'AND'), ' ', ''))
                    ELSE UPPER(REPLACE(REPLACE(call_history.billingplan, '&', 'AND'), ' ', ''))
                END, ' ', ''
            ), '-OUT'
        )
        WHEN call_history.flow = 'in' THEN CONCAT(
            REPLACE(
                CASE
                    WHEN LOWER(call_history.billingplan) LIKE '% - inbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), ' - inbound', '')), '&', 'AND'), ' ', ''))
                    WHEN LOWER(call_history.billingplan) LIKE '% - outbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), ' - outbound', '')), '&', 'AND'), ' ', ''))
                    WHEN LOWER(call_history.billingplan) LIKE '%inbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), 'inbound', '')), '&', 'AND'), ' ', ''))
                    WHEN LOWER(call_history.billingplan) LIKE '%outbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), 'outbound', '')), '&', 'AND'), ' ', ''))
                    ELSE UPPER(REPLACE(REPLACE(call_history.billingplan, '&', 'AND'), ' ', ''))
                END, ' ', ''
            ), '-IN'
        )
        ELSE
            REPLACE(
                CASE
                    WHEN LOWER(call_history.billingplan) LIKE '% - inbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), ' - inbound', '')), '&', 'AND'), ' ', ''))
                    WHEN LOWER(call_history.billingplan) LIKE '% - outbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), ' - outbound', '')), '&', 'AND'), ' ', ''))
                    WHEN LOWER(call_history.billingplan) LIKE '%inbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), 'inbound', '')), '&', 'AND'), ' ', ''))
                    WHEN LOWER(call_history.billingplan) LIKE '%outbound' THEN UPPER(REPLACE(REPLACE(RTRIM(REPLACE(LOWER(call_history.billingplan), 'outbound', '')), '&', 'AND'), ' ', ''))
                    ELSE UPPER(REPLACE(REPLACE(call_history.billingplan, '&', 'AND'), ' ', ''))
                END, ' ', ''
            )
    END AS plan,
    call_history.disposion,
    call_history.start,
    call_history.extension_number AS extension,
    CASE
        WHEN call_history.did IS NULL OR call_history.did = '' THEN 'N/A'
        ELSE call_history.did
    END AS phone_number,
    call_history.partyid AS destination,
    call_history.prefix AS charging_zone,
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
    (
        (call_history.disposion = 'ANSWERED' AND call_history.flow = 'in' AND call_history.costadmin > 0 AND call_history.costres > 0)
        OR
        (call_history.disposion = 'ANSWERED' AND call_history.flow = 'out' AND call_history.costadmin > 0 AND call_history.costres > 0)
    )
    AND call_history.calltype != 'local'
    AND call_history.start BETWEEN DATE_FORMAT(STR_TO_DATE(CONCAT(%s, '-', %s, '-01'), '%Y-%m-%d'), '%Y-%m-01 00:00:00')
                   AND LAST_DAY(STR_TO_DATE(CONCAT(%s, '-', %s, '-01'), '%Y-%m-%d')) + INTERVAL 1 DAY - INTERVAL 1 SECOND
ORDER BY
    reseller_name,
    client_name,
    call_history.extension_number,
    call_history.start ASC;
"""

# Execute main query with date parameters
cursor_main.execute(query, (args.year, args.month, args.year, args.month))

# Fetch all call records
rows = cursor_main.fetchall()

# Process data by reseller
resellers_data = {}
for row in rows:
    reseller_name = row["reseller_name"]
    client_name = row["client_name"]
    reseller_id = row["reseller_id"]

    # Initialize reseller data structure if needed
    if reseller_name not in resellers_data:
        resellers_data[reseller_name] = []

    # Add pre-fetched DID and extension counts to the row
    row["reseller_did_count"] = reseller_did_counts.get(reseller_id, 0)
    row["client_did_count"] = client_did_counts.get(row["client_id"], 0)
    row["reseller_extension_count"] = reseller_extension_counts.get(reseller_id, 0)
    row["client_extension_count"] = client_extension_counts.get(row["client_id"], 0)

    resellers_data[reseller_name].append(row)

# Generate CSV reports
year, month = get_last_month()
year_month_str = f"{year}{month:02d}"

# Process each reseller's data and generate a report
for reseller_name, calls in resellers_data.items():
    filename = f"{year_month_str}_{reseller_name.replace(' ', '_')}_E164_BILL.csv"
    with open(filename, "w", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)

        # Calculate reseller summary statistics
        total_reseller_cost = sum(call["reseller_cost"] for call in calls)
        total_client_cost = sum(call["client_cost"] for call in calls)
        total_duration = sum(call["duration"] for call in calls)
        total_hours = total_duration // 3600
        total_minutes = (total_duration % 3600) // 60
        total_seconds = total_duration % 60

        # Write reseller summary section
        reseller_id = calls[0]["reseller_id"]
        csvwriter.writerow(["Company Name:", f"{reseller_name}", f"Reseller ID:", f"{reseller_id}"])
        # Write duration totals
        csvwriter.writerow([
            "Total Call Time:",
            f"{total_hours} hours, {total_minutes} minutes, {total_seconds} seconds",
        ])
        
        # Write cost summaries
        csvwriter.writerow(["Total Client Billables:", f"${total_client_cost:.2f}"])
        csvwriter.writerow(["Total Reseller Cost:", f"${total_reseller_cost:.2f}"])
        
        # Write DID and extension counts
        csvwriter.writerow(["Total Reseller DIDs:", f"{calls[0]['reseller_did_count']}"])
        csvwriter.writerow([
            "Total Reseller Extensions:",
            f"{calls[0]['reseller_extension_count']}",
        ])

        # Group data by client for detailed reporting
        clients_grouped = {}
        for call in calls:
            client_name = call["client_name"]
            if client_name not in clients_grouped:
                clients_grouped[client_name] = []
            clients_grouped[client_name].append(call)

        # Process each client's call data
        for client_name, client_calls in clients_grouped.items():
            # Calculate client-level totals
            client_total_duration = sum(call["duration"] for call in client_calls)
            client_total_reseller_cost = sum(call["reseller_cost"] for call in client_calls)
            client_total_client_cost = sum(call["client_cost"] for call in client_calls)
            
            # Convert duration to hours, minutes, seconds
            client_hours = client_total_duration // 3600
            client_minutes = (client_total_duration % 3600) // 60
            client_seconds = client_total_duration % 60

            # Add spacing between sections
            csvwriter.writerow([])  # Blank line between client sections
            
            # Write client header and summary
            client_id = client_calls[0]["client_id"]
            csvwriter.writerow([
                "Client Name:",
                f"{client_name}",
                "Client ID:",
                f"{client_id}",
            ])
            csvwriter.writerow([
                "Client Call Time:",
                f"{client_hours} hours, {client_minutes} minutes, {client_seconds} seconds",
            ])
            
            # Write client financials and statistics
            csvwriter.writerow(["Client Billables:", f"${client_total_client_cost:.2f}"])
            csvwriter.writerow(["Client DIDs:", f"{client_calls[0]['client_did_count']}"])
            csvwriter.writerow([
                "Client Extensions:",
                f"{client_calls[0]['client_extension_count']}",
            ])
            csvwriter.writerow(["Reseller Cost:", f"${client_total_reseller_cost:.2f}"])
            csvwriter.writerow([])  # Spacing before call details

            # Track current extension for grouping
            current_extension = None
            
            # Process calls grouped by extension
            for call in client_calls:
                extension = call["extension"]
                
                # Start new extension section if needed
                if current_extension != extension:
                    if current_extension is not None:
                        csvwriter.writerow([])  # Spacing between extensions

                    # Calculate extension-level totals
                    extension_calls = [c for c in client_calls if c["extension"] == extension]
                    extension_total_duration = sum(c["duration"] for c in extension_calls)
                    extension_total_reseller_cost = sum(c["reseller_cost"] for c in extension_calls)
                    extension_total_client_cost = sum(c["client_cost"] for c in extension_calls)
                    
                    # Convert extension duration to hours, minutes, seconds
                    extension_hours = extension_total_duration // 3600
                    extension_minutes = (extension_total_duration % 3600) // 60
                    extension_seconds = extension_total_duration % 60

                    # Update tracking and write extension header
                    current_extension = extension
                    csvwriter.writerow([
                        "Phone Number:",
                        f"{call['phone_number']}",
                        "Extension:",
                        f"{call['extension']}",
                    ])
                    
                    # Write extension details
                    csvwriter.writerow(["Plan:", f"{call['plan']}"])
                    csvwriter.writerow([
                        "Call Time:",
                        f"{extension_hours} hours, {extension_minutes} minutes, {extension_seconds} seconds",
                    ])
                    csvwriter.writerow([
                        "Client Billables:",
                        f"${extension_total_client_cost:.2f}",
                    ])
                    csvwriter.writerow([
                        "Reseller Cost:",
                        f"${extension_total_reseller_cost:.2f}",
                    ])
                    
                    # Write CDR header
                    csvwriter.writerow(["Call Detail Records (CDRs)"])
                    csvwriter.writerow([
                        "Start",
                        "Source",
                        "Destination",
                        "Duration",
                        "Reseller Cost",
                        "Client Cost",
                        "Caller IP",
                        "Call ID",
                        "Hangup Cause",
                    ])

                # Write individual call records
                csvwriter.writerow([
                    call["start"],
                    extension,
                    call["destination"],
                    call["duration"],
                    call["reseller_cost"],
                    call["client_cost"],
                    call["caller_ip"],
                    call["callid"],
                    call["hangupcause"],
                ])

# Fetch DID information for final report section
cursor_main.execute(
    """
    SELECT
        did.did,
        did.reseller_id,
        did.client_id,
        COALESCE(client.company, reseller.company) AS client_name,
        did.cr_date AS created_date
    FROM
        voipnow.channel_did AS did
    LEFT JOIN
        voipnow.client AS client ON did.client_id = client.id
    LEFT JOIN
        voipnow.client AS reseller ON did.reseller_id = reseller.id
"""
)
dids = cursor_main.fetchall()

# Append DID section to each reseller's report
for reseller_name, calls in resellers_data.items():
    filename = f"{year_month_str}_{reseller_name.replace(' ', '_')}_E164_BILL.csv"
    with open(filename, "a", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)
        
        # Write DID section header
        csvwriter.writerow([])  # Spacing before DID section
        csvwriter.writerow(["Reseller DIDs"])
        csvwriter.writerow(["Total DIDs:", f"{calls[0]['reseller_did_count']}"])
        csvwriter.writerow([
            "did", "reseller_id", "client_id", "client_name", "created_date"
        ])

        # Write DID details for this reseller
        for did in dids:
            if did["reseller_id"] == calls[0]["reseller_id"]:
                csvwriter.writerow([
                    did["did"],
                    did["reseller_id"],
                    did["client_id"],
                    did["client_name"],
                    did["created_date"]
                ])

# Cleanup database connections
cursor_main.close()
db_connection.close()