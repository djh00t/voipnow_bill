#!/data/opt/miniconda3/envs/e164bill/bin/python

import mysql.connector
import csv
from datetime import datetime, timedelta


# Function to get MySQL credentials
def get_mysql_credentials():
    with open("/etc/voipnow/.sqldb", "r") as file:
        data = file.read().strip()
        parts = data.split(":")
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
    host="localhost", user=username, password=password, database="voipnow"
)

cursor_main = db_connection.cursor(dictionary=True)

# Fetch all the DID counts for resellers
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

# Fetch all the DID counts for clients
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

# Fetch all the extension counts for resellers
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
    row["reseller_id"]: row["extension_count"]
    for row in cursor_main.fetchall()
}

# Fetch all the extension counts for clients
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

# Define the main query
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
    AND call_history.start BETWEEN DATE_SUB(LAST_DAY(NOW() - INTERVAL 1 MONTH), INTERVAL DAY(LAST_DAY(NOW() - INTERVAL 1 MONTH)) - 1 DAY)
                   AND LAST_DAY(NOW() - INTERVAL 1 MONTH)
ORDER BY
    reseller_name,
    client_name,
    call_history.extension_number,
    call_history.start ASC;
"""

# Execute the main query
cursor_main.execute(query)

# Fetch all rows at once
rows = cursor_main.fetchall()

# Process data per reseller and client
resellers_data = {}
for row in rows:
    reseller_name = row["reseller_name"]
    client_name = row["client_name"]
    reseller_id = row["reseller_id"]

    if reseller_name not in resellers_data:
        resellers_data[reseller_name] = []

    # Add pre-fetched DID and extension counts
    row["reseller_did_count"] = reseller_did_counts.get(reseller_id, 0)
    row["client_did_count"] = client_did_counts.get(row["client_id"], 0)
    row["reseller_extension_count"] = reseller_extension_counts.get(
        reseller_id, 0
    )
    row["client_extension_count"] = client_extension_counts.get(
        row["client_id"], 0
    )

    resellers_data[reseller_name].append(row)

# Generate CSV files
year, month = get_last_month()
year_month_str = f"{year}{month:02d}"

for reseller_name, calls in resellers_data.items():
    filename = f"{year_month_str}_{reseller_name.replace(' ', '_')}_OUTBOUND_CALLS.csv"
    with open(filename, "w", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)

        # Write reseller summary
        total_reseller_cost = sum(call["reseller_cost"] for call in calls)
        total_client_cost = sum(call["client_cost"] for call in calls)
        total_duration = sum(call["duration"] for call in calls)
        total_hours = total_duration // 3600
        total_minutes = (total_duration % 3600) // 60
        total_seconds = total_duration % 60

        reseller_id = calls[0][
            "reseller_id"
        ]  # Get the reseller_id from the first call
        csvwriter.writerow(
            [
                "Company Name:",
                f"{reseller_name}",
                f"Reseller ID:",
                f"{reseller_id}",
            ]
        )
        csvwriter.writerow(
            [
                "Total Call Time:",
                f"{total_hours} hours, {total_minutes} minutes, {total_seconds} seconds",
            ]
        )
        csvwriter.writerow(
            ["Total Client Billables:", f"${total_client_cost:.2f}"]
        )
        csvwriter.writerow(
            ["Total Reseller Cost:", f"${total_reseller_cost:.2f}"]
        )
        csvwriter.writerow(
            ["Total Reseller DIDs:", f"{calls[0]['reseller_did_count']}"]
        )
        csvwriter.writerow(
            [
                "Total Reseller Extensions:",
                f"{calls[0]['reseller_extension_count']}",
            ]
        )

        # Write data grouped by client and extension
        clients_grouped = {}
        for call in calls:
            client_name = call["client_name"]
            if client_name not in clients_grouped:
                clients_grouped[client_name] = []
            clients_grouped[client_name].append(call)

        for client_name, client_calls in clients_grouped.items():
            client_total_duration = sum(
                call["duration"] for call in client_calls
            )
            client_total_reseller_cost = sum(
                call["reseller_cost"] for call in client_calls
            )
            client_total_client_cost = sum(
                call["client_cost"] for call in client_calls
            )
            client_hours = client_total_duration // 3600
            client_minutes = (client_total_duration % 3600) // 60
            client_seconds = client_total_duration % 60

            client_id = client_calls[0][
                "client_id"
            ]  # Get the client_id from the first call
            csvwriter.writerow([])  # Blank line between client sections
            csvwriter.writerow(
                [
                    "Client Name:",
                    f"{client_name}",
                    "Client ID:",
                    f"{client_id}",
                ]
            )
            csvwriter.writerow(
                [
                    "Client Call Time:",
                    f"{client_hours} hours, {client_minutes} minutes, {client_seconds} seconds",
                ]
            )
            csvwriter.writerow(
                ["Client Billables:", f"${client_total_client_cost:.2f}"]
            )
            csvwriter.writerow(
                ["Client DIDs:", f"{client_calls[0]['client_did_count']}"]
            )
            csvwriter.writerow(
                [
                    "Client Extensions:",
                    f"{client_calls[0]['client_extension_count']}",
                ]
            )
            csvwriter.writerow(
                ["Reseller Cost:", f"${client_total_reseller_cost:.2f}"]
            )
            csvwriter.writerow([])  # Blank line between client sections

            current_extension = None
            for call in client_calls:
                extension = call["extension"]
                if current_extension != extension:
                    if current_extension is not None:
                        csvwriter.writerow([])  # Blank line between extensions

                    # Calculate totals for the current extension
                    extension_calls = [
                        c for c in client_calls if c["extension"] == extension
                    ]
                    extension_total_duration = sum(
                        c["duration"] for c in extension_calls
                    )
                    extension_total_reseller_cost = sum(
                        c["reseller_cost"] for c in extension_calls
                    )
                    extension_total_client_cost = sum(
                        c["client_cost"] for c in extension_calls
                    )
                    extension_hours = extension_total_duration // 3600
                    extension_minutes = (extension_total_duration % 3600) // 60
                    extension_seconds = extension_total_duration % 60

                    current_extension = extension
                    csvwriter.writerow(
                        [
                            "Phone Number:",
                            f"{call['phone_number']}",
                            "Extension:",
                            f"{call['extension']}",
                        ]
                    )
                    csvwriter.writerow(["Plan:", f"{call['plan']}"])
                    csvwriter.writerow(
                        [
                            "Call Time:",
                            f"{extension_hours} hours, {extension_minutes} minutes, {extension_seconds} seconds",
                        ]
                    )
                    csvwriter.writerow(
                        [
                            "Client Billables:",
                            f"${extension_total_client_cost:.2f}",
                        ]
                    )
                    csvwriter.writerow(
                        [
                            "Reseller Cost:",
                            f"${extension_total_reseller_cost:.2f}",
                        ]
                    )
                    csvwriter.writerow(["Call Detail Records (CDRs)"])
                    csvwriter.writerow(
                        [
                            "Start",
                            "Source",
                            "Destination",
                            "Duration",
                            "Reseller Cost",
                            "Client Cost",
                            "Caller IP",
                            "Call ID",
                            "Hangup Cause",
                        ]
                    )

                csvwriter.writerow(
                    [
                        call["start"],
                        extension,
                        call["destination"],
                        call["duration"],
                        call["reseller_cost"],
                        call["client_cost"],
                        call["caller_ip"],
                        call["callid"],
                        call["hangupcause"],
                    ]
                )

# Close the cursor and connection
cursor_main.close()
db_connection.close()
