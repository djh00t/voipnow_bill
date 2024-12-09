import mysql.connector
import pandas as pd
import networkx as nx

class ClientHierarchyGraph:
    def __init__(self):
        self.username, self.password = self.get_mysql_credentials()
        self.db = mysql.connector.connect(
            host="localhost",
            user=self.username,
            password=self.password,
            database="voipnow"
        )
        self.cursor = self.db.cursor(dictionary=True)

    @staticmethod
    def get_mysql_credentials():
        with open("/etc/voipnow/.sqldb", "r") as file:
            data = file.read().strip()
            parts = data.split(":")
            return parts[1], parts[2]

    def fetch_client_data(self):
        query = "SELECT id, parent_client_id, company, level FROM client;"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def fetch_did_counts(self):
        query = """
        SELECT reseller_id, client_id, COUNT(did) AS did_count
        FROM channel_did
        GROUP BY reseller_id, client_id;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def fetch_did_details(self):
        # Query to retrieve DID details with reseller name and product code, sorted by DID
        query = """
        SELECT 
            cd.did, 
            IFNULL(cd.reseller_id, 1) AS reseller_id, 
            COALESCE(c.company, 'Owner') AS reseller_name, 
            ep.E164_product_code AS product_code
        FROM channel_did cd
        LEFT JOIN client c ON cd.reseller_id = c.id
        LEFT JOIN E164_products ep ON cd.E164_carrier_product = ep.E164_product_id
        ORDER BY cd.did ASC;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def validate_did_groups(self, did_details):
        # Initialize missing DID log
        missing_did_log = []

        # Group DIDs by 100-block prefix
        did_groups = {}
        for entry in did_details:
            did = entry["did"]
            prefix = did[:-2]  # Extract prefix, excluding the last two digits
            suffix = int(did[-2:])  # Extract the last two digits as integer
            
            if prefix not in did_groups:
                did_groups[prefix] = set()
            did_groups[prefix].add(suffix)

        # Validate each 100-block group for completeness
        for prefix, suffixes in did_groups.items():
            # Check if all numbers from 0 to 99 are in the group
            expected_suffixes = set(range(100))
            missing_suffixes = expected_suffixes - suffixes
            if missing_suffixes:
                missing_dids = [f"{prefix}{str(suffix).zfill(2)}" for suffix in sorted(missing_suffixes)]
                missing_did_log.append((prefix, missing_dids))

        # Display missing DIDs, if any
        if missing_did_log:
            print("Missing DIDs in 100-number blocks:")
            for prefix, missing_dids in missing_did_log:
                print(f"{prefix}: Missing DIDs - {', '.join(missing_dids)}")
        else:
            print("All 100-number DID blocks are complete.")

    def print_text_hierarchy(self):
        # Initialize graph
        G = nx.DiGraph()
        clients = self.fetch_client_data()
        did_data = self.fetch_did_counts()

        # Dictionaries to store counts of users, clients, and DIDs
        user_counts = {}
        client_counts = {}
        did_counts = {}

        # Process DID data to populate did_counts for resellers and clients
        for did in did_data:
            reseller_id = did['reseller_id']
            client_id = did['client_id']
            count = did['did_count']

            if reseller_id:
                did_counts[reseller_id] = did_counts.get(reseller_id, 0) + count
            if client_id:
                did_counts[client_id] = did_counts.get(client_id, 0) + count

        # Add nodes and edges while counting Level 100 users and Level 50 clients
        for client in clients:
            client_id = client['id']
            parent_client_id = client['parent_client_id']
            company = client['company']
            level = client['level']

            # Track Level 100 users by their parent Level 50 clients
            if level == 100 and parent_client_id:
                user_counts[parent_client_id] = user_counts.get(parent_client_id, 0) + 1
            # Track Level 50 clients by their parent Level 10 resellers
            elif level == 50 and parent_client_id:
                client_counts[parent_client_id] = client_counts.get(parent_client_id, 0) + 1

            # Create label with client ID and company name for all levels
            label = f"{client_id} - {company}"
            G.add_node(client_id, label=label, level=level)
            if parent_client_id:
                G.add_edge(parent_client_id, client_id)

        # Traverse and print the hierarchy with client, user, and DID counts
        def print_hierarchy(node, indent=""):
            level = G.nodes[node].get("level")
            if level == 100:
                return

            label = G.nodes[node]["label"]
            did_count = did_counts.get(node, 0)

            # For Level 10 (Carrier/Reseller), show client and DID counts
            if level == 10:
                client_count = client_counts.get(node, 0)
                print(f"{indent}{label} (Clients: {client_count}, DIDs: {did_count})")
            # For Level 50 (Company/Client), show user and DID counts
            elif level == 50:
                user_count = user_counts.get(node, 0)
                print(f"{indent}{label} (Users: {user_count}, DIDs: {did_count})")
            elif level == 0:
                print(f"{indent}{label} (Owner, DIDs: {did_count})")
            else:
                print(f"{indent}{label} (DIDs: {did_count})")

            # Recurse on children
            children = list(G.successors(node))
            for child in children:
                print_hierarchy(child, indent + "    ")

        # Find and print the top-level clients (those without a parent)
        top_clients = [n for n, d in G.in_degree() if d == 0]
        for top_client in top_clients:
            print_hierarchy(top_client)

    def display_did_table(self):
        # Fetch DID details
        did_details = self.fetch_did_details()

        # Validate DID groups
        self.validate_did_groups(did_details)

        # Create a DataFrame for tabular display
        df = pd.DataFrame(did_details)

        # Display the DataFrame as a table if data is not empty
        if not df.empty:
            print("\nDID Table:")
            print(df.to_string(index=False))
        else:
            print("No DID data found.")

    def close_connection(self):
        self.cursor.close()
        self.db.close()

# Usage
graph_generator = ClientHierarchyGraph()
graph_generator.print_text_hierarchy()  # Prints the client hierarchy
graph_generator.display_did_table()     # Displays DID details table and missing DID check
graph_generator.close_connection()      # Closes the database connection