#!/usr/bin/env python3
"""

This module provides functionality for managing and processing DIDs (Direct Inward Dialing numbers)
in a VoIP system. It handles DID range identification, product classification, database updates,
and report generation for carriers, resellers, and clients.

The module connects to a MySQL database to process and update DID information, identifies DID
ranges and their associated products, and can generate reports in both CSV and JSON formats.

Dependencies:
    - mysql.connector
    - argparse
    - datetime
    - csv
    - json
    - os
    - typing

Database Requirements:
    - MySQL/MariaDB with the following tables:
        - channel_did
        - client
"""

import mysql.connector
import argparse
from datetime import datetime, date, timedelta
import csv
import json
import os
from typing import Literal

# Define custom type for customer classification
CustomerType = Literal['CLIENT', 'RESELLER', 'CARRIER']

class DIDHandler:
    """
    Handles DID processing, management, and reporting operations.
    
    This class provides methods for processing DIDs, identifying ranges, updating database
    records, and generating reports. It supports different customer types (CLIENT, RESELLER,
    CARRIER) and can handle various DID products and range sizes.
    
    Attributes:
        customer_type (CustomerType): Type of customer (CLIENT, RESELLER, CARRIER)
        cutoff_date (date): Date limit for processing DIDs
        username (str): Database username
        password (str): Database password
        db (mysql.connector.connection): Database connection
        cursor (mysql.connector.cursor): Database cursor
        product_mappings (dict): Cached product mappings from database
    """

    def __init__(self, customer_type: CustomerType, cutoff_date: date):
        """
        Initialize DIDHandler with customer type and cutoff date.
        
        Args:
            customer_type (CustomerType): Type of customer to process
            cutoff_date (date): Date limit for processing DIDs
        """
        self.customer_type = customer_type
        self.cutoff_date = cutoff_date
        self.username, self.password = self.get_mysql_credentials()
        
        # Establish database connection
        self.db = mysql.connector.connect(
            host="localhost",
            user=self.username,
            password=self.password,
            database="voipnow"
        )
        self.cursor = self.db.cursor(dictionary=True)
        
        # Initialize product mappings
        self.product_mappings = self.load_product_mappings()

    @staticmethod
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

    def load_product_mappings(self) -> dict:
        """
        Load product mappings from E164_products table.
        
        Returns:
            dict: Mapping of product codes to their database information
        """
        query = """
            SELECT E164_product_id, E164_product_code, did_map, 
                   product_name, billing_cost_setup, billing_cost_mrc
            FROM E164_products 
            WHERE did_map IS NOT NULL AND did_map != ''
        """
        self.cursor.execute(query)
        mappings = {}
        for row in self.cursor.fetchall():
            mappings[row['E164_product_code']] = {
                'id': row['E164_product_id'],
                'did_map': row['did_map'],
                'name': row['product_name'],
                'default_setup': float(row['billing_cost_setup']),
                'default_mrc': float(row['billing_cost_mrc'])
            }
        return mappings

    def determine_did_product(self, did_str: str) -> str:
        """
        Determine the product type for a given DID using database mappings.
        
        Args:
            did_str (str): DID number to analyze
            
        Returns:
            str: Product code or None if no match found
        """
        for product_code, mapping in self.product_mappings.items():
            query = f"""
                SELECT 1 
                FROM (SELECT %s as did) AS temp 
                WHERE {mapping['did_map']} = temp.did
            """
            self.cursor.execute(query, (did_str,))
            if self.cursor.fetchone():
                return product_code
        return None

    def get_product_pricing(self, product_code: str, owner_id: int) -> tuple[float, float]:
        """
        Get pricing information for a given product and owner.
        
        Args:
            product_code (str): E164 product code
            owner_id (int): Owner (carrier/reseller/client) ID
            
        Returns:
            tuple[float, float]: Setup cost and MRC for the product
        """
        if product_code not in self.product_mappings:
            return 0.0, 0.0
            
        product_id = self.product_mappings[product_code]['id']
        
        query = """
            SELECT 
                COALESCE(
                    MAX(CASE WHEN reseller_id = %s THEN billing_cost_setup END),
                    MAX(CASE WHEN reseller_id IS NULL THEN billing_cost_setup END)
                ) as setup_cost,
                COALESCE(
                    MAX(CASE WHEN reseller_id = %s THEN billing_cost_mrc END),
                    MAX(CASE WHEN reseller_id IS NULL THEN billing_cost_mrc END)
                ) as mrc_cost
            FROM E164_products
            WHERE E164_product_id = %s
        """
        
        self.cursor.execute(query, (owner_id, owner_id, product_id))
        result = self.cursor.fetchone()
        
        if result and result['setup_cost'] is not None and result['mrc_cost'] is not None:
            return float(result['setup_cost']), float(result['mrc_cost'])
            
        # Fall back to default pricing from mappings if no specific pricing found
        mapping = self.product_mappings[product_code]
        return mapping['default_setup'], mapping['default_mrc']

    def should_charge_setup(self, mod_date: datetime, report_month: date) -> bool:
        """
        Determine if setup fee should be charged based on modification date.
        
        Args:
            mod_date (datetime): DID modification date
            report_month (date): Report month
            
        Returns:
            bool: True if setup fee should be charged
        """
        if not mod_date:
            return False
            
        # Check if DID was modified in month prior to report
        prior_month = report_month.replace(day=1) - timedelta(days=1)
        mod_month = mod_date.date().replace(day=1)
        
        return mod_month == prior_month

    def get_base_query(self) -> str:
        """
        Get base SQL query for DID retrieval based on customer type.
        
        Returns:
            str: SQL query string
        """
        if self.customer_type == 'CLIENT':
            return """
                SELECT did, client_id as owner_id, cr_date, mod_date
                FROM channel_did
                WHERE client_id IS NOT NULL
                ORDER BY client_id, CAST(did AS UNSIGNED)
            """
        else:  # Both RESELLER and CARRIER views use reseller_id
            return """
                SELECT did, reseller_id as owner_id, cr_date, mod_date
                FROM channel_did
                WHERE reseller_id IS NOT NULL
                ORDER BY reseller_id, CAST(did AS UNSIGNED)
            """

    def get_update_query(self) -> str:
        """
        Get SQL query for updating DID information based on customer type.
        
        Returns:
            str: SQL query template string
        """
        if self.customer_type == 'CLIENT':
            return """
                UPDATE channel_did
                SET E164_client_product = %s
                WHERE client_id = %s AND {}
            """
        else:
            return """
                UPDATE channel_did
                SET E164_reseller_product = %s
                WHERE reseller_id = %s AND {}
            """

    def get_customer_name(self, owner_id: int) -> str:
        """
        Get customer name from database.
        
        Args:
            owner_id (int): Owner (carrier/reseller/client) ID
            
        Returns:
            str: Customer name
        """
        query = """
            SELECT company 
            FROM client 
            WHERE id = %s
        """
        self.cursor.execute(query, (owner_id,))
        result = self.cursor.fetchone()
        return result['company'] if result else f"Unknown ({owner_id})"

    def identify_ranges(self, dids, report_date: date):
        """
        Identify DID ranges from a list of DIDs.
        
        Args:
            dids (list): List of DID records
            report_date (date): Date of the report for setup fee calculation
            
        Returns:
            list: Processed DID ranges and individual DIDs
        """
        ranges = []
        current_range = []
        
        sorted_dids = sorted(dids, key=lambda x: (x['owner_id'], int(x['did'])))
        
        for did_entry in sorted_dids:
            if did_entry['cr_date'] and did_entry['cr_date'].date() > self.cutoff_date:
                continue
                
            if not current_range:
                current_range = [did_entry]
                continue
                
            prev_did = int(current_range[-1]['did'])
            curr_did = int(did_entry['did'])
            
            if (did_entry['owner_id'] == current_range[0]['owner_id'] and 
                curr_did == prev_did + 1):
                current_range.append(did_entry)
            else:
                ranges.extend(self.process_range(current_range, report_date))
                current_range = [did_entry]
        
        if current_range:
            ranges.extend(self.process_range(current_range, report_date))
        
        return ranges

    def process_range(self, range_entries, report_date: date):
        """
        Process a range of DIDs to determine their classification and pricing.
        
        Args:
            range_entries (list): List of DIDs in a potential range
            report_date (date): Date of the report for setup fee calculation
            
        Returns:
            list: Processed DID range information including pricing
        """
        results = []
        
        first_did = range_entries[0]['did']
        product_code = self.determine_did_product(first_did)
        
        if not product_code:
            return results
            
        if (product_code == 'AU-DID-100' and len(range_entries) >= 100) or \
           (product_code == 'AU-DID-10' and len(range_entries) >= 10):
            setup_cost, mrc_cost = self.get_product_pricing(product_code, range_entries[0]['owner_id'])
            
            should_setup = self.should_charge_setup(range_entries[0].get('mod_date'), report_date)
            actual_setup = setup_cost if should_setup else 0.0
            
            results.append({
                'did': range_entries[0]['did'],
                'range_start': range_entries[0]['did'],
                'range_end': range_entries[-1]['did'],
                'did_product': product_code,
                'owner_id': range_entries[0]['owner_id'],
                'product_name': self.product_mappings[product_code]['name'],
                'setup': actual_setup,
                'mrc': mrc_cost
            })
        else:
            for entry in range_entries:
                product_code = self.determine_did_product(entry['did'])
                if product_code:
                    setup_cost, mrc_cost = self.get_product_pricing(product_code, entry['owner_id'])
                    
                    should_setup = self.should_charge_setup(entry.get('mod_date'), report_date)
                    actual_setup = setup_cost if should_setup else 0.0
                    
                    results.append({
                        'did': entry['did'],
                        'range_start': None,
                        'range_end': None,
                        'did_product': product_code,
                        'owner_id': entry['owner_id'],
                        'product_name': self.product_mappings[product_code]['name'],
                        'setup': actual_setup,
                        'mrc': mrc_cost
                    })
        
        return results

    def process(self, report_date: date = None):
        """
        Process all DIDs and update database.
        
        Args:
            report_date (date, optional): Report date for setup fee calculation
            
        Returns:
            list: Processed DID results
        """
        if report_date is None:
            report_date = self.cutoff_date

        self.cursor.execute(self.get_base_query())
        dids = self.cursor.fetchall()
        results = self.identify_ranges(dids, report_date)
        
        if self.customer_type == 'CARRIER':
            results = sorted(results, key=lambda x: (x['owner_id'], int(x['did'])))
        
        self.update_database(results)
        return results

    def update_database(self, results):
        """
        Update database with processed DID information.
        
        Args:
            results (list): Processed DID information to update
        """
        for result in results:
            base_query = self.get_update_query()
            if result['range_start']:
                where_clause = "CAST(did AS UNSIGNED) BETWEEN CAST(%s AS UNSIGNED) AND CAST(%s AS UNSIGNED)"
                query = base_query.format(where_clause)
                self.cursor.execute(query, (
                    result['did_product'],
                    result['owner_id'],
                    result['range_start'],
                    result['range_end']
                ))
            else:
                where_clause = "did = %s"
                query = base_query.format(where_clause)
                self.cursor.execute(query, (
                    result['did_product'],
                    result['owner_id'],
                    result['did']
                ))
        
        self.db.commit()

    def generate_summary(self, results):
        """
        Generate summary statistics from processed results.
        
        Args:
            results (list): Processed DID results
            
        Returns:
            tuple[dict, list]: Summary statistics and list of product codes
        """
        summary = {
            'total_dids': 0,
            'total_setup': 0.0,
            'total_mrc': 0.0,
            'customers': {}
        }
        
        product_codes = sorted(self.product_mappings.keys())
        
        for result in results:
            owner_id = result['owner_id']
            
            if owner_id not in summary['customers']:
                summary['customers'][owner_id] = {
                    'name': self.get_customer_name(owner_id),
                    'total_dids': 0,
                    'total_setup': 0.0,
                    'total_mrc': 0.0
                }
                for code in product_codes:
                    summary['customers'][owner_id][code] = 0
            
            num_dids = 1
            if result['range_end']:
                start_num = int(result['range_start'])
                end_num = int(result['range_end'])
                num_dids = end_num - start_num + 1
            
            customer = summary['customers'][owner_id]
            customer['total_dids'] += num_dids
            customer['total_setup'] += result['setup']
            customer['total_mrc'] += result['mrc']
            
            if result['did_product'] in product_codes:
                customer[result['did_product']] += num_dids
            
            summary['total_dids'] += num_dids
            summary['total_setup'] += result['setup']
            summary['total_mrc'] += result['mrc']
                
        return summary, product_codes

    def print_results(self, results):
        """
        Print results with detailed summary table.
        
        Args:
            results (list): Processed DID results
        """
        # Print individual DID results
        print(f"{'DID':<15} {'Range Start':<15} {'Range End':<15} {'Product':<20} "
              f"{'Owner ID':<10} {'Setup':<10} {'MRC':<10}")
        print("-" * 110)
        
        for result in results:
            print(f"{result['did']:<15} "
                  f"{str(result['range_start'] or ''):<15} "
                  f"{str(result['range_end'] or ''):<15} "
                  f"{result['product_name'][:19]:<20} "
                  f"{str(result['owner_id']):<10} "
                  f"${result['setup']:<9.2f} "
                  f"${result['mrc']:<9.2f}")

        # Generate and print detailed summary
        summary, product_codes = self.generate_summary(results)
        
        print("\nCUSTOMER SUMMARY")
        print("-" * 150)
        
        # Print header
        header = (
            f"{'Customer Name':<30} {'ID':<6} {'Setup':>10} {'MRC':>10} {'Total':>8} "
        )
        for code in product_codes:
            product_name = self.product_mappings[code]['name']
            header += f"{product_name[:10]:>10} "
        print(header)
        print("-" * 150)
        
        # Print each customer's details
        for owner_id, customer in sorted(summary['customers'].items(), 
                                       key=lambda x: x[1]['name'].lower()):
            line = (
                f"{customer['name'][:29]:<30} "
                f"{owner_id:<6} "
                f"${customer['total_setup']:>9.2f} "
                f"${customer['total_mrc']:>9.2f} "
                f"{customer['total_dids']:>8} "
            )
            for code in product_codes:
                line += f"{customer[code]:>10} "
            print(line)
        
        # Print overall totals
        print("-" * 150)
        total_line = (
            f"{'TOTAL':<30} {'':>6} "
            f"${summary['total_setup']:>9.2f} "
            f"${summary['total_mrc']:>9.2f} "
            f"{summary['total_dids']:>8} "
        )
        product_totals = {code: sum(c[code] for c in summary['customers'].values())
                         for code in product_codes}
        for code in product_codes:
            total_line += f"{product_totals[code]:>10} "
        print(total_line)

    def cleanup(self):
        """Close database connections and cleanup resources."""
        self.cursor.close()
        self.db.close()


def save_to_csv(results, filename):
    """
    Save results to CSV file.
    
    Args:
        results (list): Results to save
        filename (str): Output filename
    """
    fieldnames = [
        'did', 'range_start', 'range_end', 'did_product', 'product_name',
        'owner_id', 'setup', 'mrc'
    ]
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Process DIDs and identify ranges")
    parser.add_argument("--customer-type", choices=['CLIENT', 'RESELLER', 'CARRIER'], 
                      default='RESELLER', help="Type of customer to process")
    parser.add_argument("-y", "--year", type=int, help="Year for cutoff date")
    parser.add_argument("-m", "--month", type=int, help="Month for cutoff date")
    parser.add_argument("-d", "--day", type=int, default=1, help="Day for cutoff date")
    parser.add_argument("--csv", help="Export to CSV file (provide filename)")
    parser.add_argument("--json", help="Export to JSON file (provide filename)")
    
    args = parser.parse_args()
    
    # Set cutoff date
    if args.year and args.month:
        report_date = date(args.year, args.month, args.day)
    else:
        report_date = date.today()
    
    # Initialize handler
    handler = DIDHandler(args.customer_type, report_date)
    
    try:
        # Process DIDs
        results = handler.process(report_date)
        
        # Export results if requested
        if args.csv:
            save_to_csv(results, args.csv)
            print(f"Results exported to CSV: {args.csv}")
            
        if args.json:
            with open(args.json, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"Results exported to JSON: {args.json}")
            
        # Print results if no export specified
        if not args.csv and not args.json:
            handler.print_results(results)
            
    finally:
        handler.cleanup()


if __name__ == "__main__":
    main()
