#!/usr/bin/env python3

import mysql.connector
import argparse
from datetime import datetime, date
import csv
import json
import os
from typing import Literal

CustomerType = Literal['CLIENT', 'RESELLER', 'CARRIER']

class DIDHandler:
    def __init__(self, customer_type: CustomerType, cutoff_date: date):
        self.customer_type = customer_type
        self.cutoff_date = cutoff_date
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

    def determine_did_product(self, did_str: str) -> str:
        print(f"Processing DID: {did_str}")
        if did_str.startswith('6113') and len(did_str) == 8:
            print(f"Matched AU-DID-13 for DID: {did_str}")
            return 'AU-DID-13'
        elif did_str.startswith('611300') and len(did_str) == 12:
            print(f"Matched AU-DID-1300 for DID: {did_str}")
            return 'AU-DID-1300'
        elif did_str.startswith('611800') and len(did_str) == 12:
            print(f"Matched AU-DID-1800 for DID: {did_str}")
            return 'AU-DID-1800'
        elif did_str.startswith('614') and len(did_str) == 11:
            print(f"Matched AU-DIDMOB-1 for DID: {did_str}")
            return 'AU-DIDMOB-1'
        elif any(did_str.startswith(prefix) for prefix in ['612', '613', '617', '618']) and len(did_str) == 11:
            print(f"Matched AU-DID-1 for DID: {did_str}")
            return 'AU-DID-1'
        print(f"No match found for DID: {did_str}, assigning DEFAULT-PLAN")
        return 'DEFAULT-PLAN'

    def get_E164_product_info(self, did_product: str) -> tuple[int, int]:
        if did_product == 'AU-DID-100':
            return 4, 100
        elif did_product == 'AU-DID-10':
            return 3, 10
        else:
            return 1, 1

    def get_base_query(self) -> str:
        if self.customer_type == 'CLIENT':
            return """
                SELECT did, client_id as owner_id, cr_date
                FROM channel_did
                WHERE client_id IS NOT NULL
                ORDER BY client_id, CAST(did AS UNSIGNED)
            """
        else:  # Both RESELLER and CARRIER views use reseller_id
            return """
                SELECT did, reseller_id as owner_id, cr_date
                FROM channel_did
                WHERE reseller_id IS NOT NULL
                ORDER BY reseller_id, CAST(did AS UNSIGNED)
            """

    def get_update_query(self) -> str:
        if self.customer_type == 'CLIENT':
            return """
                UPDATE channel_did
                SET E164_client_product = %s,
                    E164_client_range_size = %s
                WHERE client_id = %s AND {}
            """
        else:  # Both RESELLER and CARRIER views update reseller fields
            return """
                UPDATE channel_did
                SET E164_reseller_product = %s,
                    E164_reseller_range_size = %s
                WHERE reseller_id = %s AND {}
            """

    def identify_ranges(self, dids):
        ranges = []
        current_range = []
        
        sorted_dids = sorted(dids, key=lambda x: (x['owner_id'], int(x['did'])))
        print(f"Sorted DIDs: {sorted_dids}")
        
        for did_entry in sorted_dids:
            print(f"Processing DID entry: {did_entry}")
            if did_entry['cr_date'] and did_entry['cr_date'].date() > self.cutoff_date:
                print(f"Skipping DID {did_entry['did']} due to cutoff date")
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
                ranges.extend(self.process_range(current_range))
                current_range = [did_entry]
        
        if current_range:
            ranges.extend(self.process_range(current_range))
        
        print(f"Identified ranges: {ranges}")
        return ranges

    def process_range(self, range_entries):
        results = []
        print(f"Processing range: {range_entries}")
        if len(range_entries) >= 100 and str(range_entries[0]['did']).endswith('00'):
            e164_product, range_size = self.get_E164_product_info('AU-DID-100')
            results.append({
                'did': range_entries[0]['did'],
                'range_start': range_entries[0]['did'],
                'range_end': range_entries[-1]['did'],
                'did_product': 'AU-DID-100',
                'owner_id': range_entries[0]['owner_id'],
                'E164_product': e164_product,
                'range_size': range_size
            })
        elif len(range_entries) >= 10 and str(range_entries[0]['did']).endswith('0'):
            e164_product, range_size = self.get_E164_product_info('AU-DID-10')
            results.append({
                'did': range_entries[0]['did'],
                'range_start': range_entries[0]['did'],
                'range_end': range_entries[-1]['did'],
                'did_product': 'AU-DID-10',
                'owner_id': range_entries[0]['owner_id'],
                'E164_product': e164_product,
                'range_size': range_size
            })
        else:
            for entry in range_entries:
                product = self.determine_did_product(entry['did'])
                if product:
                    e164_product, range_size = self.get_E164_product_info(product)
                    results.append({
                        'did': entry['did'],
                        'range_start': None,
                        'range_end': None,
                        'did_product': product,
                        'owner_id': entry['owner_id'],
                        'E164_product': e164_product,
                        'range_size': range_size
                    })
        print(f"Processed range results: {results}")
        return results

    def update_database(self, results):
        for result in results:
            base_query = self.get_update_query()
            if result['range_start']:
                where_clause = "CAST(did AS UNSIGNED) BETWEEN CAST(%s AS UNSIGNED) AND CAST(%s AS UNSIGNED)"
                query = base_query.format(where_clause)
                self.cursor.execute(query, (
                    result['E164_product'],
                    result['range_size'],
                    result['owner_id'],
                    result['range_start'],
                    result['range_end']
                ))
            else:
                where_clause = "did = %s"
                query = base_query.format(where_clause)
                self.cursor.execute(query, (
                    result['E164_product'],
                    result['range_size'],
                    result['owner_id'],
                    result['did']
                ))
        
        self.db.commit()

    def process(self):
        print("Starting process method")
        self.cursor.execute(self.get_base_query())
        dids = self.cursor.fetchall()
        print(f"Fetched DIDs: {dids}")
        results = self.identify_ranges(dids)
        
        # For carrier view, we want to aggregate the results differently
        if self.customer_type == 'CARRIER':
            results = sorted(results, key=lambda x: (x['owner_id'], int(x['did'])))
        
        self.update_database(results)
        return results

    def generate_summary(self, results):
        """Generate summary statistics from results"""
        summary = {
            'products': {},
            'total_dids': 0,
            'total_owners': len(set(r['owner_id'] for r in results))
        }
        
        for result in results:
            # Count products
            if result['did_product'] not in summary['products']:
                summary['products'][result['did_product']] = 0
            summary['products'][result['did_product']] += 1
            
            # Count total DIDs (accounting for ranges)
            if result['range_end']:
                start_num = int(result['range_start'])
                end_num = int(result['range_end'])
                summary['total_dids'] += (end_num - start_num + 1)
            else:
                summary['total_dids'] += 1
                
        return summary

    def print_results(self, results):
        """Print results with summary table"""
        # Print main results
        print(f"{'DID':<15} {'Range Start':<15} {'Range End':<15} {'Product':<12} {'Owner ID':<10} {'E164 Product':<13} {'Range Size'}")
        print("-" * 90)
        for result in results:
            print(f"{result['did']:<15} "
                  f"{str(result['range_start'] or ''):<15} "
                  f"{str(result['range_end'] or ''):<15} "
                  f"{result['did_product']:<12} "
                  f"{str(result['owner_id']):<10} "
                  f"{str(result['E164_product']):<13} "
                  f"{result['range_size']}")
        
        # Generate summary
        summary = self.generate_summary(results)
        
        # Print summary table
        print("\nSUMMARY")
        print("-" * 50)
        
        # Print product counts
        print("Products:")
        for product, count in sorted(summary['products'].items()):
            print(f"  {product:<15} {count:>8}")
        
        print("\nTotals:")
        print(f"  {'Total DIDs:':<15} {summary['total_dids']:>8}")
        owner_type = "Clients" if self.customer_type == 'CLIENT' else "Resellers" if self.customer_type == 'RESELLER' else "Carriers"
        print(f"  {'Total ' + owner_type + ':':<15} {summary['total_owners']:>8}")

    def cleanup(self):
        self.cursor.close()
        self.db.close()

def save_to_csv(results, filename):
    fieldnames = ['did', 'range_start', 'range_end', 'did_product', 'owner_id', 
                 'E164_product', 'range_size']
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

def save_to_json(results, filename):
    with open(filename, 'w') as jsonfile:
        json.dump(results, jsonfile, indent=2)

def main():
    parser = argparse.ArgumentParser(description="Process DIDs and identify ranges")
    parser.add_argument("-y", "--year", type=int, help="Year for cutoff date")
    parser.add_argument("-m", "--month", type=int, help="Month for cutoff date")
    parser.add_argument("-d", "--day", type=int, help="Day for cutoff date")
    parser.add_argument("--csv", nargs='?', const='', help="Export to CSV file (optional filename)")
    parser.add_argument("--json", nargs='?', const='', help="Export to JSON file (optional filename)")
    
    # Add mutually exclusive group for customer type
    customer_group = parser.add_mutually_exclusive_group()
    customer_group.add_argument("--client", action="store_true", default=True, help="Process client DIDs (default)")
    customer_group.add_argument("--reseller", action="store_true", help="Process reseller DIDs")
    customer_group.add_argument("--carrier", action="store_true", help="Process carrier DIDs")
    
    args = parser.parse_args()

    # Determine customer type
    customer_type = 'CLIENT'
    if args.reseller:
        customer_type = 'RESELLER'
    elif args.carrier:
        customer_type = 'CARRIER'

    # Set cutoff date
    if args.year and args.month and args.day:
        cutoff_date = date(args.year, args.month, args.day)
    else:
        cutoff_date = date.today()

    # Generate default filenames
    date_str = datetime.now().strftime('%Y%m%d')
    default_csv = f"{date_str}_{customer_type}_DID_RANGES.csv"
    default_json = f"{date_str}_{customer_type}_DID_RANGES.json"

    # Process DIDs
    handler = DIDHandler(customer_type, cutoff_date)
    results = handler.process()
    
    # Export results if requested
    if args.csv is not None:
        csv_filename = args.csv if args.csv else default_csv
        save_to_csv(results, csv_filename)
        print(f"Results exported to CSV: {csv_filename}")

    if args.json is not None:
        json_filename = args.json if args.json else default_json
        save_to_json(results, json_filename)
        print(f"Results exported to JSON: {json_filename}")

    # Print results to console if no export options specified
    if args.csv is None and args.json is None:
        handler.print_results(results)

    handler.cleanup()

if __name__ == "__main__":
    main()