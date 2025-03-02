import sys
import logging
import argparse
import csv
import os
import requests
import time
from collections import Counter

from lib import *

parser = argparse.ArgumentParser(
    description="Deletes entities created in a previous Pivotal import",
)
parser.add_argument(
    "--apply", action="store_true", help="Actually deletes the entities inside Shortcut"
)
parser.add_argument("--debug", action="store_true", help="Turns on debugging logs")

def delete_entity(entity_type, entity_id):
    """Delete an entity and return True if successful, False otherwise."""
    prefix = {
        "story": "/stories/",
        "epic": "/epics/",
        "file": "/files/",
        "iteration": "/iterations/",
        "label": "/labels/"
    }.get(entity_type)

    if not prefix:
        printerr(f"Unknown entity type: {entity_type}")
        return False

    try:
        print_with_timestamp(f"Sending delete request for {entity_type} {entity_id}...")
        response = sc_delete(f"{prefix}{entity_id}")

        # Check if deletion was successful
        if response.status_code == 204:
            print_with_timestamp(f"Successfully deleted {entity_type} {entity_id}")
            return True
        else:
            printerr(f"Unexpected response status {response.status_code} for {entity_type} {entity_id}")
            return False

    except requests.HTTPError as err:
        if err.response.status_code == 404:
            # Entity already deleted or doesn't exist
            print_with_timestamp(f"{entity_type} {entity_id} not found (may have been already deleted)")
            return True
        else:
            printerr(f"Unable to delete {entity_type} {entity_id}")
            printerr(f"Error: {err}")
            printerr(f"Response: {err.response.text if err.response else 'No response'}")
            return False

    except requests.exceptions.RequestException as e:
        printerr(f"Request failed for {entity_type} {entity_id}: {str(e)}")
        return False

    except Exception as e:
        printerr(f"Unexpected error deleting {entity_type} {entity_id}: {str(e)}")
        return False


def update_csv_after_deletion(successful_deletions):
    """Update the CSV file to remove successfully deleted entities."""
    if not successful_deletions:
        return

    temp_filename = "temp_imported_entities.csv"
    remaining_entities = []

    try:
        # Read existing entries and filter out deleted ones
        with open(shortcut_imported_entities_csv, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            fieldnames = reader.fieldnames
            for row in reader:
                key = (row['type'], row['id'])
                if key not in successful_deletions:
                    remaining_entities.append(row)

        # Write remaining entries to temporary file
        with open(temp_filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(remaining_entities)

        # Replace original file with temporary file
        os.replace(temp_filename, shortcut_imported_entities_csv)
        print_with_timestamp(f"Updated {shortcut_imported_entities_csv} - removed {len(successful_deletions)} deleted entities")

    except Exception as e:
        printerr(f"Error updating CSV file: {str(e)}")
        if os.path.exists(temp_filename):
            os.remove(temp_filename)


def main(argv):
    args = parser.parse_args(argv[1:])
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    print_with_timestamp("Starting deletion process...")
    print_rate_limiting_explanation()

    # We need to make API requests before fully validating local config.
    validate_environment()

    if not os.path.exists(shortcut_imported_entities_csv):
        printerr(f"CSV file not found: {shortcut_imported_entities_csv}")
        return 1

    counter = Counter()
    successful_deletions = set()
    entities_to_delete = set()  # For deduplication

    # Define deletion order (reverse order of creation)
    type_order = {'file': 1, 'story': 2, 'iteration': 3, 'epic': 4, 'label': 5}

    try:
        # First pass: read and deduplicate entries
        with open(shortcut_imported_entities_csv) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                entities_to_delete.add((row["type"], row["id"]))

        # Sort entities based on type order
        sorted_entities = sorted(entities_to_delete,
                               key=lambda x: type_order.get(x[0], 0))

        print_with_timestamp(f"Found {len(sorted_entities)} unique entities to delete")

        # Count by type before deletion
        pre_delete_counter = Counter(entity_type for entity_type, _ in sorted_entities)
        print_with_timestamp("Entities to be deleted:")
        print_stats(pre_delete_counter)

        if args.apply:
            for entity_type, entity_id in sorted_entities:
                print_with_timestamp(f"Attempting to delete {entity_type} {entity_id}...")
                if delete_entity(entity_type, entity_id):
                    counter[entity_type] += 1
                    successful_deletions.add((entity_type, entity_id))
                    time.sleep(0.5)  # 500ms delay

            update_csv_after_deletion(successful_deletions)
        else:
            counter = pre_delete_counter
            print_with_timestamp("Dry run! Rerun with --apply to actually delete!")

        if counter:
            print_with_timestamp("Deletion stats:")
            print_stats(counter)

    except Exception as e:
        printerr(f"Error processing CSV file: {str(e)}")
        return 1

    print_with_timestamp("Deletion process completed.")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
