import os
import csv
import json
from datetime import datetime
from lib import sc_get, sc_post, print_with_timestamp

def read_config():
    """
    Read configuration from config.json file
    """
    try:
        with open('config.json', 'r') as config_file:
            return json.load(config_file)
    except Exception as e:
        print_with_timestamp(f"Error reading config.json: {str(e)}")
        return None

def get_group_stories(group_id):
    """
    Fetch all stories for a given group ID from Shortcut and create initial CSV
    """
    try:
        stories = sc_get(f"/groups/{group_id}/stories")
        return stories
    except Exception as e:
        print_with_timestamp(f"Error fetching stories: {str(e)}")
        return []

def create_initial_csv(stories, output_file):
    """
    Create initial CSV file with story IDs and external IDs
    Only called if the file doesn't exist
    """
    try:
        os.makedirs('data', exist_ok=True)
        
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['id', 'external_id', 'comment_created_at', 'comment_id', 'success', 'error']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for story in stories:
                writer.writerow({
                    'id': story.get('id'),
                    'external_id': story.get('external_id', ''),
                    'comment_created_at': '',
                    'comment_id': '',
                    'success': '',
                    'error': ''
                })
        print_with_timestamp(f"Successfully created initial CSV with {len(stories)} stories")
    except Exception as e:
        print_with_timestamp(f"Error creating CSV: {str(e)}")

def add_comment_to_story(story_id, external_id):
    """
    Add a comment to a story and return the response or error
    """
    try:
        comment_data = {
            "text": f"Pivotal Tracker Id {external_id}"
        }
        response = sc_post(f"/stories/{story_id}/comments", comment_data)
        return {
            'success': True,
            'created_at': response.get('created_at', ''),
            'comment_id': response.get('id', ''),
            'error': ''
        }
    except Exception as e:
        return {
            'success': False,
            'created_at': '',
            'comment_id': '',
            'error': str(e)
        }

def process_existing_stories(output_file):
    """
    Process stories from existing CSV and update comment information
    """
    updated_stories = []

    try:
        # Read existing CSV
        with open(output_file, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            stories = list(reader)

        # Process each story
        for story in stories:
            if story['external_id'] and not story['success']:  # Only process if has external_id and not already successful
                print_with_timestamp(f"Adding comment to story {story['id']}...")
                result = add_comment_to_story(story['id'], story['external_id'])

                # Update only the comment-related fields
                story.update({
                    'comment_created_at': result['created_at'],
                    'comment_id': result['comment_id'],
                    'success': str(result['success']),  # Convert to string for CSV
                    'error': result['error']
                })

            updated_stories.append(story)

        # Write updated data back to CSV
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['id', 'external_id', 'comment_created_at', 'comment_id', 'success', 'error']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(updated_stories)

        # Print summary
        successful = sum(1 for story in updated_stories if story['success'] == 'True')
        print_with_timestamp(f"Processing complete. Successfully processed {successful} out of {len(updated_stories)} stories.")

    except Exception as e:
        print_with_timestamp(f"Error processing existing stories: {str(e)}")
        # Print full error traceback for debugging
        import traceback
        print_with_timestamp("Full error details:")
        print(traceback.format_exc())

def main():
    try:
        # Check if API token is set
        if not os.getenv("SHORTCUT_API_TOKEN"):
            print_with_timestamp("Error: SHORTCUT_API_TOKEN environment variable is not set")
            return

        # Read configuration
        config = read_config()
        if not config or 'group_id' not in config:
            print_with_timestamp("Error: Unable to read group_id from config.json")
            return

        group_id = config['group_id']
        output_file = os.path.join('data', 'story_external_ids.csv')

        # Check if output file exists
        if not os.path.exists(output_file):
            # If file doesn't exist, fetch stories and create initial CSV
            print_with_timestamp(f"Fetching stories for group {group_id}...")
            stories = get_group_stories(group_id)

            if stories:
                create_initial_csv(stories, output_file)
            else:
                print_with_timestamp("No stories found or error occurred")
                return

        # Process existing stories and add comments
        print_with_timestamp("Processing existing stories from CSV...")
        process_existing_stories(output_file)

    except Exception as e:
        print_with_timestamp(f"Error in main execution: {str(e)}")
        # Print full error traceback for debugging
        import traceback
        print_with_timestamp("Full error details:")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()