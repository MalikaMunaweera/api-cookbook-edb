import os
import csv
from lib import sc_delete, print_with_timestamp

def delete_comments(csv_file):
    """
    Delete comments that match our specific text pattern
    """
    updated_stories = []
    try:
        # Read the CSV file
        with open(csv_file, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            stories = list(reader)

        # Process each story
        for story in stories:
            if story['success'] == 'True' and story['comment_id']:
                story_id = story['id']
                comment_id = story['comment_id']
                external_id = story['external_id']

                print_with_timestamp(f"Deleting comment {comment_id} from story {story_id}...")

                try:
                    sc_delete(f"/stories/{story_id}/comments/{comment_id}")
                    # Reset the comment-related fields
                    story.update({
                        'comment_created_at': '',
                        'comment_id': '',
                        'success': '',
                        'error': ''
                    })
                    print_with_timestamp(f"Successfully deleted comment from story {story_id}")
                except Exception as e:
                    print_with_timestamp(f"Error deleting comment from story {story_id}: {str(e)}")
                    story['error'] = f"Error deleting comment: {str(e)}"

            updated_stories.append(story)

        # Write the updated data back to CSV
        with open(csv_file, 'w', newline='') as csvfile:
            fieldnames = ['id', 'external_id', 'comment_created_at', 'comment_id', 'success', 'error']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(updated_stories)

        print_with_timestamp("Comment deletion process complete")

    except Exception as e:
        print_with_timestamp(f"Error processing CSV: {str(e)}")
        import traceback
        print_with_timestamp("Full error details:")
        print(traceback.format_exc())

def main():
    try:
        # Check if API token is set
        if not os.getenv("SHORTCUT_API_TOKEN"):
            print_with_timestamp("Error: SHORTCUT_API_TOKEN environment variable is not set")
            return

        csv_file = os.path.join('data', 'story_external_ids.csv')

        if not os.path.exists(csv_file):
            print_with_timestamp(f"Error: CSV file not found at {csv_file}")
            return

        print_with_timestamp("Starting comment deletion process...")
        delete_comments(csv_file)

    except Exception as e:
        print_with_timestamp(f"Error in main execution: {str(e)}")
        import traceback
        print_with_timestamp("Full error details:")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()