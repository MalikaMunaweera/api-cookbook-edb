#!/usr/bin/env python
# This imports a pivotal export CSV into Shortcut. Requires that users
# and states are properly mapped in users.csv and states.csv.
# See README.md for prerequisites, setup, and usage.
import argparse
import csv
import logging
import os
import sqlite3
import sys
from datetime import datetime
from collections import Counter

from lib import *

parser = argparse.ArgumentParser(
    description="Imports the Pivotal Tracker CSV export to Shortcut",
)
parser.add_argument(
    "--apply", action="store_true", help="Actually creates the entities inside Shortcut"
)
parser.add_argument("--debug", action="store_true", help="Turns on debugging logs")


"""The batch size when running in batch mode"""
BATCH_SIZE = 100

"""The labels associated with all stories and epics that are created with this import script."""
PIVOTAL_TO_SHORTCUT_LABEL = "pivotal->shortcut"
_current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M")
PIVOTAL_TO_SHORTCUT_RUN_LABEL = f"pivotal->shortcut {_current_datetime}"

"""The label associated with all chore stories created from release types in Pivotal."""
PIVOTAL_RELEASE_TYPE_LABEL = "pivotal-release"

"""The label indicating a story had reviews in Pivotal."""
PIVOTAL_HAD_REVIEW_LABEL = "pivotal-had-review"


def write_failed_stories(failed_stories):
    """
    Write failed story creation attempts to CSV, appending to existing file.

    Args:
        failed_stories: List of stories that failed to create
    """
    if not failed_stories:
        return

    filename = "data/failed_stories.csv"
    fieldnames = ["story_name", "external_id", "error_message", "story_payload", "timestamp"]
    file_exists = os.path.exists(filename)

    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write header only if file is new
            if not file_exists:
                writer.writeheader()

            # Add timestamp to each row
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for story in failed_stories:
                writer.writerow({
                    "story_name": story["entity"].get("name", "Unknown"),
                    "external_id": story["entity"].get("external_id", "Unknown"),
                    "error_message": story.get("error_message", "Unknown error"),
                    "story_payload": json.dumps(story["entity"]),
                    "timestamp": current_time
                })

        print_with_timestamp(f"Added {len(failed_stories)} failed stories to {filename}")

    except IOError as e:
        printerr(f"Error writing to {filename}: {str(e)}")


def create_stories(stories):
    failed_stories = []
    successful_stories = []

    try:
        entities = [s["entity"] for s in stories]
        created_entities = sc_post("/stories/bulk", {"stories": entities})

        # Match created entities with original stories
        for created, story in zip(created_entities, stories):
            story["imported_entity"] = created
            successful_stories.append(story)

    except Exception as e:
        # If bulk creation fails, try creating stories one by one
        print_with_timestamp("Bulk creation failed. Attempting individual story creation...")

        for story in stories:
            try:
                created = sc_post("/stories", story["entity"])
                story["imported_entity"] = created
                successful_stories.append(story)
            except Exception as story_error:
                story["error_message"] = str(story_error)
                failed_stories.append(story)
                print_with_timestamp(f"Failed to create story {story['entity'].get('name', 'Unknown')}: {story_error}")

    # Write failed stories to CSV
    if failed_stories:
        write_failed_stories(failed_stories)

    return successful_stories


# These are the keys that are currently correctly populated in the
# build_entity map. They can be passed to the SC api unchanged. This
# list is effectively an allow list of top level attributes.
select_keys = {
    "story": [
        "comments",
        "created_at",
        "custom_fields",
        "deadline",
        "description",
        "estimate",
        "external_id",
        "external_links",
        "follower_ids",
        "group_id",
        "iteration_id",
        "labels",
        "name",
        "owner_ids",
        "requested_by_id",
        "story_type",
        "tasks",
        "workflow_state_id",
    ],
    "epic": [
        "created_at",
        "description",
        "external_id",
        "group_ids",
        "labels",
        "name",
    ],
}

review_as_comment_text_prefix = """\\[Pivotal Importer\\] Reviewers have been added as followers on this Shortcut Story.

The following table describes the state of their reviews when they were imported into Shortcut from Pivotal Tracker:

| Reviewer | Review Type | Review Status |
|---|---|---|"""


def escape_md_table_syntax(s):
    return s.replace("|", "\\|")


def build_run_label_entity():
    return {"type": "label", "entity": {"name": PIVOTAL_TO_SHORTCUT_RUN_LABEL}}


def build_entity(ctx, d):
    """Process the row to generate the payload needed to create the entity in Shortcut."""
    # ensure Shortcut entities have a Label that identifies this import
    d.setdefault("labels", []).extend(
        [{"name": PIVOTAL_TO_SHORTCUT_LABEL}, {"name": PIVOTAL_TO_SHORTCUT_RUN_LABEL}]
    )

    # The Shortcut Team/Group ID to assign to stories/epics,
    # may be None which the REST API interprets correctly.
    group_id = ctx["group_id"]

    # reconcile entity types
    type = "story"
    if d["story_type"] == "epic":
        type = "epic"

    # process comments
    comments = []
    for comment in d.get("comments", []):
        new_comment = comment.copy()
        author = new_comment.get("author")
        if author:
            del new_comment["author"]
            author_id = ctx["user_config"].get(author)
            if author_id:
                new_comment["author_id"] = author_id
        comments.append(new_comment)
    # other things we process are reified as comments,
    # so we'll add comments to the d later in processing

    # releases become Shortcut Stories of type "chore"
    if d["story_type"] == "release":
        d["story_type"] = "chore"
        d.setdefault("labels", []).append({"name": PIVOTAL_RELEASE_TYPE_LABEL})

    iteration = None
    pt_iteration_id = d["pt_iteration_id"] if "pt_iteration_id" in d else None
    if type == "story":
        # assign to team/group
        d["group_id"] = group_id
        # process workflow state
        pt_state = d.get("pt_state")
        if pt_state:
            d["workflow_state_id"] = ctx["workflow_config"][pt_state]

        # process tasks
        tasks = [
            {"description": title, "complete": state == "completed"}
            for (title, state) in zip(
                d.get("task_titles", []), d.get("task_states", [])
            )
        ]
        if tasks:
            d["tasks"] = tasks

        # process user fields
        user_to_sc_id = ctx["user_config"]
        requester = d.get("requester")
        if requester:
            # if requester isn't found, this will cause the api to use
            # the owner of the token as the requester
            sc_requester_id = user_to_sc_id.get(requester)
            if sc_requester_id:
                d["requested_by_id"] = sc_requester_id

        owners = d.get("owners")
        if owners:
            d["owner_ids"] = [
                # filter out owners that aren't found
                user_to_sc_id[owner]
                for owner in owners
                if owner in user_to_sc_id
            ]

        reviewers = d.get("reviewers")
        if reviewers:
            d["follower_ids"] = [
                user_to_sc_id[reviewer]
                for reviewer in reviewers
                if reviewer in user_to_sc_id
            ]
            d.setdefault("labels", []).append({"name": PIVOTAL_HAD_REVIEW_LABEL})

        # format table of all reviewers, types, and statuses as a comment on the imported story
        if reviewers:
            comment_text = review_as_comment_text_prefix
            for reviewer, review_type, review_status in zip(
                d.get("reviewers", []),
                d.get("review_types", []),
                d.get("review_states", []),
            ):
                reviewer = escape_md_table_syntax(reviewer)
                review_type = escape_md_table_syntax(review_type)
                review_status = escape_md_table_syntax(review_status)
                comment_text += f"\n|{reviewer}|{review_type}|{review_status}|"
            comments.append(
                {"author_id": d.get("requested_by_id", None), "text": comment_text}
            )

        # Custom Fields
        custom_fields = []
        # process priority as Priority custom field
        pt_priority = d.get("priority")
        if pt_priority:
            custom_fields.append(
                {
                    "field_id": ctx["priority_custom_field_id"],
                    "value_id": ctx["priority_config"][pt_priority],
                }
            )

        if custom_fields:
            d["custom_fields"] = custom_fields

        if pt_iteration_id:
            # Python dicts are not hashable and thus can't be
            # put into a set. To avoid extra-extra bookeeping,
            # capturing this as a trivially-parsable string
            # which can be accrued in a set in the entity
            # collector.
            start_date = d["pt_iteration_start_date"]
            end_date = d["pt_iteration_end_date"]
            iteration = "|".join([pt_iteration_id, start_date, end_date])

        # as a last step, ensure comments (both those that were comments
        # in Pivotal, and those we add during import to fill feature gaps)
        # are all added to the d dict
        if comments:
            d["comments"] = comments
        elif "comments" in d:
            del d["comments"]

    elif type == "epic":
        # While Pivotal's model does not have a requester or owners for
        # Epics, we can still apply the provided Team/Group assignment.
        d["group_ids"] = [group_id] if group_id is not None else []

    entity = {k: d[k] for k in select_keys[type] if k in d}
    return {
        "type": type,
        "entity": entity,
        "iteration": iteration,
        "pt_iteration_id": pt_iteration_id,
        "parsed_row": d,
    }


def load_mapping_csv(csv_file, from_key, to_key, to_transform=identity):
    d = {}
    with open(csv_file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            val_str = row.get(to_key)
            val = None
            if val_str:
                val = to_transform(val_str)
            d[row[from_key]] = val

    return d


def load_priorities(csv_file):
    logger.debug(f"Loading priorities from {csv_file}")
    return load_mapping_csv(csv_file, "pt_priority", "shortcut_custom_field_value_id")


def load_users(csv_file):
    logger.debug(f"Loading users from {csv_file}")
    email_to_id = {user["email"]: user["id"] for user in fetch_members()}
    user_to_email = load_mapping_csv(csv_file, "pt_user_name", "shortcut_user_email")
    return {
        pt_user: email_to_id.get(sc_email)
        for pt_user, sc_email in user_to_email.items()
        if sc_email
    }


def load_workflow_states(csv_file):
    logger.debug(f"Loading workflow states from {csv_file}")
    return load_mapping_csv(csv_file, "pt_state", "shortcut_state_id", int)


def get_mock_emitter():
    _mock_global_id = 0

    def _get_next_id():
        nonlocal _mock_global_id
        id = _mock_global_id
        _mock_global_id += 1
        return id

    def mock_emitter(items):
        for item in items:
            entity_id = _get_next_id()
            created_entity = item["entity"].copy()
            created_entity["id"] = entity_id
            created_entity["entity_type"] = item["type"]
            created_entity["app_url"] = f"https://example.com/entity/{entity_id}"
            item["imported_entity"] = created_entity
            print_with_timestamp(
                '[DRY RUN] Creating {} {} "{}"'.format(
                    item["type"], entity_id, item["entity"]["name"]
                )
            )
        return items

    return mock_emitter


def collect_epic_label_mapping(epics):
    """
    Return a dict mapping label names to Shortcut Epic ID.
    """
    epic_label_map = {}
    for epic in epics:
        for label in epic["entity"]["labels"]:
            label_name = label["name"]
            if (
                label_name is not PIVOTAL_TO_SHORTCUT_LABEL
                and label_name is not PIVOTAL_TO_SHORTCUT_RUN_LABEL
            ):
                epic_label_map[label_name] = epic["imported_entity"]["id"]
    return epic_label_map


def assign_stories_to_epics(stories, epics):
    """
    Mutate the `stories` to set an epic_id if that story is assigned to that epic.
    """
    epic_label_map = collect_epic_label_mapping(epics)
    for story in stories:
        for label in story["entity"].get("labels", []):
            label_name = label["name"]
            epic_id = epic_label_map.get(label_name)
            logger.debug(f"story epic id {epic_id}")
            if epic_id is not None:
                story["entity"]["epic_id"] = epic_id
    return stories


def collect_pt_iteration_mapping(iterations):
    """
    Return a dict mapping Pivotal iteration IDs to their corresponding Shortcut Iteration IDs.
    """
    d = {}
    for iteration in iterations:
        pt_iteration_id = iteration["pt_iteration_id"]
        sc_iteration_id = iteration["imported_entity"]["id"]
        d[str(pt_iteration_id)] = sc_iteration_id
    return d


def assign_stories_to_iterations(stories, iterations):
    """
    Mutate the `stories` to set an iteration_id if that story is assigned to that iteration.
    """
    pt_iteration_mapping = collect_pt_iteration_mapping(iterations)
    for story in stories:
        pt_iteration_id = story["pt_iteration_id"]
        if pt_iteration_id:
            sc_iteration_id = pt_iteration_mapping[str(pt_iteration_id)]
            story["entity"]["iteration_id"] = sc_iteration_id
    return stories


class EntityCollector:
    """
    Manages the collection and processing of entities for Shortcut import.

    Handles stories, epics, iterations, and labels while maintaining proper
    relationships between entities. Processes files and manages batch operations.
    """

    def __init__(self, emitter, is_dry_run):
        self.stories = []
        self.epics = []
        self.files = []
        self.iteration_strings = set()
        self.iterations = []
        self.labels = []
        self.emitter = emitter
        self.is_dry_run = is_dry_run
        print_with_timestamp(f"EntityCollector initialized with is_dry_run={is_dry_run}")

    def collect(self, item):
        if item["type"] == "story":
            self.stories.append(item)
            if item["iteration"]:
                self.iteration_strings.add(item["iteration"])
        elif item["type"] == "epic":
            self.epics.append(item)
        elif item["type"] == "label":
            self.labels.append(item)
        else:
            raise RuntimeError("Unknown entity type {}".format(item["type"]))

        return {item["type"]: 1}

    def process_story_batch(self, batch):
        """Process a batch of stories including their file attachments."""
        # First process all files for this batch
        processed_stories = process_files_for_stories(batch, self.is_dry_run)

        # Then create the stories using bulk API
        try:
            created_stories = self.emitter(processed_stories)
            print_with_timestamp(f"{'[DRY RUN] ' if self.is_dry_run else ''}Successfully created batch of {len(created_stories)} stories")
            return created_stories
        except Exception as e:
            print_with_timestamp(f"{'[DRY RUN] ' if self.is_dry_run else ''}Batch creation failed: {str(e)}")
            write_failed_stories(processed_stories)
            return []

    def commit(self):
        created_entities = []
        written_entities = set()  # Track what we've written to CSV

        # Create all the default labels
        print_with_timestamp("Processing labels...")
        self.labels = self.emitter(self.labels)
        if not self.is_dry_run:
            label_entities = [label["imported_entity"] for label in self.labels]
            write_to_imported_entities_csv(label_entities, mode='w')  # Initialize file with labels
            written_entities.update(('label', entity['id']) for entity in label_entities)
        created_entities.extend(label["imported_entity"] for label in self.labels)
        for label in self.labels:
            if PIVOTAL_TO_SHORTCUT_RUN_LABEL == label["entity"]["name"]:
                label_url = label["imported_entity"]["app_url"]
                print_with_timestamp(
                    f"Import Started\n\n==> Click here to monitor import progress: {label_url}"
                )

        # Create all the epics
        print_with_timestamp("Processing epics...")
        self.epics = self.emitter(self.epics)
        if not self.is_dry_run:
            epic_entities = [epic["imported_entity"] for epic in self.epics]
            new_epics = [entity for entity in epic_entities
                         if ('epic', entity['id']) not in written_entities]
            if new_epics:
                write_to_imported_entities_csv(new_epics)  # Append epics
                written_entities.update(('epic', entity['id']) for entity in new_epics)
        created_entities.extend(epic["imported_entity"] for epic in self.epics)
        print_with_timestamp(f"Finished creating {len(self.epics)} epics")

        # Create all iterations
        iteration_entities = []
        for iteration_string in self.iteration_strings:
            id, start_date, end_date = iteration_string.split("|")
            name = f"PT {id}"
            iteration_entities.append(
                {
                    "type": "iteration",
                    "pt_iteration_id": id,
                    "entity": {
                        "name": name,
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                }
            )
        self.iterations = self.emitter(iteration_entities)
        if not self.is_dry_run:
            iteration_entities = [iteration["imported_entity"] for iteration in self.iterations]
            new_iterations = [entity for entity in iteration_entities
                             if ('iteration', entity['id']) not in written_entities]
            if new_iterations:
                write_to_imported_entities_csv(new_iterations)
                written_entities.update(('iteration', entity['id']) for entity in new_iterations)
        created_entities.extend(iteration["imported_entity"] for iteration in self.iterations)
        print_with_timestamp(f"Finished creating {len(self.iterations)} iterations")

        # Process stories in batches
        successful_stories = []
        for i in range(0, len(self.stories), BATCH_SIZE):
            batch = self.stories[i:i + BATCH_SIZE]
            print_with_timestamp(f"Processing batch {i//BATCH_SIZE + 1} of {(len(self.stories)-1)//BATCH_SIZE + 1}")

            # Link epics and iterations before processing
            assign_stories_to_epics(batch, self.epics)
            assign_stories_to_iterations(batch, self.iterations)

            # Process the batch
            created_batch = self.process_story_batch(batch)
            successful_stories.extend(created_batch)

            # If not in dry run, Write successful stories to CSV immediately
            if not self.is_dry_run:
                new_stories = []
                for story in created_batch:
                    entity = story["imported_entity"]
                    if ('story', entity['id']) not in written_entities:
                        new_stories.append(entity)
                        written_entities.add(('story', entity['id']))

                if new_stories:
                    print_with_timestamp(f"Writing batch of {len(new_stories)} stories to CSV")
                    write_to_imported_entities_csv(new_stories, mode='a')
                    created_entities.extend(new_stories)
            else:
                # In dry run, just add to created_entities without writing to CSV
                created_entities.extend(story["imported_entity"] for story in created_batch)

        print_with_timestamp(f"Finished creating {len(successful_stories)} stories")
        return created_entities


def add_attached_files_in_comments(row_info):
    try:
        # Establish connection to the SQLite database
        conn = sqlite3.connect("pivotal_dump.db")
        cursor = conn.cursor()

        if "external_id" in row_info and "comments" in row_info:
            external_id = row_info["external_id"]

            # Fetch detailed comment information
            cursor.execute("""
                SELECT C.id, C.text, FA.filename, FA.content_type
                FROM comment AS C
                LEFT JOIN file_attachment AS FA
                ON C.id = FA.comment_id
                WHERE C.story_id = ?
                ORDER BY C.id, FA.filename
            """, (external_id,))

            db_comments = cursor.fetchall()

            # Process and store the detailed comment information
            processed_comments = {}
            for comment in db_comments:
                comment_id = comment[0]
                if comment_id not in processed_comments:
                    processed_comments[comment_id] = {
                        'id': comment_id,
                        'text': comment[1] or "",  # Use empty string if text is None
                        'attachments': []
                    }
                if comment[2]:  # If there's a filename
                    processed_comments[comment_id]['attachments'].append({
                        'filename': comment[2],
                        'content_type': comment[3]
                    })

            row_info['db_comments'] = list(processed_comments.values())

            # Update the original comments in row_info with the processed ones
            for i, comment in enumerate(row_info['comments']):
                comment['text'] = row_info['db_comments'][i]['text']
                comment['attachments'] = row_info['db_comments'][i]['attachments']

    except sqlite3.Error as e:
        print(f"An error occurred while processing comments for story {row_info.get('external_id', 'unknown')}: {e}")

    finally:
        # Close the database connection
        if conn:
            conn.close()

    return row_info


def process_pt_csv_export(ctx, pt_csv_file, entity_collector):
    stats = Counter()
    stats.update(entity_collector.collect(build_run_label_entity()))

    with open(pt_csv_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        header = [col.lower() for col in next(reader)]
        for row in reader:
            row_info = parse_row(row, header)
            row_info = add_attached_files_in_comments(row_info)
            entity = build_entity(ctx, row_info)
            logger.debug("Emitting Entity: %s", entity)
            stats.update(entity_collector.collect(entity))

    print_with_timestamp("Summary of data to be imported")
    print_stats(stats)


def build_ctx(cfg):
    ctx = {
        "group_id": cfg["group_id"],
        "priority_config": load_priorities(cfg["priorities_csv_file"]),
        "priority_custom_field_id": cfg["priority_custom_field_id"],
        "user_config": load_users(cfg["users_csv_file"]),
        "workflow_config": load_workflow_states(cfg["states_csv_file"]),
    }
    logger.debug("Built context %s", ctx)
    return ctx


def process_files_for_stories(stories_batch, is_dry_run=False):
    """
    Process file attachments for a batch of stories.

    Args:
        stories_batch: List of story entities to process
        is_dry_run: Boolean indicating whether to actually upload files

    Returns:
        List of stories that were successfully processed (all files uploaded)
    """
    successful_stories = []
    failed_stories = []
    all_failed_files = []

    for story in stories_batch:
        pt_id = story["entity"]["external_id"]
        pt_files_dir = f"data/{pt_id}"
        story_failed = False

        if not os.path.isdir(pt_files_dir):
            successful_stories.append(story)
            continue

        print_with_timestamp(f"{'[DRY RUN] ' if is_dry_run else ''}Processing files for story {pt_id}...")

        # Get all files in the story directory
        all_story_files = {
            f: os.path.join(dirpath, f)
            for (dirpath, _, filenames) in os.walk(pt_files_dir)
            for f in filenames
        }

        # Process each comment's attachments
        for i, comment in enumerate(story["entity"].get("comments", [])):
            comment_attachments = comment.pop("attachments", [])

            if not comment_attachments:
                continue

            print_with_timestamp(f"{'[DRY RUN] ' if is_dry_run else ''}Uploading {len(comment_attachments)} files for comment {i} in story {pt_id}")

            # Find the full paths of files mentioned in the comment
            comment_file_paths = [
                all_story_files[attachment['filename']]
                for attachment in comment_attachments
                if attachment['filename'] in all_story_files
            ]

            if comment_file_paths:
                if is_dry_run:
                    successful_files = [{
                        "filename": os.path.basename(path),
                        "url": f"https://mock-url/{os.path.basename(path)}",
                    } for path in comment_file_paths]
                    failed_files = []
                else:
                    successful_files, failed_files = sc_upload_files(comment_file_paths)
                    # Write successful files to CSV immediately after upload
                    if successful_files:
                        write_to_imported_entities_csv(successful_files)

                # If any files failed to upload, mark the story as failed
                if failed_files:
                    story_failed = True
                    for failed in failed_files:
                        failed["story_id"] = pt_id
                    all_failed_files.extend(failed_files)
                    story["error_message"] = f"Failed to upload files: {', '.join(f['filename'] for f in failed_files)}"
                    break  # Stop processing remaining files for this story

                # Create file attachment strings and append to comment text
                file_attachment_strings = []
                for file_entity, attachment in zip(successful_files, comment_attachments):
                    filename = file_entity["filename"]
                    url = file_entity["url"]
                    content_type = attachment['content_type']
                    is_image = content_type.startswith('image/')
                    attachment_string = f"{'!' if is_image else ''}[{filename}]({url})"
                    file_attachment_strings.append(attachment_string)

                # Append file attachment strings to comment text
                if file_attachment_strings:
                    comment["text"] += "\n\n" + "\n".join(file_attachment_strings) + "\n"

            # Update the comment in the story entity
            story["entity"]["comments"][i] = comment

        # Add story to appropriate list based on success/failure
        if story_failed:
            failed_stories.append(story)
        else:
            successful_stories.append(story)

    # Write failed results to CSV files
    if not is_dry_run:
        if all_failed_files:
            write_failed_files_csv(all_failed_files)
        if failed_stories:
            write_failed_stories(failed_stories)

    return successful_stories


def write_to_imported_entities_csv(entities, mode='a'):
    """Write created entities to CSV file for future deletion."""
    if not entities:
        return

    try:
        with open(shortcut_imported_entities_csv, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, ["type", "id"])
            if mode == 'w':
                writer.writeheader()

            for entity in entities:
                row = {
                    "type": entity["entity_type"],
                    "id": str(entity["id"])
                }
                writer.writerow(row)
                print_with_timestamp(f"Wrote {row['type']} {row['id']} to shortcut_imported_entities CSV")

    except Exception as e:
        printerr(f"Error writing to CSV: {str(e)}")


def write_failed_files_csv(failed_files):
    """Write failed file uploads to CSV, appending to existing file."""
    if not failed_files:
        return

    filename = "data/failed_files.csv"
    file_exists = os.path.exists(filename)

    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, ["story_id", "filename", "error", "timestamp"])

            # Write header only if file is new
            if not file_exists:
                writer.writeheader()

            # Add timestamp to each row
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for failed_file in failed_files:
                failed_file["timestamp"] = current_time
                writer.writerow(failed_file)

        print_with_timestamp(f"Added {len(failed_files)} failed file entries to {filename}")

    except IOError as e:
        printerr(f"Error writing to {filename}: {str(e)}")


def clean_story(story):
    if "comments" in story["entity"]:
        for comment in story["entity"]["comments"]:
            comment.pop("attachments", None)
    return story


def sc_creator(items):
    """
    Creates entities in Shortcut via API calls.

    Processes different entity types (stories, epics, iterations, labels)
    and handles file attachments. Uses batch processing for stories to
    optimize API usage.
    """
    batch_stories = []
    all_successful_items = []

    # Initialize CSV file
    write_to_imported_entities_csv([], mode='w')

    def process_batch():
        if not batch_stories:
            return

        print_with_timestamp(f"Processing batch of {len(batch_stories)} stories")

        # First process all files for this batch
        processed_stories = process_files_for_stories(batch_stories)

        # Clean all stories to remove 'attachments' property from comments
        processed_stories = [clean_story(story) for story in processed_stories]

        # Then create the stories using bulk API
        try:
            entities = [s["entity"] for s in processed_stories]
            created_entities = sc_post("/stories/bulk", {"stories": entities})

            # Update stories with created entities
            for created, story in zip(created_entities, processed_stories):
                story["imported_entity"] = created
                all_successful_items.append(story)

            # Write successful stories to CSV
            write_to_imported_entities_csv(created_entities)

        except Exception as e:
            print_with_timestamp(f"Batch creation failed: {str(e)}")
            write_failed_stories(processed_stories)

    # Process non-story items first
    for item in items:
        if item["type"] != "story":
            try:
                if item["type"] == "epic":
                    res = sc_post("/epics", item["entity"])
                elif item["type"] == "iteration":
                    res = sc_post("/iterations", item["entity"])
                elif item["type"] == "label":
                    res = sc_post("/labels", item["entity"])
                else:
                    raise RuntimeError(f"Unknown entity type {item['type']}")

                item["imported_entity"] = res
                all_successful_items.append(item)
                write_to_imported_entities_csv([res])
            except Exception as e:
                print_with_timestamp(f"Failed to create {item['type']}: {str(e)}")
                write_failed_stories([item], f"failed_{item['type']}s.csv")

    # Process stories in batches
    for item in items:
        if item["type"] == "story":
            batch_stories.append(item)

            if len(batch_stories) >= BATCH_SIZE:
                process_batch()
                batch_stories.clear()

    # Process remaining stories
    if batch_stories:
        process_batch()

    return all_successful_items


def main(argv):
    args = parser.parse_args(argv[1:])
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    is_dry_run = not args.apply
    print_with_timestamp(f"Running in {'APPLY' if args.apply else 'DRY RUN'} mode")

    # Pass is_dry_run directly instead of trying to detect it from emitter
    emitter = sc_creator if args.apply else get_mock_emitter()
    entity_collector = EntityCollector(emitter, is_dry_run)

    # Rest of the main function remains the same
    validate_environment()
    cfg = load_config()
    ctx = build_ctx(cfg)
    print_rate_limiting_explanation()
    process_pt_csv_export(ctx, cfg["pt_csv_file"], entity_collector)

    created_entities = entity_collector.commit()
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
