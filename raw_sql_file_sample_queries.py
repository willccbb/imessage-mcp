import polars as pl
import sqlite3
import pprint

# Connect to the database
conn = sqlite3.connect('./chat.db')


# Get list of all tables
# cursor = conn.cursor()
# cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# tables = cursor.fetchall()

# print("Tables in chat.db:")
# print("----------------")
# for table in tables:
#     print(table[0])


# SQL query to print all messages given a list of chat ROWIDs
query = """
SELECT 
    chat.ROWID AS chat_rowid,
    message.ROWID AS message_rowid,
    chat.guid,
    chat.style,
    chat.state,
    chat.account_id,
    chat.properties,
    chat.chat_identifier,
    chat.service_name,
    chat.room_name,
    chat.account_login,
    chat.is_archived,
    chat.last_addressed_handle,
    chat.display_name,
    chat.group_id,
    chat.is_filtered,
    chat.successful_query,
    chat.engram_id,
    chat.server_change_token,
    chat.ck_sync_state,
    chat.original_group_id,
    chat.last_read_message_timestamp,
    chat.sr_server_change_token,
    chat.sr_ck_sync_state,
    chat.cloudkit_record_id,
    chat.sr_cloudkit_record_id,
    chat.last_addressed_sim_id,
    chat.is_blackholed,
    chat.syndication_date,
    chat.syndication_type,
    chat.is_recovered,
    message.text,
    message.date,
    message.handle_id,
    message.service,
    message.destination_caller_id,
    message.is_from_me,
    message.attributedBody,
    message.cache_has_attachments
FROM chat
LEFT JOIN chat_message_join ON chat.ROWID = chat_message_join.chat_id
LEFT JOIN message ON chat_message_join.message_id = message_rowid
WHERE chat.ROWID IN (
  112,
  224,
  424
)
"""

# Read into dataframe
# Print the message contents as a list, one per line
df = pl.read_database(query, conn, infer_schema_length=10_000)
pprint.pprint(df['text'].to_list())


# Close connection
conn.close()

