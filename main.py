import glob
import os
import polars as pl
from datetime import datetime, timedelta
from extract_contacts import extract_contacts
from extract_chats import extract_chats
from generate_embedding_vectors import process_chats

# Check if chat.db exists
if not os.path.exists('chat.db'):
    print("ERROR: chat.db file not found. Please make sure it is in the current directory. (Copy it here from ~/Library/Messages/chat.db)")
    exit()

if not os.path.exists('contacts_cache.csv') and not os.path.exists('contacts.abbu'):
    print("ERROR: contacts.abbu file not found. Please make sure it is in the current directory." +
          "Copy your `contacts.abbu` file from your mac, by going to the Contacts app, clicking on `Contacts` in the top left, then `File` -> `Export...` -> `Address Book Archive` -> Save to this directory as `contacts.abbu`.")
    exit()
if not os.path.exists('contacts_cache.csv'):
    # Extract contacts from the .abbu file
    print("Extracting contacts...")
    contacts = extract_contacts('contacts.abbu')
    contacts.write_csv('contacts_cache.csv')
else:
    # Load contacts from the .csv file
    contacts = pl.read_csv('contacts_cache.csv')

print(f"Found {len(contacts.unique('Name'))} contacts")

# Get all chats from the chat.db file
if len(glob.glob('chats/*.csv')) < 100: 
    print("Extracting & Formatting chats...")
    chat_dfs = extract_chats('chat.db', contacts)
    for chat_df in chat_dfs:
        try:
            name_maybe = list(chat_df.drop_nulls('group_chat_name')['group_chat_name'])
            name_maybe = name_maybe if len(name_maybe) > 0 else list(set(chat_df.filter(~pl.col('author_name').str.to_lowercase().str.contains('me'))['author_name']))
            name_maybe = name_maybe[0]if len(name_maybe) > 0 else list(set(chat_df.filter(~pl.col('author_handle').str.to_lowercase().str.contains('me'))['author_handle']))[0]
            name = ' '.join(name_maybe.split()).replace(' ', '_').replace("+","00").lower()
            if os.path.exists(f'chats/{name}.csv'):
                chat_df0 = pl.read_csv(f'chats/{name}.csv')
                chat_df = chat_df0.vstack(chat_df)
            chat_df.write_csv(f'chats/{name}.csv')
        except Exception as e:
            continue
else:
    chat_dfs = [pl.read_csv(f) for f in glob.glob('chats/*.csv')]
print(f"Retrieved {len(chat_dfs)} non-empty chats")


# # TODO: do embedding stuff here
print("Generating embeddings...")
processed_chunks = process_chats(chat_dfs)
print(f"Processed {len(processed_chunks)} chunks")

import pprint
pprint.pprint(processed_chunks[:10])