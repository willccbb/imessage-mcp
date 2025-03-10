import datetime as dt
import sqlite3
import polars as pl


def extract_chats(db_path: str, contacts_df: pl.DataFrame) -> list[pl.DataFrame]:
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT ROWID, guid, display_name FROM chat')
    chats = cursor.fetchall()

    chat_dfs = []

    # print(f"Found {len(chats)} chats in sqlite db")
    for chat in chats:
        chat_id = chat[0]
        chat_guid = chat[1]
        group_chat_name = chat[2] if chat[2] else None
        
        # Query to get messages for the chat
        cursor.execute('''
            SELECT 
                message.ROWID, 
                message.text, 
                message.date, 
                message.date_delivered, 
                message.date_read, 
                message.is_from_me, 
                handle.id
            FROM message
            JOIN chat_message_join ON message.ROWID = chat_message_join.message_id
            LEFT JOIN handle ON message.handle_id = handle.ROWID
            WHERE chat_message_join.chat_id = ?
            ORDER BY message.date ASC
        ''', (chat_id,))
        
        messages = cursor.fetchall()
        
        data = []
        for msg in messages:
            message_id = msg[0]
            text = msg[1]
            date_sent = msg[2]
            date_delivered = msg[3]
            date_read = msg[4]
            is_from_me = msg[5]
            author = "Me" if is_from_me else msg[6]
            
            # Convert Apple timestamps to datetime
            # Apple timestamps are in nanoseconds since 2001-01-01
            def parse_apple_timestamp(ts):
                if ts:
                    return dt.datetime(2001, 1, 1) + dt.timedelta(seconds=ts/1e9)
                else:
                    return None
            
            date_sent = parse_apple_timestamp(date_sent)
            date_delivered = parse_apple_timestamp(date_delivered)
            date_read = parse_apple_timestamp(date_read)
            
            data.append({
                "chat_id": chat_id,
                "chat_guid": chat_guid,
                "group_chat_name": group_chat_name,
                "author_handle": author,
                "text": text,
                "date_sent": date_sent,
                "date_delivered": date_delivered,
                "date_read": date_read
            })
        
        df = pl.DataFrame(data, infer_schema_length=10_000)
        if len(df) == 0:
            # print(f"Warning: No messages found for chat {chat_id}")
            continue
        
        # Add author names to the dataframe
        df = df.join(contacts_df.select(pl.col("Phone Number"), pl.col("Name")), left_on="author_handle", right_on="Phone Number", how="left")\
            .with_columns(pl.when(pl.col("author_handle")=="Me").then(pl.lit("Me")).otherwise(pl.col("Name")).alias("author_name")).drop("Name") # Fill in my rows

        # You can save each DataFrame to a file or process it as needed
        # print(f"Data for Chat ID {chat_id} - {group_chat_name}:")
        # print(df.head())

        chat_dfs.append(df) 

    # Close the connection
    conn.close()

    return chat_dfs