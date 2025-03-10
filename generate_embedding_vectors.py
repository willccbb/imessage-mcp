import polars as pl
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any
import numpy as np
import requests
import json

BASE_URL = "http://localhost:8000"

def create_chunks_with_overlap(df: pl.DataFrame, window_minutes: int = 30, offset_minutes: int = 10) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Create overlapping chunks from a conversation with metadata.
    Returns list of (chunk_text, metadata) tuples.
    """
    chunks = []
    
    # Sort messages by date
    df = df.sort("date_sent")
    
    # Create 3 different offsets
    offsets = [timedelta(minutes=i * offset_minutes) for i in range(3)]
    
    # Determine which author column to use
    author_col = 'author_name' if 'author_name' in df.columns else 'author_handle'
    
    for offset in offsets:
        current_chunk_messages = []
        chunk_start_time = None
        last_message_time = None
        
        for row in df.iter_rows(named=True):
            message_time = row['date_sent']
            
            # Add offset to message time for this window
            adjusted_time = message_time + offset
            
            # Start new chunk if:
            # 1. This is the first message
            # 2. There's a gap > 30 minutes from last message
            # 3. Current chunk window (30 mins) is exceeded
            if (chunk_start_time is None or 
                (last_message_time and (message_time - last_message_time).total_seconds() > 30 * 60) or
                (chunk_start_time and (adjusted_time - chunk_start_time).total_seconds() > window_minutes * 60)):
                
                # Save previous chunk if it exists
                if current_chunk_messages:
                    chunk_text = "\n".join(msg['text'] for msg in current_chunk_messages if msg['text'] is not None)
                    authors = [str(author) for author in set(msg[author_col] for msg in current_chunk_messages) if author is not None]
                    metadata = {
                        'chat_id': current_chunk_messages[0]['chat_id'],
                        'group_chat_name': current_chunk_messages[0].get('group_chat_name'),
                        'start_time': current_chunk_messages[0]['date_sent'],
                        'end_time': current_chunk_messages[-1]['date_sent'],
                        'authors': ', '.join(authors),  # Convert list to string immediately
                        'offset_minutes': offset.total_seconds() / 60
                    }
                    chunks.append((chunk_text, metadata))
                
                # Start new chunk
                current_chunk_messages = [row]
                chunk_start_time = adjusted_time
            else:
                current_chunk_messages.append(row)
            
            last_message_time = message_time
        
        # Don't forget the last chunk
        if current_chunk_messages:
            chunk_text = "\n".join(msg['text'] for msg in current_chunk_messages if msg['text'] is not None)
            authors = [str(author) for author in set(msg[author_col] for msg in current_chunk_messages) if author is not None]
            metadata = {
                'chat_id': current_chunk_messages[0]['chat_id'],
                'group_chat_name': current_chunk_messages[0].get('group_chat_name'),
                'start_time': current_chunk_messages[0]['date_sent'],
                'end_time': current_chunk_messages[-1]['date_sent'],
                'authors': ', '.join(authors),  # Convert list to string immediately
                'offset_minutes': offset.total_seconds() / 60
            }
            chunks.append((chunk_text, metadata))
    
    return chunks

def process_chats(chat_dfs: List[pl.DataFrame], batch_size: int = 32) -> None:
    """
    Process all chat dataframes to create chunks and store them in the local embeddings database.
    
    Args:
        chat_dfs: List of polars DataFrames containing chat data
        batch_size: Number of chunks to process at once
    """
    # Reset/create the imessages collection
    try:
        requests.post(f"{BASE_URL}/reset_collection/imessages").json()
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the embeddings server. Make sure it's running at http://localhost:8000")
        return
    
    all_documents = []
    all_metadata = []
    
    # Create chunks for all chats
    for chat_df in chat_dfs:
        chunks_with_metadata = create_chunks_with_overlap(chat_df)
        
        # Process in batches
        for i in range(0, len(chunks_with_metadata), batch_size):
            batch = chunks_with_metadata[i:i + batch_size]
            
            # Separate texts and metadata
            documents = [chunk[0] for chunk in batch]
            metadatas = [
                {
                    **chunk[1],
                    'start_time': chunk[1]['start_time'].isoformat(),
                    'end_time': chunk[1]['end_time'].isoformat(),
                }
                for chunk in batch
            ]
            
            # Add to collections for batch processing
            all_documents.extend(documents)
            all_metadata.extend(metadatas)
            
            # If we've reached batch_size or this is the last batch, send to server
            if len(all_documents) >= batch_size or i + batch_size >= len(chunks_with_metadata):
                try:
                    response = requests.post(
                        f"{BASE_URL}/batch_insert",
                        json={
                            "documents": all_documents,
                            "metadatas": all_metadata,
                            "collection_name": "imessages"
                        }
                    )
                    if response.status_code != 200:
                        print(f"Error inserting batch: {response.json()}")
                    
                    # Clear the collections after successful insertion
                    all_documents = []
                    all_metadata = []
                    
                except requests.exceptions.RequestException as e:
                    print(f"Error sending batch to server: {e}")
                    continue
        
        # Use the correct column name for the final print
        author_col = 'author_name' if 'author_name' in chat_df.columns else 'author_handle'
        authors = set(chat_df.filter(~pl.col(author_col).str.to_lowercase().str.contains('me'))[author_col])
        print(f"Processed chunks for chat with {authors}")
    
    # Get collection info to verify insertion
    try:
        info_response = requests.get(f"{BASE_URL}/collection_info/imessages")
        info = info_response.json()
        print(f"\nCollection Info:")
        print(json.dumps(info, indent=2))
    except requests.exceptions.RequestException as e:
        print(f"Error getting collection info: {e}")

# Example query function that can be used after processing
def query_messages(query_text: str, n_results: int = 5, filter_metadata: Dict = None) -> Dict:
    """gr
    Query the message database for similar messages.
    
    Args:
        query_text: The text to search for
        n_results: Number of results to return
        filter_metadata: Optional metadata filter criteria
    
    Returns:
        Dict containing search results
    """
    try:
        payload = {
            "query_texts": [query_text],
            "n_results": n_results,
            "collection_name": "imessages",
            "include": ["documents", "metadatas", "distances"]
        }
        if filter_metadata:
            payload["where"] = filter_metadata
            
        response = requests.post(f"{BASE_URL}/query", json=payload)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error querying messages: {e}")
        return None