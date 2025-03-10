import glob
import os
import polars as pl
from datetime import datetime
from generate_embedding_vectors import process_chats

def load_all_chats():
    # Get all CSV files from chats directory
    chat_files = glob.glob('chats/*.csv')
    if not chat_files:
        print("No CSV files found in chats/ directory")
        return
    
    print(f"Found {len(chat_files)} chat files")
    
    # Load each CSV into a Polars DataFrame
    chat_dfs = []
    for file in chat_files:
        try:
            # Read CSV with proper datetime parsing
            df = pl.read_csv(file)
            
            # Convert date columns to datetime using ISO format
            date_columns = ['date_sent', 'date_delivered', 'date_read']
            for col in date_columns:
                if col in df.columns:
                    df = df.with_columns(
                        pl.col(col).str.strptime(
                            pl.Datetime, 
                            format="%Y-%m-%dT%H:%M:%S.%f",
                            strict=False
                        )
                    )
            
            chat_dfs.append(df)
            print(f"Loaded {file}")
        except Exception as e:
            print(f"Error loading {file}: {e}")
            continue
    
    if not chat_dfs:
        print("No chat DataFrames were successfully loaded!")
        return
        
    # Process all chats and add to embeddings database
    print("\nProcessing chats and generating embeddings...")
    process_chats(chat_dfs)

if __name__ == "__main__":
    load_all_chats() 