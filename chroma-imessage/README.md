# ChromaDB FastAPI Server

A FastAPI server that hosts a persistent ChromaDB instance with local embeddings using BAAI/bge-base-en-v1.5 model.

## Features

- Persistent ChromaDB storage
- Local embedding generation using BAAI/bge-base-en-v1.5
- Batch document insertion
- Similarity search queries
- Collection information endpoint

## Setup

1. Install dependencies:
```bash
uv venv
source .venv/bin/activate
uv pip install -e .
```

2. Run the server:
```bash
python app.py
```

The server will start at `http://localhost:8000`

## API Endpoints

### 1. Batch Insert Documents
POST `/batch_insert`

Request body:
```json
{
    "documents": ["text1", "text2", ...],
    "metadatas": [{"key": "value"}, ...],  // optional
    "ids": ["id1", "id2", ...]  // optional
}
```

### 2. Query Documents
POST `/query`

Request body:
```json
{
    "query_texts": ["search query"],
    "n_results": 5  // optional, default=5
}
```

### 3. Get Collection Info
GET `/collection_info`

Returns the collection name and document count.

## Interactive API Documentation

Visit `http://localhost:8000/docs` for the interactive Swagger UI documentation.
