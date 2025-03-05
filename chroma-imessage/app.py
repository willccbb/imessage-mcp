from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import chromadb
from chromadb.config import Settings
from chromadb.utils import embedding_functions
import time
import shutil
import os

app = FastAPI(title="ChromaDB API Server")

# Initialize ChromaDB with persistent storage
DB_PATH = "./chroma_db"
chroma_client = chromadb.PersistentClient(path=DB_PATH)

# Initialize the embedding function using BAAI/bge-base-en-v1.5
embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(
    model_name="BAAI/bge-base-en-v1.5"
)

def get_or_create_collection(name: str = "default"):
    """Get or create a collection with the specified name."""
    return chroma_client.get_or_create_collection(
        name=name,
        embedding_function=embedding_function,
        metadata={"description": f"Collection for document embeddings: {name}"}
    )

# Create a default collection
collection = get_or_create_collection()

class BatchInsertRequest(BaseModel):
    documents: List[str]
    metadatas: List[Dict[str, Any]] | None = None
    ids: List[str] | None = None
    collection_name: str = "default"

class QueryRequest(BaseModel):
    query_texts: List[str]
    n_results: int = 5
    where: Dict[str, Any] | None = None
    where_document: Dict[str, Any] | None = None
    include: List[str] = ["metadatas", "documents", "distances"]
    collection_name: str = "default"

@app.post("/batch_insert")
async def batch_insert(request: BatchInsertRequest):
    try:
        collection = get_or_create_collection(request.collection_name)
        
        # Generate sequential IDs if not provided
        if request.ids is None:
            current_count = collection.count()
            request.ids = [str(i) for i in range(current_count, current_count + len(request.documents))]
        
        # Generate timestamps and default metadata if not provided
        if request.metadatas is None:
            timestamp = int(time.time())
            request.metadatas = [{"timestamp": timestamp, "index": i} for i in range(len(request.documents))]
        
        collection.add(
            documents=request.documents,
            metadatas=request.metadatas,
            ids=request.ids
        )
        return {"message": f"Successfully inserted {len(request.documents)} documents", "ids": request.ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def query(request: QueryRequest):
    try:
        collection = get_or_create_collection(request.collection_name)
        results = collection.query(
            query_texts=request.query_texts,
            n_results=request.n_results,
            where=request.where,
            where_document=request.where_document,
            include=request.include
        )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/collection_info/{collection_name}")
async def get_collection_info(collection_name: str = "default"):
    try:
        collection = get_or_create_collection(collection_name)
        return {
            "count": collection.count(),
            "name": collection.name,
            "metadata": collection.metadata
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/reset_collection/{collection_name}")
async def reset_collection(collection_name: str = "default"):
    """Delete and recreate a collection."""
    try:
        chroma_client.delete_collection(collection_name)
        collection = get_or_create_collection(collection_name)
        return {"message": f"Collection {collection_name} has been reset"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/delete_collection/{collection_name}")
async def delete_collection(collection_name: str):
    """Permanently delete a collection."""
    try:
        chroma_client.delete_collection(collection_name)
        return {"message": f"Collection {collection_name} has been deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/reset_database")
async def reset_database():
    """Delete and recreate the entire database."""
    try:
        # Close the client connection
        chroma_client.reset()
        
        # Remove the database directory
        if os.path.exists(DB_PATH):
            shutil.rmtree(DB_PATH)
        
        # Create default collection
        collection = get_or_create_collection()
        return {"message": "Database has been reset"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 