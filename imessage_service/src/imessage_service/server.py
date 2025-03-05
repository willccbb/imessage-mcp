import os
import requests
import logging
from collections.abc import Sequence
from typing import Any, Optional, Dict, List, Union
from pathlib import Path

from dotenv import load_dotenv
from mcp.server import Server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
    ImageContent,
    EmbeddedResource,
    LoggingLevel
)
from pydantic import BaseModel

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("imessage-service")

# Vector DB Configuration
VECTOR_DB_URL = os.getenv('VECTOR_DB_URL', 'http://localhost:8000')
DEFAULT_COLLECTION = "imessages"

class VectorDBClient:
    def __init__(self, base_url: str = VECTOR_DB_URL):
        self.base_url = base_url.rstrip('/')
        
    def query_collection(
        self,
        query_text: str,
        n_results: int = 10,
        collection_name: str = DEFAULT_COLLECTION,
        where: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Query the vector database for similar chunks."""
        try:
            payload = {
                "query_texts": [query_text],
                "n_results": n_results,
                "collection_name": collection_name,
                "include": ["documents", "metadatas", "distances"]
            }
            if where:
                payload["where"] = where
                
            response = requests.post(
                f"{self.base_url}/query",
                json=payload
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Vector DB query failed: {str(e)}")
            raise RuntimeError(f"Vector DB error: {str(e)}")

class QueryResult(BaseModel):
    document: str
    metadata: Dict[str, Any]
    distance: float

def format_query_results(results: Dict[str, Any]) -> List[QueryResult]:
    """Format raw vector DB results into structured objects."""
    formatted_results = []
    
    if not results.get('documents') or not results['documents'][0]:
        return formatted_results
        
    documents = results['documents'][0]
    metadatas = results['metadatas'][0]
    distances = results['distances'][0]
    
    for doc, meta, dist in zip(documents, metadatas, distances):
        formatted_results.append(
            QueryResult(
                document=doc,
                metadata=meta,
                distance=dist
            )
        )
    
    return formatted_results

# Initialize vector DB client
vector_db = VectorDBClient()

app = Server("imessage-service")

@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools for searching iMessage history."""
    return [
        Tool(
            name="search_messages",
            description="Search through iMessage history using semantic similarity to find relevant messages and conversations",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "What you want to search for in your message history (e.g. 'conversations about machine learning')"
                    },
                    "n_results": {
                        "type": "integer",
                        "description": "Number of message chunks to return (default: 10)",
                        "default": 10
                    },
                    "category": {
                        "type": "string",
                        "description": "Optional filter by message category (e.g. 'personal', 'work', 'family')",
                        "default": None
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="search_chat",
            description="Search within a specific iMessage chat or conversation",
            inputSchema={
                "type": "object", 
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "What to search for in this conversation"
                    },
                    "chat_id": {
                        "type": "string",
                        "description": "ID of the chat/conversation to search within"
                    },
                    "n_results": {
                        "type": "integer",
                        "description": "Number of message chunks to return (default: 10)",
                        "default": 10
                    }
                },
                "required": ["query", "chat_id"]
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: Any) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
    """Handle iMessage search tool calls."""
    try:
        if name == "search_messages":
            if not isinstance(arguments, dict) or "query" not in arguments:
                raise ValueError("query parameter is required")
                
            n_results = arguments.get("n_results", 10)
            where = None
            if category := arguments.get("category"):
                where = {"category": category}
                
            # Query vector DB
            results = vector_db.query_collection(
                arguments["query"],
                n_results=n_results,
                where=where
            )
            
            # Format results
            formatted_results = format_query_results(results)
            
            # Generate response text
            response_parts = ["Message Search Results:\n"]
            for i, result in enumerate(formatted_results, 1):
                response_parts.append(
                    f"\n{i}. Message: {result.document}\n"
                    f"   From: {result.metadata.get('sender', 'Unknown')}\n"
                    f"   Chat: {result.metadata.get('chat_name', 'N/A')}\n"
                    f"   Date: {result.metadata.get('timestamp', 'N/A')}\n"
                    f"   Relevance: {1 - result.distance:.4f}\n"
                )
            
            return [TextContent(
                type="text",
                text="".join(response_parts)
            )]
            
        elif name == "search_chat":
            if not isinstance(arguments, dict) or "query" not in arguments or "chat_id" not in arguments:
                raise ValueError("Both query and chat_id parameters are required")
                
            n_results = arguments.get("n_results", 10)
            where = {"chat_id": arguments["chat_id"]}
            
            # Query vector DB
            results = vector_db.query_collection(
                arguments["query"],
                n_results=n_results,
                where=where
            )
            
            # Format results
            formatted_results = format_query_results(results)
            
            # Generate response text
            response_parts = [f"Search Results for Chat {arguments['chat_id']}:\n"]
            for i, result in enumerate(formatted_results, 1):
                response_parts.append(
                    f"\n{i}. Message: {result.document}\n"
                    f"   From: {result.metadata.get('sender', 'Unknown')}\n"
                    f"   Date: {result.metadata.get('timestamp', 'N/A')}\n"
                    f"   Relevance: {1 - result.distance:.4f}\n"
                )
            
            return [TextContent(
                type="text",
                text="".join(response_parts)
            )]
            
        else:
            raise ValueError(f"Unknown tool: {name}")
            
    except Exception as e:
        logger.error(f"Tool execution error: {str(e)}")
        return [TextContent(
            type="text",
            text=f"Error searching messages: {str(e)}"
        )]

async def main():
    from mcp.server.stdio import stdio_server
    
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )
