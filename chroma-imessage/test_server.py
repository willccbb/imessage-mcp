import requests
import json
from typing import Dict, Any
import time

BASE_URL = "http://localhost:8000"

def print_response(response: Dict[str, Any], title: str) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(response, indent=2))

def test_server():
    # First, reset the database
    print_response(
        requests.post(f"{BASE_URL}/reset_database").json(),
        "Resetting Database"
    )
    
    # Create two collections: default and custom
    collections = ["default", "custom"]
    
    for collection_name in collections:
        print(f"\n\n=== Testing Collection: {collection_name} ===")
        
        # Sample documents with metadata
        documents = [
            # Programming Languages
            "Python is a versatile programming language known for its readability",
            "JavaScript powers the interactive web and runs in browsers",
            "Rust provides memory safety without garbage collection",
            "Java is popular for enterprise software development",
            
            # Machine Learning
            "Deep learning models can recognize patterns in images and text",
            "Neural networks are inspired by biological brain structures",
            "Transformer architecture revolutionized natural language processing",
            "Reinforcement learning helps AI agents learn through trial and error",
            
            # General Tech
            "Cloud computing enables scalable infrastructure solutions",
            "Docker containers simplify application deployment",
            "Git helps teams collaborate on source code",
            "APIs enable communication between different services",
            
            # Random Topics
            "The Eiffel Tower was completed in 1889",
            "Photosynthesis converts sunlight into chemical energy",
            "The Pacific Ocean is Earth's largest ocean",
        ]
        
        timestamp = int(time.time())
        metadatas = [
            # Programming Languages
            {"category": "programming", "subcategory": "python", "timestamp": timestamp, "index": 0},
            {"category": "programming", "subcategory": "javascript", "timestamp": timestamp, "index": 1},
            {"category": "programming", "subcategory": "rust", "timestamp": timestamp, "index": 2},
            {"category": "programming", "subcategory": "java", "timestamp": timestamp, "index": 3},
            
            # Machine Learning
            {"category": "ml", "subcategory": "deep_learning", "timestamp": timestamp, "index": 4},
            {"category": "ml", "subcategory": "neural_networks", "timestamp": timestamp, "index": 5},
            {"category": "ml", "subcategory": "nlp", "timestamp": timestamp, "index": 6},
            {"category": "ml", "subcategory": "reinforcement", "timestamp": timestamp, "index": 7},
            
            # General Tech
            {"category": "tech", "subcategory": "cloud", "timestamp": timestamp, "index": 8},
            {"category": "tech", "subcategory": "devops", "timestamp": timestamp, "index": 9},
            {"category": "tech", "subcategory": "tools", "timestamp": timestamp, "index": 10},
            {"category": "tech", "subcategory": "integration", "timestamp": timestamp, "index": 11},
            
            # Random Topics
            {"category": "history", "subcategory": "architecture", "timestamp": timestamp, "index": 12},
            {"category": "science", "subcategory": "biology", "timestamp": timestamp, "index": 13},
            {"category": "geography", "subcategory": "oceans", "timestamp": timestamp, "index": 14},
        ]
        
        # Test batch insertion
        insert_response = requests.post(
            f"{BASE_URL}/batch_insert",
            json={
                "documents": documents,
                "metadatas": metadatas,
                "collection_name": collection_name
            }
        )
        print_response(insert_response.json(), "Testing Batch Insertion")
        
        # Test collection info
        info_response = requests.get(f"{BASE_URL}/collection_info/{collection_name}")
        print_response(info_response.json(), "Testing Collection Info")
        
        # Test various semantic searches
        queries = [
            "Tell me about programming languages and software development",
            "What's related to artificial intelligence and neural networks?",
            "How do different technologies help with deployment and integration?",
            "What information is available about natural phenomena and geography?",
        ]
        
        for query in queries:
            query_response = requests.post(
                f"{BASE_URL}/query",
                json={
                    "query_texts": [query],
                    "n_results": 3,
                    "collection_name": collection_name,
                    "include": ["documents", "metadatas", "distances"]
                }
            )
            results = query_response.json()
            print(f"\n=== Query: {query} ===")
            for i, doc in enumerate(results['documents'][0]):
                print(f"\nMatch {i+1}:")
                print(f"Document: {doc}")
                print(f"Distance: {results['distances'][0][i]:.4f}")
                print(f"Metadata: {results['metadatas'][0][i]}")
        
        # Test metadata filtering
        print("\n=== Testing Metadata Filtering (ML Category) ===")
        query_response = requests.post(
            f"{BASE_URL}/query",
            json={
                "query_texts": ["What technologies are mentioned?"],
                "n_results": 4,
                "where": {"category": "ml"},
                "collection_name": collection_name,
                "include": ["documents", "metadatas", "distances"]
            }
        )
        results = query_response.json()
        for i, doc in enumerate(results['documents'][0]):
            print(f"\nMatch {i+1}:")
            print(f"Document: {doc}")
            print(f"Distance: {results['distances'][0][i]:.4f}")
            print(f"Metadata: {results['metadatas'][0][i]}")
    
    # Test collection reset
    print_response(
        requests.post(f"{BASE_URL}/reset_collection/custom").json(),
        "Resetting Custom Collection"
    )
    
    # Test collection deletion
    print_response(
        requests.delete(f"{BASE_URL}/delete_collection/custom").json(),
        "Deleting Custom Collection"
    )

if __name__ == "__main__":
    try:
        test_server()
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the server. Make sure it's running at http://localhost:8000") 