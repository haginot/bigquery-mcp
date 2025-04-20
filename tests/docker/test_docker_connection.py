import requests
import json

def test_mcp_server():
    """Test the MCP server running in Docker."""
    url = "http://localhost:8000/mcp"
    
    try:
        print("Testing GET endpoint...")
        response = requests.get(url)
        print(f"GET Status code: {response.status_code}")
        print(f"GET Response: {response.text}")
    except Exception as e:
        print(f"GET Error: {e}")
    
    # Test execute_query tool
    payload = {
        "jsonrpc": "2.0",
        "method": "call_tool",
        "params": {
            "tool": "execute_query",
            "params": {
                "sql": "SELECT 1 as test",
                "dryRun": True
            }
        },
        "id": 1
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        print("\nTesting execute_query tool...")
        response = requests.post(url, json=payload, headers=headers)
        print(f"POST Status code: {response.status_code}")
        print(f"POST Response text: {response.text}")
        
        if response.status_code == 200:
            print(f"POST Response JSON: {json.dumps(response.json(), indent=2)}")
        
        # Test list_datasets tool only if first call succeeded
        if response.status_code == 200:
            payload = {
                "jsonrpc": "2.0",
                "method": "call_tool",
                "params": {
                    "tool": "list_datasets",
                    "params": {}
                },
                "id": 2
            }
            
            response = requests.post(url, json=payload, headers=headers)
            print(f"\nList datasets status code: {response.status_code}")
            print(f"List datasets response text: {response.text}")
            
            if response.status_code == 200:
                print(f"List datasets response JSON: {json.dumps(response.json(), indent=2)}")
        
        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    print("Testing MCP BigQuery server in Docker...")
    success = test_mcp_server()
    print(f"\nTest {'succeeded' if success else 'failed'}")
