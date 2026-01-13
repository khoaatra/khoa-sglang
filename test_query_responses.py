#!/usr/bin/env python3
"""
Example script to query all responses from the OCI Oracle database.

This demonstrates how to use the new /v1/responses/query_all endpoint
that was added to query all items in the RESPONSES table.
"""

import requests
import json

def query_all_responses(base_url="http://localhost:30000", api_key=None):
    """
    Query all responses from the database.

    Args:
        base_url: Base URL of the SGLang router
        api_key: Optional API key for authentication

    Returns:
        List of response objects from the database
    """
    url = f"{base_url}/v1/responses/query_all"

    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        print(f"Querying responses from: {url}")
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            responses = response.json()
            print(f"Successfully retrieved {len(responses)} responses")
            return responses
        else:
            print(f"Error: HTTP {response.status_code}")
            print(f"Response: {response.text}")
            return []

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return []

def main():
    """Main function to demonstrate the query functionality."""

    # Configuration
    BASE_URL = "http://localhost:30000"
    API_KEY = None  # Set this if authentication is required

    print("=== OCI Oracle Responses Query Example ===\n")

    # Query all responses
    responses = query_all_responses(BASE_URL, API_KEY)

    if responses:
        print("\n=== Response Details ===")
        for i, response in enumerate(responses, 1):
            print(f"\nResponse {i}:")
            print(f"  ID: {response.get('id', 'N/A')}")
            print(f"  Model: {response.get('model', 'N/A')}")
            print(f"  Created: {response.get('created_at', 'N/A')}")
            print(f"  Conversation ID: {response.get('conversation_id', 'N/A')}")

            # Show input/output preview
            input_text = str(response.get('input', ''))[:100]
            output_text = str(response.get('output', ''))[:100]
            print(f"  Input (preview): {input_text}...")
            print(f"  Output (preview): {output_text}...")

    else:
        print("No responses found in the database.")
        print("\nPossible reasons:")
        print("1. No responses have been stored yet")
        print("2. Database connection not configured correctly")
        print("3. Server not running with OCI Oracle backend")

    print("\n=== Usage Instructions ===")
    print("1. Start the SGLang router with OCI Oracle backend:")
    print("   export TNS_ADMIN='/path/to/wallet'")
    print("   export DB_USER='ADMIN'")
    print("   export DB_PASSWORD='password'")
    print("   export DB_CONNECT_STRING='service_name'")
    print("   ./sgl-model-gateway --history-backend oci_oracle --worker-urls http://localhost:8000")
    print()
    print("2. Make some requests to store responses")
    print("3. Run this script to query all stored responses")

if __name__ == "__main__":
    main()
