#!/usr/bin/env python3
"""
Trigger OpenMetadata Search Re-indexing.
"""

import requests
import base64
import time
import json

OM_API_URL = "http://localhost:8585/api/v1"

def get_admin_token():
    """Get Admin JWT token."""
    import base64
    password_b64 = base64.b64encode("admin".encode("utf-8")).decode("utf-8")
    response = requests.post(
        f"{OM_API_URL}/users/login",
        json={"email": "admin@open-metadata.org", "password": password_b64},
        headers={"Content-Type": "application/json"},
        timeout=10
    )
    if response.status_code == 200:
        return response.json().get("accessToken")
    print(f"Login failed: {response.text}")
    return None

def trigger_reindex(token):
    """Trigger full re-index."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    payload = {
        "entities": ["all"],
        "recreateIndex": True,
        "searchIndexType": "elasticsearch",
        "publisherType": "async"
    }
    
    response = requests.post(
        f"{OM_API_URL}/search/reindex",
        json=payload,
        headers=headers
    )
    
    if response.status_code in [200, 201]:
        print(f"Re-index job started: {response.json()}")
        return response.json().get("jobId")
    else:
        print(f"Failed to start re-index: {response.status_code} - {response.text}")
        return None

def main():
    print("Triggering OpenMetadata Re-index...")
    token = get_admin_token()
    if not token:
        return
        
    trigger_reindex(token)

if __name__ == "__main__":
    main()
