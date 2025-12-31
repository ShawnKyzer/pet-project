#!/usr/bin/env python3
"""
Register Pet Activity Data Product in OpenMetadata.

This script:
1. Creates a PostgreSQL database service for the output database
2. Runs metadata ingestion to discover tables
3. Creates a Data Product for the Pet Activity Enjoyment data
"""

import requests
import time
import json

# OpenMetadata API configuration
OM_API_URL = "http://localhost:8585/api/v1"

# Global headers with auth token
HEADERS = {"Content-Type": "application/json"}

def get_auth_token():
    """Get JWT token for API calls."""
    import base64
    password_b64 = base64.b64encode("admin".encode("utf-8")).decode("utf-8")
    response = requests.post(
        f"{OM_API_URL}/users/login",
        json={"email": "admin@open-metadata.org", "password": password_b64},
        headers={"Content-Type": "application/json"},
        timeout=10
    )
    if response.status_code == 200:
        token = response.json().get("accessToken")
        HEADERS["Authorization"] = f"Bearer {token}"
        print(f"   Got auth token")
        return token
    else:
        print(f"   Login failed: {response.status_code} - {response.text}")
    return None

def create_database_service():
    """Create PostgreSQL database service for output database."""
    service_payload = {
        "name": "pet-activity-postgres",
        "displayName": "Pet Activity Database",
        "description": "PostgreSQL database containing aggregated pet activity and enjoyment data",
        "serviceType": "Postgres",
        "connection": {
            "config": {
                "type": "Postgres",
                "hostPort": "output-postgres:5432",
                "username": "outputdb",
                "authType": {
                    "password": "outputdb"
                },
                "database": "outputdb",
                "sslMode": "disable"
            }
        }
    }

    response = requests.post(
        f"{OM_API_URL}/services/databaseServices",
        json=service_payload,
        headers=HEADERS,
        timeout=30
    )

    if response.status_code == 201:
        print(f"   Created database service: pet-activity-postgres")
        return response.json()
    elif response.status_code == 409:
        print("   Database service already exists, fetching existing...")
        response = requests.get(
            f"{OM_API_URL}/services/databaseServices/name/pet-activity-postgres",
            headers=HEADERS,
            timeout=10
        )
        return response.json()
    else:
        print(f"   Failed to create service: {response.status_code} - {response.text}")
        return None

def create_ingestion_pipeline(service_id):
    """Create and trigger metadata ingestion pipeline."""
    pipeline_payload = {
        "name": "pet-activity-postgres_metadata",
        "displayName": "Pet Activity Metadata Ingestion",
        "pipelineType": "metadata",
        "service": {
            "id": service_id,
            "type": "databaseService"
        },
        "sourceConfig": {
            "config": {
                "type": "DatabaseMetadata",
                "markDeletedTables": True,
                "includeTables": True,
                "includeViews": True
            }
        },
        "airflowConfig": {
            "scheduleInterval": "0 0 * * *"
        }
    }

    response = requests.post(
        f"{OM_API_URL}/services/ingestionPipelines",
        json=pipeline_payload,
        headers=HEADERS,
        timeout=30
    )

    if response.status_code == 201:
        print(f"   Created ingestion pipeline")
        return response.json()
    elif response.status_code == 409:
        print("   Ingestion pipeline already exists")
        response = requests.get(
            f"{OM_API_URL}/services/ingestionPipelines/name/pet-activity-postgres.pet-activity-postgres_metadata",
            headers=HEADERS,
            timeout=10
        )
        if response.status_code == 200:
            return response.json()

    print(f"   Pipeline response: {response.status_code} - {response.text}")
    return None

def trigger_ingestion(pipeline_id):
    """Trigger the ingestion pipeline to run."""
    response = requests.post(
        f"{OM_API_URL}/services/ingestionPipelines/trigger/{pipeline_id}",
        headers=HEADERS,
        timeout=30
    )

    if response.status_code == 200:
        print("   Triggered metadata ingestion")
        return True
    else:
        print(f"   Failed to trigger ingestion: {response.status_code} - {response.text}")
        return False

def create_data_product():
    """Create a Data Product for the Pet Activity Enjoyment data."""

    # First, create a domain if it doesn't exist
    domain_payload = {
        "name": "pet-analytics",
        "displayName": "Pet Analytics",
        "description": "Domain for pet-related data products and analytics",
        "domainType": "Aggregate"
    }

    response = requests.post(
        f"{OM_API_URL}/domains",
        json=domain_payload,
        headers=HEADERS,
        timeout=30
    )

    if response.status_code in [201, 409]:
        print("Domain 'pet-analytics' ready")

    # Get domain
    domain_response = requests.get(
        f"{OM_API_URL}/domains/name/pet-analytics",
        headers=HEADERS,
        timeout=10
    )

    if domain_response.status_code != 200:
        print(f"Failed to get domain: {domain_response.text}")
        return None

    domain = domain_response.json()

    # Create data product
    data_product_payload = {
        "name": "pet-activity-enjoyment",
        "displayName": "Pet Activity Enjoyment Data Product",
        "description": """# Pet Activity Enjoyment Data Product

This data product provides aggregated metrics about pet activities and their enjoyment levels.

## Data Sources
- Pet sensor collar data (via Kafka)
- Pet registration database

## Key Metrics
- Average enjoyment score by pet, species, and city
- Activity counts and durations
- Daily aggregations

## Tables
- `pet_activity_enjoyment` - Per-pet activity aggregations
- `daily_activity_by_species` - Species-level daily aggregations
- `daily_activity_by_city` - City-level daily aggregations

## Use Cases
- Monitor pet happiness across activities
- Identify popular activities by species
- Track regional pet activity trends
""",
        "domain": domain["fullyQualifiedName"]
    }

    # Check if exists and delete to ensure clean creation with correct FQN
    print("   Checking for existing data product...")
    check_response = requests.get(
        f"{OM_API_URL}/dataProducts/name/pet-activity-enjoyment",
        headers=HEADERS
    )
    if check_response.status_code == 200:
        dp_id = check_response.json()['id']
        print(f"   Deleting existing data product {dp_id} to fix FQN...")
        requests.delete(
            f"{OM_API_URL}/dataProducts/{dp_id}",
            headers=HEADERS,
            params={"hardDelete": "true"}
        )
        time.sleep(2) # Wait for deletion

    # Create data product
    print("   Creating Data Product...")
    response = requests.post(
        f"{OM_API_URL}/dataProducts",
        json=data_product_payload,
        headers=HEADERS,
        timeout=30
    )

    if response.status_code == 201:
        print("Created Data Product: pet-activity-enjoyment")
        dp_id = response.json().get("id")
        
        # Link to domain via PATCH (reliable method)
        print("   Linking to domain...")
        patch_payload = [{
            "op": "add",
            "path": "/domain",
            "value": {
                "id": domain["id"],
                "type": "domain",
                "name": domain["name"],
                "description": domain["description"]
            }
        }]
        requests.patch(
            f"{OM_API_URL}/dataProducts/{dp_id}",
            json=patch_payload,
            headers={"Content-Type": "application/json-patch+json", **HEADERS},
            timeout=10
        )
        return response.json()
    elif response.status_code == 409:
        print("Data Product already exists")
        return True
    else:
        print(f"Failed to create data product: {response.status_code} - {response.text}")
        return None

def main():
    print("=" * 60)
    print("Registering Pet Activity Data in OpenMetadata")
    print("=" * 60)

    # Authenticate
    if not get_auth_token():
        return

    # Step 1: Create database service
    print("\n1. Creating database service...")
    service = create_database_service()
    if not service:
        print("Failed to create database service")
        return

    service_id = service.get("id")
    print(f"   Service ID: {service_id}")

    # Step 2: Create ingestion pipeline
    print("\n2. Creating ingestion pipeline...")
    pipeline = create_ingestion_pipeline(service_id)
    if pipeline:
        pipeline_id = pipeline.get("id")
        print(f"   Pipeline ID: {pipeline_id}")
        trigger_ingestion(pipeline_id)

    # Step 3: Create domain and data product
    print("\n3. Creating data product...")
    data_product = create_data_product()

    print("\n" + "=" * 60)
    print("Registration complete!")
    print("=" * 60)
    print("\nAccess OpenMetadata at: http://localhost:8585")
    print("Login with:")
    print("  Email:    admin@open-metadata.org")
    print("  Password: admin")
    print("\nYou can find:")
    print("  - Database Service: Services > Databases > pet-activity-postgres")
    print("  - Data Product: Data Products > pet-activity-enjoyment")
    print("  - Domain: Domains > pet-analytics")

if __name__ == "__main__":
    main()
