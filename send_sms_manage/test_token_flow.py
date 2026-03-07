import requests
import json

# Base URLs
DJANGO_URL = "http://127.0.0.1:8000"
LAYER1_URL = "http://127.0.0.1:8001"

def test_auth_flow():
    """Test the complete auth flow"""
    
    print("=" * 50)
    print("🔐 Testing Authentication Flow")
    print("=" * 50)
    
    # Step 1: Get token from Django
    print("\n1. Getting token from Django...")
    token_response = requests.post(
        f"{DJANGO_URL}/api/token/",
        json={"username": "test", "password": "test"}
    )
    
    if token_response.status_code != 200:
        print(f"❌ Failed to get token: {token_response.status_code}")
        return
    
    token_data = token_response.json()
    access_token = token_data['access']
    print(f"✅ Got access token: {access_token[:20]}...")
    
    # Step 2: Test Django API with token
    print("\n2. Testing Django API with token...")
    campaigns_response = requests.get(
        f"{DJANGO_URL}/api/campaigns/",
        headers={"Authorization": f"Bearer {access_token}"}
    )
    
    if campaigns_response.status_code == 200:
        campaigns = campaigns_response.json()
        print(f"✅ Success! Found {campaigns.get('count', 0)} campaigns")
    else:
        print(f"❌ Failed: {campaigns_response.status_code}")
    
    # Step 3: Test Layer 1 endpoint
    print("\n3. Testing Layer 1 START endpoint...")
    start_response = requests.post(
        f"{LAYER1_URL}/campaign/start",
        json={"campaign_id": 1, "reason": "Test from script"}
    )
    
    print(f"Status: {start_response.status_code}")
    print(f"Response: {json.dumps(start_response.json(), indent=2)}")
    
    # Step 4: Check Layer 1 health
    print("\n4. Checking Layer 1 health...")
    health_response = requests.get(f"{LAYER1_URL}/health")
    print(f"Health: {json.dumps(health_response.json(), indent=2)}")

if __name__ == "__main__":
    test_auth_flow()