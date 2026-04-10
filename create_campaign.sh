#!/bin/bash

# ============================================
# Campaign Manager API - Full Campaign Creation Script
# ============================================

# Configuration
BASE_URL="http://localhost:8000/api"
AUTH_URL="http://localhost:8000/api/token"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}Campaign Manager API - Full Campaign Setup${NC}"
echo -e "${BLUE}============================================${NC}\n"

# ============================================
# Step 1: Get Authentication Token
# ============================================
echo -e "${YELLOW}Step 1: Getting authentication token...${NC}"

# Replace with your actual username and password
USERNAME="admin"
PASSWORD="admin123"

TOKEN_RESPONSE=$(curl -s -X POST "${AUTH_URL}/" \
  -H "Content-Type: application/json" \
  -d "{
    \"username\": \"${USERNAME}\",
    \"password\": \"${PASSWORD}\"
  }")

ACCESS_TOKEN=$(echo $TOKEN_RESPONSE | jq -r '.access')

if [ "$ACCESS_TOKEN" == "null" ] || [ -z "$ACCESS_TOKEN" ]; then
    echo -e "${RED}Failed to get token. Response: ${TOKEN_RESPONSE}${NC}"
    echo -e "${YELLOW}Note: You may need to create a user first or use different credentials${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Authentication successful!${NC}\n"

# ============================================
# Step 2: Create Campaign
# ============================================
echo -e "${YELLOW}Step 2: Creating campaign...${NC}"

CAMPAIGN_RESPONSE=$(curl -s -X POST "${BASE_URL}/campaigns/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Summer Sale Campaign",
    "sender_id": "SMSINFO",
    "channels": ["sms", "app_notification"]
  }')

CAMPAIGN_ID=$(echo $CAMPAIGN_RESPONSE | jq -r '.id')

if [ "$CAMPAIGN_ID" == "null" ] || [ -z "$CAMPAIGN_ID" ]; then
    echo -e "${RED}Failed to create campaign. Response: ${CAMPAIGN_RESPONSE}${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Campaign created with ID: ${CAMPAIGN_ID}${NC}"
echo -e "  Name: $(echo $CAMPAIGN_RESPONSE | jq -r '.name')"
echo -e "  Status: $(echo $CAMPAIGN_RESPONSE | jq -r '.status')"
echo -e "  Sender ID: $(echo $CAMPAIGN_RESPONSE | jq -r '.sender_id')"
echo -e "  Channels: $(echo $CAMPAIGN_RESPONSE | jq -r '.channels | join(", ")')"
echo ""

# ============================================
# Step 3: Add Audience
# ============================================
echo -e "${YELLOW}Step 3: Adding audience to campaign...${NC}"

AUDIENCE_RESPONSE=$(curl -s -X POST "${BASE_URL}/audiences/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"campaign\": ${CAMPAIGN_ID},
    \"recipients\": [
      {\"msisdn\": \"+251912345678\", \"lang\": \"en\"},
      {\"msisdn\": \"+251911223344\", \"lang\": \"am\"},
      {\"msisdn\": \"+251922334455\", \"lang\": \"ti\"},
      {\"msisdn\": \"+251933445566\", \"lang\": \"om\"},
      {\"msisdn\": \"+251944556677\", \"lang\": \"so\"},
      {\"msisdn\": \"+251955667788\", \"lang\": \"en\"},
      {\"msisdn\": \"+251966778899\", \"lang\": \"am\"},
      {\"msisdn\": \"+251977889900\", \"lang\": \"ti\"}
    ]
  }")

AUDIENCE_ID=$(echo $AUDIENCE_RESPONSE | jq -r '.id')

if [ "$AUDIENCE_ID" == "null" ] || [ -z "$AUDIENCE_ID" ]; then
    echo -e "${RED}Failed to add audience. Response: ${AUDIENCE_RESPONSE}${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Audience added with ID: ${AUDIENCE_ID}${NC}"
echo -e "  Total recipients: $(echo $AUDIENCE_RESPONSE | jq -r '.total_count')"
echo -e "  Valid recipients: $(echo $AUDIENCE_RESPONSE | jq -r '.valid_count')"
echo -e "  Invalid recipients: $(echo $AUDIENCE_RESPONSE | jq -r '.invalid_count')"
echo ""

# ============================================
# Step 4: Add Message Content
# ============================================
echo -e "${YELLOW}Step 4: Adding message content...${NC}"

MESSAGE_CONTENT_RESPONSE=$(curl -s -X POST "${BASE_URL}/message-contents/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"campaign\": ${CAMPAIGN_ID},
    \"content\": {
      \"en\": \"Hello {name}, welcome to our Summer Sale! Get 50% off on all items. Use code SUMMER50.\",
      \"am\": \"ሰላም {name}፣ እንኳን ደህና መጣህ! በሁሉም እቃዎች ላይ 50% ቅናሽ ያግኙ። ኮድ SUMMER50 ይጠቀሙ።\",
      \"ti\": \"ሰላም {name}፣ እንቋዕ ብደሓን መጻእካ! ኣብ ኩሎም ዕቃታት 50% ቅናሽ ርኸቡ። ኮድ SUMMER50 ተጠቀሙ።\",
      \"om\": \"Akkam {name}, baga nagaan dhufte! 50% diramma argadhu. Koodii SUMMER50 fayyadami.\",
      \"so\": \"Salaan {name}, soo dhawow! 50% ka dib u dhac ku hel. Koodhka SUMMER50 isticmaal.\"
    },
    \"default_language\": \"en\"
  }")

MESSAGE_ID=$(echo $MESSAGE_CONTENT_RESPONSE | jq -r '.id')

if [ "$MESSAGE_ID" == "null" ] || [ -z "$MESSAGE_ID" ]; then
    echo -e "${RED}Failed to add message content. Response: ${MESSAGE_CONTENT_RESPONSE}${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Message content added with ID: ${MESSAGE_ID}${NC}"
echo -e "  Default language: $(echo $MESSAGE_CONTENT_RESPONSE | jq -r '.default_language')"
echo -e "  Languages available: $(echo $MESSAGE_CONTENT_RESPONSE | jq -r '.languages_available | join(", ")')"
echo ""

# ============================================
# Step 5: Add Schedule
# ============================================
echo -e "${YELLOW}Step 5: Adding schedule...${NC}"

# Get tomorrow's date
TOMORROW=$(date -d "+1 day" +%Y-%m-%d)

SCHEDULE_RESPONSE=$(curl -s -X POST "${BASE_URL}/schedules/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"campaign\": ${CAMPAIGN_ID},
    \"schedule_type\": \"daily\",
    \"start_date\": \"${TOMORROW}\",
    \"end_date\": \"2024-12-31\",
    \"time_windows\": [
      {\"start\": \"09:00\", \"end\": \"12:00\"},
      {\"start\": \"14:00\", \"end\": \"17:00\"}
    ],
    \"timezone\": \"Africa/Addis_Ababa\",
    \"auto_reset\": true
  }")

SCHEDULE_ID=$(echo $SCHEDULE_RESPONSE | jq -r '.id')

if [ "$SCHEDULE_ID" == "null" ] || [ -z "$SCHEDULE_ID" ]; then
    echo -e "${RED}Failed to add schedule. Response: ${SCHEDULE_RESPONSE}${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Schedule added with ID: ${SCHEDULE_ID}${NC}"
echo -e "  Schedule type: $(echo $SCHEDULE_RESPONSE | jq -r '.schedule_type')"
echo -e "  Start date: $(echo $SCHEDULE_RESPONSE | jq -r '.start_date')"
echo -e "  Schedule summary: $(echo $SCHEDULE_RESPONSE | jq -r '.schedule_summary')"
echo ""

# ============================================
# Step 6: Start Campaign
# ============================================
echo -e "${YELLOW}Step 6: Starting campaign...${NC}"

START_RESPONSE=$(curl -s -X POST "${BASE_URL}/campaigns/${CAMPAIGN_ID}/start/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json")

START_STATUS=$(echo $START_RESPONSE | jq -r '.status')

if [ "$START_STATUS" == "active" ]; then
    echo -e "${GREEN}✓ Campaign started successfully!${NC}"
    echo -e "  Status: $(echo $START_RESPONSE | jq -r '.status')"
    echo -e "  Execution status: $(echo $START_RESPONSE | jq -r '.execution_status')"
    echo -e "  Message: $(echo $START_RESPONSE | jq -r '.message')"
    echo -e "  Total recipients: $(echo $START_RESPONSE | jq -r '.total_recipients')"
    echo -e "  Next run: $(echo $START_RESPONSE | jq -r '.next_run')"
else
    echo -e "${RED}Failed to start campaign. Response: ${START_RESPONSE}${NC}"
    exit 1
fi

echo ""

# ============================================
# Step 7: Get Campaign Summary
# ============================================
echo -e "${YELLOW}Step 7: Getting campaign summary...${NC}"

CAMPAIGN_DETAIL=$(curl -s -X GET "${BASE_URL}/campaigns/${CAMPAIGN_ID}/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}")

echo -e "${GREEN}✓ Campaign Details:${NC}"
echo -e "  Campaign ID: $(echo $CAMPAIGN_DETAIL | jq -r '.id')"
echo -e "  Name: $(echo $CAMPAIGN_DETAIL | jq -r '.name')"
echo -e "  Status: $(echo $CAMPAIGN_DETAIL | jq -r '.status')"
echo -e "  Execution Status: $(echo $CAMPAIGN_DETAIL | jq -r '.execution_status')"
echo -e "  Sender ID: $(echo $CAMPAIGN_DETAIL | jq -r '.sender_id')"
echo -e "  Channels: $(echo $CAMPAIGN_DETAIL | jq -r '.channels | join(", ")')"
echo -e "  Total Messages: $(echo $CAMPAIGN_DETAIL | jq -r '.progress.total_messages')"
echo -e "  Can Start: $(echo $CAMPAIGN_DETAIL | jq -r '.can_start')"
echo -e "  Can Pause: $(echo $CAMPAIGN_DETAIL | jq -r '.can_pause')"
echo -e "  Can Resume: $(echo $CAMPAIGN_DETAIL | jq -r '.can_resume')"
echo -e "  Can Stop: $(echo $CAMPAIGN_DETAIL | jq -r '.can_stop')"
echo ""

# ============================================
# Step 8: Get Campaign Progress
# ============================================
echo -e "${YELLOW}Step 8: Getting campaign progress...${NC}"

PROGRESS_RESPONSE=$(curl -s -X GET "${BASE_URL}/campaigns/${CAMPAIGN_ID}/progress/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}")

echo -e "${GREEN}✓ Campaign Progress:${NC}"
echo -e "  Campaign: $(echo $PROGRESS_RESPONSE | jq -r '.campaign_name')"
echo -e "  Status: $(echo $PROGRESS_RESPONSE | jq -r '.status')"
echo -e "  Execution Status: $(echo $PROGRESS_RESPONSE | jq -r '.execution_status')"
echo -e "  Progress: $(echo $PROGRESS_RESPONSE | jq -r '.progress.progress_percent')%"
echo -e "  Total Processed: $(echo $PROGRESS_RESPONSE | jq -r '.total_processed')"
echo -e "  Last Processed ID: $(echo $PROGRESS_RESPONSE | jq -r '.last_processed_id')"
echo ""

# ============================================
# Step 9: Get Audience Statistics
# ============================================
echo -e "${YELLOW}Step 9: Getting audience statistics...${NC}"

AUDIENCE_STATS=$(curl -s -X GET "${BASE_URL}/audiences/${AUDIENCE_ID}/statistics/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}")

echo -e "${GREEN}✓ Audience Statistics:${NC}"
echo -e "  Total: $(echo $AUDIENCE_STATS | jq -r '.total_count')"
echo -e "  Valid: $(echo $AUDIENCE_STATS | jq -r '.valid_count')"
echo -e "  Invalid: $(echo $AUDIENCE_STATS | jq -r '.invalid_count')"
echo -e "  Valid Percentage: $(echo $AUDIENCE_STATS | jq -r '.valid_percentage')%"
echo -e "  Language Distribution:"
for lang in en am ti om so; do
    count=$(echo $AUDIENCE_STATS | jq -r ".language_distribution.${lang} // 0")
    if [ "$count" != "0" ]; then
        echo -e "    - ${lang}: ${count}"
    fi
done
echo ""

# ============================================
# Step 10: Get Schedule Upcoming Windows
# ============================================
echo -e "${YELLOW}Step 10: Getting upcoming schedule windows...${NC}"

UPCOMING_WINDOWS=$(curl -s -X GET "${BASE_URL}/schedules/${SCHEDULE_ID}/upcoming_windows/?limit=3" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}")

echo -e "${GREEN}✓ Upcoming Schedule Windows:${NC}"
echo "$UPCOMING_WINDOWS" | jq -r '.[] | "  - Date: \(.date), Type: \(.type)"'
echo ""

# ============================================
# Final Summary
# ============================================
echo -e "${BLUE}============================================${NC}"
echo -e "${GREEN}✅ Campaign Setup Complete!${NC}"
echo -e "${BLUE}============================================${NC}"
echo -e "Campaign ID: ${GREEN}${CAMPAIGN_ID}${NC}"
echo -e "Audience ID: ${GREEN}${AUDIENCE_ID}${NC}"
echo -e "Schedule ID: ${GREEN}${SCHEDULE_ID}${NC}"
echo -e "Message Content ID: ${GREEN}${MESSAGE_ID}${NC}"
echo ""
echo -e "${YELLOW}Useful API Endpoints:${NC}"
echo -e "  - View Campaign: ${BASE_URL}/campaigns/${CAMPAIGN_ID}/"
echo -e "  - View Progress: ${BASE_URL}/campaigns/${CAMPAIGN_ID}/progress/"
echo -e "  - View Batches: ${BASE_URL}/campaigns/${CAMPAIGN_ID}/batches/"
echo -e "  - Pause Campaign: ${BASE_URL}/campaigns/${CAMPAIGN_ID}/pause/"
echo -e "  - Resume Campaign: ${BASE_URL}/campaigns/${CAMPAIGN_ID}/resume/"
echo -e "  - Stop Campaign: ${BASE_URL}/campaigns/${CAMPAIGN_ID}/stop/"
echo -e "  - Complete Campaign: ${BASE_URL}/campaigns/${CAMPAIGN_ID}/complete/"
echo ""

# ============================================
# Bonus: Create a test user if needed
# ============================================
echo -e "${YELLOW}Bonus: Create a test user (optional)${NC}"
read -p "Do you want to create a test user? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    USER_RESPONSE=$(curl -s -X POST "${BASE_URL}/users/" \
      -H "Content-Type: application/json" \
      -d '{
        "username": "testuser",
        "email": "test@example.com",
        "password": "TestPass123!",
        "confirm_password": "TestPass123!",
        "first_name": "Test",
        "last_name": "User"
      }')
    
    echo -e "${GREEN}✓ Test user created!${NC}"
    echo -e "  Username: testuser"
    echo -e "  Password: TestPass123!"
fi

echo -e "\n${GREEN}Done! 🚀${NC}"