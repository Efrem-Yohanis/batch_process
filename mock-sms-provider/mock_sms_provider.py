"""
Mock SMS Provider API
Simulates a 3rd party SMS service for testing Layer 3
Run this separately from your main application
"""

import os
import uuid
import random
import asyncio
from datetime import datetime
from typing import Optional, List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

# =============== CONFIGURATION ===============
SUCCESS_RATE = float(os.getenv("MOCK_SUCCESS_RATE", "0.98"))  # 98% success rate
RESPONSE_DELAY_MS = int(os.getenv("MOCK_RESPONSE_DELAY_MS", "50"))  # 50ms delay
RANDOM_FAILURES = os.getenv("MOCK_RANDOM_FAILURES", "true").lower() == "true"
PORT = int(os.getenv("MOCK_API_PORT", "8003"))
HOST = os.getenv("MOCK_API_HOST", "0.0.0.0")

app = FastAPI(
    title="Mock SMS Provider API", 
    description="Standalone mock SMS provider for testing Layer 3",
    version="1.0.0"
)

# =============== DATA MODELS ===============
class SMSRequest(BaseModel):
    """Request from Layer 3 with from_id"""
    to: str
    text: str
    from_id: Optional[str] = None  # Sender ID from campaign
    message_id: str
    campaign_id: int

class SMSResponse(BaseModel):
    """Response to Layer 3"""
    message_id: str
    status: str
    provider_message_id: str
    timestamp: str
    from_id: Optional[str] = None
    error: Optional[str] = None

class DeliveryReport(BaseModel):
    """Async delivery report"""
    message_id: str
    provider_message_id: str
    status: str  # delivered, failed
    delivered_at: str
    from_id: Optional[str] = None
    error: Optional[str] = None

# =============== MOCK DATABASE ===============
# Store sent messages for delivery reports
sent_messages = {}
delivery_reports = []

# =============== API ENDPOINTS ===============
@app.post("/api/send", response_model=SMSResponse, status_code=200)
async def send_sms(request: SMSRequest):
    """
    Main endpoint for sending SMS
    Layer 3 will POST to this endpoint
    Includes from_id from campaign
    """
    # Simulate network delay
    await asyncio.sleep(RESPONSE_DELAY_MS / 1000)
    
    # Generate provider message ID
    provider_msg_id = f"prov_{uuid.uuid4().hex[:12]}"
    
    # Log the request with from_id
    print(f"\n📤 [{datetime.utcnow().isoformat()}] SMS Request Received:")
    print(f"   From ID: {request.from_id or 'default'}")
    print(f"   To: {request.to}")
    print(f"   Message ID: {request.message_id}")
    print(f"   Campaign ID: {request.campaign_id}")
    print(f"   Text: {request.text[:50]}..." if len(request.text) > 50 else f"   Text: {request.text}")
    
    # Simulate random failures if enabled
    if RANDOM_FAILURES:
        # 2% failure rate by default
        if random.random() > SUCCESS_RATE:
            # Random failure types
            failure_type = random.choice([
                "invalid_number",
                "network_error",
                "provider_reject",
                "rate_limit_exceeded",
                "invalid_sender_id"
            ])
            
            error_messages = {
                "invalid_number": "Invalid phone number format",
                "network_error": "Network timeout contacting carrier",
                "provider_reject": "Message rejected by provider",
                "rate_limit_exceeded": "Rate limit exceeded, try again later",
                "invalid_sender_id": f"Sender ID '{request.from_id}' not approved"
            }
            
            print(f"❌ Simulated failure: {failure_type}")
            
            return SMSResponse(
                message_id=request.message_id,
                status="failed",
                provider_message_id=provider_msg_id,
                timestamp=datetime.utcnow().isoformat(),
                from_id=request.from_id,
                error=error_messages[failure_type]
            )
    
    # Success - store message for potential delivery report
    sent_messages[provider_msg_id] = {
        "message_id": request.message_id,
        "campaign_id": request.campaign_id,
        "to": request.to,
        "text": request.text,
        "from_id": request.from_id,
        "sent_at": datetime.utcnow().isoformat()
    }
    
    print(f"✅ Message accepted. Provider ID: {provider_msg_id}")
    
    # Simulate async delivery report (80% of messages get delivered, 20% stay as "sent")
    if random.random() < 0.8:
        # Schedule delivery report after 2-10 seconds
        asyncio.create_task(send_delivery_report_after_delay(
            provider_msg_id, 
            from_id=request.from_id,
            delay=random.randint(2, 10)
        ))
    
    return SMSResponse(
        message_id=request.message_id,
        status="accepted",
        provider_message_id=provider_msg_id,
        timestamp=datetime.utcnow().isoformat(),
        from_id=request.from_id
    )

@app.post("/api/batch/send", status_code=200)
async def send_batch(requests: List[SMSRequest]):
    """
    Batch send endpoint - process multiple messages at once
    """
    print(f"\n📦 Batch request received with {len(requests)} messages")
    responses = []
    for i, req in enumerate(requests):
        # Process each request (simulate parallel processing)
        print(f"   Processing message {i+1}/{len(requests)}")
        resp = await send_sms(req)
        responses.append(resp)
    
    return {"responses": responses, "count": len(responses)}

@app.get("/api/status/{provider_message_id}")
async def get_status(provider_message_id: str):
    """
    Check status of a specific message
    """
    if provider_message_id in sent_messages:
        msg_data = sent_messages[provider_message_id]
        
        # Check if delivered
        for report in delivery_reports:
            if report["provider_message_id"] == provider_message_id:
                return {
                    "provider_message_id": provider_message_id,
                    "message_id": msg_data["message_id"],
                    "from_id": msg_data["from_id"],
                    "status": report["status"],
                    "delivered_at": report["delivered_at"]
                }
        
        return {
            "provider_message_id": provider_message_id,
            "message_id": msg_data["message_id"],
            "from_id": msg_data["from_id"],
            "status": "sent",
            "message": "Message accepted by provider, awaiting delivery receipt"
        }
    
    raise HTTPException(status_code=404, detail="Message not found")

@app.post("/webhook/delivery", status_code=200)
async def receive_delivery_report(report: DeliveryReport):
    """
    Webhook endpoint for delivery reports
    Layer 3 can expose this to receive async updates
    """
    # Store the delivery report
    delivery_reports.append(report.dict())
    
    # Log it
    print(f"📨 Delivery report received: {report.message_id} -> {report.status} (from: {report.from_id})")
    
    return {"status": "received"}

@app.get("/api/stats")
async def get_stats():
    """
    Get mock provider statistics
    """
    total_sent = len(sent_messages)
    delivered = len([r for r in delivery_reports if r["status"] == "delivered"])
    failed = len([r for r in delivery_reports if r["status"] == "failed"])
    
    # Group by from_id
    by_sender = {}
    for msg_id, msg in sent_messages.items():
        sender = msg.get("from_id", "default")
        if sender not in by_sender:
            by_sender[sender] = 0
        by_sender[sender] += 1
    
    return {
        "total_messages_sent": total_sent,
        "delivery_reports_received": len(delivery_reports),
        "delivered": delivered,
        "failed": failed,
        "delivery_rate": round((delivered / total_sent * 100), 2) if total_sent > 0 else 0,
        "messages_by_sender": by_sender,
        "uptime": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Mock SMS Provider",
        "success_rate": SUCCESS_RATE,
        "response_delay_ms": RESPONSE_DELAY_MS,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/")
async def root():
    """Root endpoint with API info"""
    return {
        "name": "Mock SMS Provider API",
        "version": "1.0.0",
        "description": "Standalone mock SMS provider for testing Layer 3",
        "endpoints": {
            "POST /api/send": "Send single SMS",
            "POST /api/batch/send": "Send batch SMS",
            "GET /api/status/{id}": "Get message status",
            "POST /webhook/delivery": "Receive delivery reports",
            "GET /api/stats": "Get provider statistics",
            "GET /health": "Health check"
        },
        "configuration": {
            "success_rate": SUCCESS_RATE,
            "response_delay_ms": RESPONSE_DELAY_MS,
            "random_failures": RANDOM_FAILURES
        }
    }

# =============== BACKGROUND TASKS ===============
async def send_delivery_report_after_delay(provider_msg_id: str, from_id: Optional[str] = None, delay: int = 5):
    """
    Simulate async delivery report after delay
    """
    await asyncio.sleep(delay)
    
    # Determine if delivered or failed (98% delivery rate)
    if random.random() < 0.98:
        status = "delivered"
    else:
        status = "failed"
    
    msg_data = sent_messages.get(provider_msg_id, {})
    
    report = DeliveryReport(
        message_id=msg_data.get("message_id", "unknown"),
        provider_message_id=provider_msg_id,
        status=status,
        delivered_at=datetime.utcnow().isoformat(),
        from_id=from_id,
        error=None if status == "delivered" else "Carrier delivery failed"
    )
    
    # Store the report
    delivery_reports.append(report.dict())
    
    print(f"🔔 [{datetime.utcnow().isoformat()}] Delivery report: {provider_msg_id} -> {status} (from: {from_id})")

# =============== RUN ===============
if __name__ == "__main__":
    print("=" * 60)
    print("📨 MOCK SMS PROVIDER API - STANDALONE MODE")
    print("=" * 60)
    print(f"Host: {HOST}")
    print(f"Port: {PORT}")
    print(f"Success Rate: {SUCCESS_RATE*100}%")
    print(f"Response Delay: {RESPONSE_DELAY_MS}ms")
    print(f"Random Failures: {RANDOM_FAILURES}")
    print("=" * 60)
    print("Endpoints:")
    print(f"  📤 POST   http://{HOST if HOST != '0.0.0.0' else 'localhost'}:{PORT}/api/send")
    print(f"  📦 POST   http://{HOST if HOST != '0.0.0.0' else 'localhost'}:{PORT}/api/batch/send")
    print(f"  🔍 GET    http://{HOST if HOST != '0.0.0.0' else 'localhost'}:{PORT}/api/status/{{id}}")
    print(f"  📨 POST   http://{HOST if HOST != '0.0.0.0' else 'localhost'}:{PORT}/webhook/delivery")
    print(f"  📊 GET    http://{HOST if HOST != '0.0.0.0' else 'localhost'}:{PORT}/api/stats")
    print(f"  ❤️  GET    http://{HOST if HOST != '0.0.0.0' else 'localhost'}:{PORT}/health")
    print(f"  🏠 GET    http://{HOST if HOST != '0.0.0.0' else 'localhost'}:{PORT}/")
    print("=" * 60)
    
    uvicorn.run(app, host=HOST, port=PORT)