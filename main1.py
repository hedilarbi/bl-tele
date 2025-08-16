import requests
import time
import json
from datetime import datetime
import uuid

# Configuration
API_HOST = "https://chauffeur-app-api.blacklane.com"
AUTH_TOKEN = "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlU5a2N3VngtMlVOMVhhc1ZlUktqMiJ9.eyJibGFja2xhbmVfcm9sZXMiOlsiZHJpdmVyIiwicHJvdmlkZXIiLCJyZXZpZXdlciJdLCJjaGF1ZmZldXJfaWQiOiI1ZGMyYTVmNC0yNTM0LTExZjAtYThhYS0wMmEyNjUwMDcxNDkiLCJpc3MiOiJodHRwczovL2xvZ2luLWNoYXVmZmV1ci5ibGFja2xhbmUuY29tLyIsInN1YiI6ImF1dGgwfDVkYzJhNWY0LTI1MzQtMTFmMC1hOGFhLTAyYTI2NTAwNzE0OSIsImF1ZCI6WyJodHRwczovL2JsYWNrbGFuZS5jb20iLCJodHRwczovL2JsYWNrbGFuZS1jaGF1ZmZldXItcHJvZHVjdGlvbi5ldS5hdXRoMC5jb20vdXNlcmluZm8iXSwiaWF0IjoxNzUyNDE5NTAxLCJleHAiOjE3NTI1MDU5MDEsInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZW1haWwgb2ZmbGluZV9hY2Nlc3MiLCJhenAiOiJnUHhlWjA1YVdOVnVGN0hRSW1qOUR1QldWVVJpWTB0VSJ9.F0QCzoijOYLik2-1TSsK01yo7Zzu3S6OekTiJ-AsTv87PX9WJ5IyEH9Jbp4TsWVlMUMWboPg_ML08WjH35qVaadyzeCXcn9Ne_vbUeq9zzXyug8MyOaGR964F1JJKzwboILHSni4R1iCGqvht96hhx3yJkQC8oSFUG6kVyqQMDvDiosEytSWfYdkbqrkNn1MtFKU2wnLhifFCHYJXVhyAR_teKPCF9Ae4TjButMBaQxcrg3zHGi8pA0qUF3w_zK81p2c2xpU7pxuwaWJ8ySF_M7omOPIldOaahZQFYVYqHvoWLG36XAU-geQ9C7CTUCi90UJqetI3HzyiroOdLpf5w"
POLL_INTERVAL = 3  # Seconds

headers = {
    "Host": "chauffeur-app-api.blacklane.com",
    "Content-Type": "application/json",
    "User-Agent": "Chauffeur/11670 CFNetwork/1494.0.7 Darwin/23.4.0",
    "Connection": "keep-alive",
    "Accept": "*/*",
    "Accept-Language": "fr-CA,fr;q=0.9",
    "Authorization": AUTH_TOKEN,
}

accepted_offer_ids = set()  # Store accepted IDs to avoid duplicates

def get_offers():
    try:
        headers.update({
            "X-Request-ID": str(uuid.uuid4()),
            "X-Correlation-ID": str(uuid.uuid4()),
        })

        response = requests.get(f"{API_HOST}/offers", headers=headers, timeout=10)

        if response.status_code == 200:
            return response.json().get("results", [])
        else:
            print(f"[{datetime.now()}] Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"[{datetime.now()}] API Error: {str(e)}")
        return None

def accept_offer(offer):
    offer_id = offer.get("id")
    price = offer.get("price")

    if not offer_id or not price:
        print(f"[{datetime.now()}] Invalid offer data: {offer}")
        return

    data = {
        "id": offer_id,
        "price": float(price),
        "action": "accept"
    }

    local_headers = headers.copy()
    local_headers.update({
        "X-Request-ID": str(uuid.uuid4()),
        "X-Correlation-ID": str(uuid.uuid4()),
    })

    try:
        response = requests.post(f"{API_HOST}/offers", headers=local_headers, json=data, timeout=10)

        if response.status_code == 200:
            print(f"[{datetime.now()}] ✅ Offer accepted: {offer_id}")
            print(json.dumps(response.json(), indent=2))
            accepted_offer_ids.add(offer_id)
        else:
            print(f"[{datetime.now()}] ❌ Failed to accept offer {offer_id}: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[{datetime.now()}] Error while accepting offer: {str(e)}")

def main():
    print(f"[{datetime.now()}] Starting offer checker (Polling every {POLL_INTERVAL}s)...")

    while True:
        offers = get_offers()

        if offers is not None:
            new_offers = [offer for offer in offers if offer.get("id") not in accepted_offer_ids]
            print(f"[{datetime.now()}] Found {len(new_offers)} new offers")
            
            if new_offers:
                print(f"\n[{datetime.now()}] 🎉 New offers found: {len(new_offers)}")
                for offer in new_offers:
                    print(json.dumps(offer, indent=2))
                    #accept_offer(offer)
            else:
                print(f"[{datetime.now()}] No new offers", end="\r")
           
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
