import asyncio, re, base64, json
import httpx
from playwright.async_api import async_playwright

import hashlib, secrets

CLIENT_ID = "gPxeZ05aWNVuF7HQImj9DuBWVURiY0tU"
REDIRECT_URI = "com.blacklane.chauffeur://login-chauffeur.blacklane.com/ios/com.blacklane.chauffeur/callback"
AUTH_BASE = "https://login-chauffeur.blacklane.com"
API_BASE = "https://chauffeur-app-api.blacklane.com"

# auth0Client that the iOS app sends
AUTH0_CLIENT = "eyJlbnYiOnsiaU9TIjoiMjYuMiIsInN3aWZ0IjoiNi54In0sIm5hbWUiOiJBdXRoMC5zd2lmdCIsInZlcnNpb24iOiIyLjE2LjAifQ"

code_verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
code_challenge = base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest()).rstrip(b"=").decode()
state = base64.urlsafe_b64encode(secrets.token_bytes(16)).rstrip(b"=").decode()

authorize_url = (
    f"{AUTH_BASE}/authorize?response_type=code&state={state}"
    f"&scope=openid%20profile%20email%20read:current_user%20offline_access"
    f"&code_challenge={code_challenge}&code_challenge_method=S256"
    f"&audience=https://blacklane.com"
    f"&redirect_uri={REDIRECT_URI}&client_id={CLIENT_ID}"
    f"&auth0Client={AUTH0_CLIENT}"
)

def decode_jwt_claims(token: str) -> dict:
    raw = token[7:].strip() if token.lower().startswith("bearer ") else token
    parts = raw.split(".")
    if len(parts) != 3:
        return {}
    try:
        payload = parts[1] + "==="
        return json.loads(base64.urlsafe_b64decode(payload.encode()))
    except Exception:
        return {}

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await (await browser.new_context()).new_page()

        code_holder = {}

        async def handle_request(request):
            if request.url.startswith("com.blacklane"):
                m = re.search(r"[?&]code=([^&]+)", request.url)
                if m:
                    code_holder["code"] = m.group(1)
                    print("Got code:", m.group(1)[:20], "...")

        page.on("request", handle_request)

        await page.goto(authorize_url)
        await page.fill('input[name="username"]', "TEST")
        await page.fill('input[name="password"]', "TEST")
        try:
            await page.click('button[type="submit"]', timeout=10000)
            await page.wait_for_timeout(4000)
        except:
            pass
        await browser.close()

    if not code_holder.get("code"):
        print("No code captured"); return

    r = httpx.post(f"{AUTH_BASE}/oauth/token", json={
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "code": code_holder["code"],
        "code_verifier": code_verifier,
        "redirect_uri": REDIRECT_URI,
    })
    data = r.json()
    if "access_token" not in data:
        print("Token exchange failed:", data); return

    access_token = data["access_token"]
    print("access_token:", access_token[:50], "...")

    # Decode and show JWT claims
    claims = decode_jwt_claims(access_token)
    print("\nJWT claims:")
    for k, v in claims.items():
        print(f"  {k}: {v}")

    # Test token against chauffeur API
    print("\nTesting against chauffeur-app-api...")
    r2 = httpx.get(
        f"{API_BASE}/offers",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "*/*",
            "Content-Type": "application/json",
            "X-Operating-System": "iOS",
            "User-Agent": "Chauffeur/18575 CFNetwork/3860.300.31 Darwin/25.2.0",
        },
        timeout=10,
    )
    print(f"API /offers status: {r2.status_code}")
    if r2.status_code != 200:
        print("Response:", r2.text[:300])

asyncio.run(main())
