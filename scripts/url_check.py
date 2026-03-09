"""
Debug script to check cinecloud R2 link extraction.
Saves full response to file for inspection.
"""
import httpx
import re
import base64
import sys
import os

TEST_URL = "https://www.cinefreak.net/generate.php?id=aHR0cHM6Ly9uZXc1LmNpbmVjbG91ZC5zaXRlL2YvNzYzMDE3MjJuZXdnb3J1"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

def decode_generate_url(url):
    if "generate.php?id=" in url:
        b64 = url.split("generate.php?id=")[1]
        return base64.b64decode(b64).decode("utf-8")
    return url

url = sys.argv[1] if len(sys.argv) > 1 else TEST_URL
decoded = decode_generate_url(url)

print(f"INPUT: {url}")
print(f"DECODED: {decoded}")

proxy = os.environ.get("SCRAPE_PROXY", None)

with httpx.Client(headers=HEADERS, proxy=proxy, timeout=30, follow_redirects=True) as client:
    r = client.get(decoded)
    print(f"Status: {r.status_code}")
    print(f"Content-Type: {r.headers.get('content-type')}")
    print(f"Length: {len(r.text)} chars")
    
    # Save full response
    with open("scripts/url_check_response.html", "w", encoding="utf-8") as f:
        f.write(r.text)
    print("Full response saved to: scripts/url_check_response.html")
