"""
Debug script to check why cinecloud R2 link extraction fails.
Tests the full chain: generate.php → cinecloud → R2 download link
"""
import httpx

# Test URL from the user
TEST_URL = "https://www.cinefreak.net/generate.php?id=aHR0cHM6Ly9uZXc1LmNpbmVjbG91ZC5zaXRlL2YvNzYzMDE3MjJuZXdnb3J1"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}



print(httpx.get(TEST_URL, headers=HEADERS).text)