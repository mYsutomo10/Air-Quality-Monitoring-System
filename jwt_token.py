import jwt
import time
import json

# Load kunci JSON dari Service Account
with open('service-account.json') as f:
    service_account_info = json.load(f)

private_key = service_account_info["private_key"]

# Buat JWT Token
iat = time.time()
exp = iat + 3600  # Berlaku 1 jam
payload = {
    "iss": service_account_info["client_email"],
    "scope": "https://www.googleapis.com/auth/pubsub",
    "aud": "https://oauth2.googleapis.com/token",
    "iat": iat,
    "exp": exp
}

token = jwt.encode(payload, private_key, algorithm="RS256")
print(token)