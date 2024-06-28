import os

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

SH_TOKEN_URL = "https://services.sentinel-hub.com/oauth/token"


def get_SH_token() -> str:
    # Your client credentials
    client_id = os.getenv("SH_CLIENT_ID")
    client_secret = os.getenv("SH_CLIENT_SECRET")
    # Create a session
    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)
    # Get token for the session
    token = oauth.fetch_token(
        token_url=SH_TOKEN_URL,
        client_secret=client_secret,
    )

    return token["access_token"]
