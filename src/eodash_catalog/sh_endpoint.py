import os

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

SH_TOKEN_URL = "https://services.sentinel-hub.com/oauth/token"
_token_cache: dict[str, str] = {}


def get_SH_token(endpoint_config: dict) -> str:
    # Your client credentials
    client_id = os.getenv("SH_CLIENT_ID", "")
    client_secret = os.getenv("SH_CLIENT_SECRET", "")
    if env_id := endpoint_config.get("CustomSHEnvId"):
        client_id = os.getenv(f"SH_CLIENT_ID_{env_id}", "")
        client_secret = os.getenv(f"SH_CLIENT_SECRET_{env_id}", "")
    if client_id in _token_cache:
        return _token_cache[client_id]
    # Create a session
    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)
    # Get token for the session
    token = oauth.fetch_token(
        token_url=SH_TOKEN_URL,
        client_secret=client_secret,
    )
    access_token = token["access_token"]
    _token_cache[client_id] = access_token

    return access_token
