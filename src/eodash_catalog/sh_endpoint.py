import os
import time
from urllib.parse import urlparse

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

_token_cache: dict[str, dict] = {}


def get_SH_token(endpoint_config: dict) -> str:
    # Your client credentials
    client_id = os.getenv("SH_CLIENT_ID", "")
    client_secret = os.getenv("SH_CLIENT_SECRET", "")
    if env_id := endpoint_config.get("CustomSHEnvId"):
        client_id = os.getenv(f"SH_CLIENT_ID_{env_id}", "")
        client_secret = os.getenv(f"SH_CLIENT_SECRET_{env_id}", "")
    # 10 minutes before end of validity
    if client_id in _token_cache and (_token_cache[client_id]["expires_at"] - (60 * 10)) > (
        time.time()
    ):
        return _token_cache[client_id]["access_token"]
    # Create a session
    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)
    # Get token for the session
    endpoint_url_parts = urlparse(endpoint_config["EndPoint"])
    SH_TOKEN_URL = f"https://{endpoint_url_parts.netloc}/oauth/token"
    token = oauth.fetch_token(
        token_url=SH_TOKEN_URL,
        client_secret=client_secret,
    )
    access_token = token["access_token"]
    _token_cache[client_id] = {"access_token": access_token, "expires_at": token["expires_at"]}
    return access_token
