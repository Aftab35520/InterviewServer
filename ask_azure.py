import os
import ssl
import certifi
import httpx2
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI

load_dotenv()

AZURE_API_KEY    = os.getenv("AZURE_API_KEY")
AZURE_ENDPOINT   = os.getenv("AZURE_ENDPOINT")
AZURE_DEPLOYMENT = os.getenv("AZURE_DEPLOYMENT")
AZURE_API_VER    = os.getenv("AZURE_API_VERSION")

# Build an SSL context backed by certifi, bypassing truststore.
# truststore conflicts with gevent's monkey_patch_all() and causes
# RecursionError → openai.APIConnectionError: Connection error.
_ssl_ctx = ssl.create_default_context(cafile=certifi.where())
_http_client = httpx2.Client(
    transport=httpx2.HTTPTransport(verify=_ssl_ctx),
    timeout=60,
)

llm = AzureChatOpenAI(
    azure_endpoint=AZURE_ENDPOINT,
    api_key=AZURE_API_KEY,
    azure_deployment=AZURE_DEPLOYMENT,
    api_version=AZURE_API_VER,
    streaming=True,
    temperature=0,
    http_client=_http_client,
)