import logging
from asyncio import run

from httpx import AsyncClient, AsyncHTTPTransport, Auth, BasicAuth

logger = logging.getLogger(__name__)


class SplunkTokenAuth(Auth):
    def __init__(self, token):
        self.token = token

    def auth_flow(self, request):
        # Send the request, with a custom `X-Authentication` header.
        request.headers["Authorization"] = f"Bearer {self.token}"
        yield request


class Client:
    def __init__(
        self,
        host: str,
        port: int = 8089,
        verify: bool = False,
        token: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ):
        if username is not None and password is not None:
            auth = BasicAuth(username=username, password=password)
        elif token is not None:
            auth = SplunkTokenAuth(token)
        else:
            auth = None

        base_url = f"https://{host}:{port}"
        transport = AsyncHTTPTransport(retries=5, verify=verify)
        self.httpx_client = AsyncClient(
            transport=transport, auth=auth, base_url=base_url
        )

    async def test_auth(self):
        """
        Simple function for verifying configured auth method
        """
        response = await self.request("GET", "/services/authentication/current-context")
        return response.status_code != 401

    async def request(self, *args, **kwargs):
        if "data" not in kwargs:
            kwargs["data"] = {"output_mode": "json"}
        elif "output_mode" not in kwargs:
            kwargs["data"]["output_mode"] = "json"

        return await self.httpx_client.request(*args, **kwargs)

    async def run_search(self, **kwargs):
        response = await self.request("POST", "/services/search/jobs", data=kwargs)
        response.raise_for_status()

        return response.json()["sid"]

    async def get_job(self, sid: str):
        url = f"/services/search/v2/jobs/{sid}"
        response = await self.request("GET", url)
        response.raise_for_status()
        return response.json()["entry"][0]["content"]

    async def get_summary(self, sid: str) -> dict:
        url = f"/services/search/jobs/{sid}"
        params = {"output_mode": "json"}

        response = await self.request("GET", url, params=params)
        response.raise_for_status()
        return response.json()

    async def get_results(
        self,
        sid: str,
        count: int,
        offset: int,
        output_mode: str,
        fields: list | None = None,
    ) -> str:
        url = f"/services/search/v2/jobs/{sid}/results"

        if not fields:
            fields = ["*"]

        params = {
            "count": count,
            "offset": offset,
            "output_mode": output_mode,
            "f": fields,
        }
        response = await self.request("GET", url, params=params)
        response.raise_for_status()

        # Parsing (if any) is done later
        return response.text

    async def close(self):
        await self.httpx_client.aclose()

    def __exit__(self, _exc_type, _exc_value, _traceback):
        run(self.close())