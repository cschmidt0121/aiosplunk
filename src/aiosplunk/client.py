import logging
from asyncio import run

from httpx import AsyncClient, AsyncHTTPTransport, Auth, BasicAuth, Response

from .exceptions import HTTPError, AuthenticationError

logger = logging.getLogger(__name__)


async def hook(response: Response) -> None:
    """
    If the response is an error, synchronously read the response body before raising.
    This is necessary because the response body is not immediately available when
    streaming.
    """
    if response.is_error:
        await response.aread()
        raise HTTPError(response)


class SplunkTokenAuth(Auth):
    def __init__(self, token):
        self.token = token

    def auth_flow(self, request):
        # Send the request, with a custom `X-Authentication` header.
        request.headers["Authorization"] = f"Bearer {self.token}"
        yield request


class SplunkClient:
    def __init__(
        self,
        host: str,
        port: int = 8089,
        verify: bool = False,
        token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: int = 10,
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
            transport=transport,
            auth=auth,
            base_url=base_url,
            event_hooks={"response": [hook]},
            timeout=timeout,
        )

    async def test_auth(self):
        """
        Simple function for verifying configured auth method
        """
        response = await self.request("GET", "/services/authentication/current-context")
        if response.status_code == 401:
            raise AuthenticationError("Splunk API authentication failed")

    async def request(self, *args, **kwargs):
        if "params" not in kwargs:
            kwargs["params"] = {"output_mode": "json"}
        elif "output_mode" not in kwargs["params"]:
            kwargs["params"]["output_mode"] = "json"

        response = await self.httpx_client.request(*args, **kwargs)
        if not 200 <= response.status_code <= 299:
            raise HTTPError(response=response)
        return response

    async def run_search(self, **kwargs):
        response = await self.request("POST", "/services/search/jobs", data=kwargs)
        response.raise_for_status()

        return response.json()["sid"]

    async def get_job(self, sid: str):
        url = f"/services/search/v2/jobs/{sid}"
        response = await self.request("GET", url)
        response.raise_for_status()
        return response.json()["entry"][0]["content"]

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

    async def eai_get(self, endpoint: str):
        # In my testing, concurency doesn't get us much here. So I just grab all
        # records.
        params = {"count": 0}
        response = await self.request("GET", endpoint, params=params)
        response.raise_for_status()

        j = response.json()
        if "entry" in j:
            return j["entry"]
        return []

    async def get_diag(self):
        """
        Stream a diag from the Splunk server.
        """
        async with self.httpx_client.stream("GET", "/services/streams/diag") as r:
            if not 200 <= r.status_code <= 299:
                raise HTTPError(r)
            async for chunk in r.aiter_bytes():
                yield chunk

    async def close(self):
        await self.httpx_client.aclose()

    def __exit__(self, _exc_type, _exc_value, _traceback):
        run(self.close())
