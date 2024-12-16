from httpx import Response


class AuthenticationError(Exception):
    pass


class HTTPError(Exception):
    def __init__(self, response: Response):
        self.status_code = response.status_code
        self.headers = response.headers
        self.text = response.text

        message = f"HTTP {self.status_code} - {self.text}"
        super().__init__(message)
