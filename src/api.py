import json
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError

class ApiClient:
    def __init__(self, first_name: str, last_name: str):
        self.headers = {
                "Authorization": f"Bearer {first_name}.{last_name}"
        }

    def get(self, url: str, params: dict | None = None) -> dict:
        if params:
            url = f"{url}?{urlencode(params)}"

        request = Request(url, headers=self.headers)

        try:
            with urlopen(request) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as e:
            raise