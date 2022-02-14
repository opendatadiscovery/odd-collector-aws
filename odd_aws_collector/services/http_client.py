from aiohttp import ClientSession


class HttpClient:
    def __init__(self, token: str) -> None:
        self.token = token
        self.headers = {
            "content-type": "application/json",
            "Authorization": f"Bearer {token}",
        }

    async def post(self, url: str, data: any, session: ClientSession, **kwargs):
        resp = await session.post(url=url, data=data, headers=self.headers, **kwargs)
        return resp
