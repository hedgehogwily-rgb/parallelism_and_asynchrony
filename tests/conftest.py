import asyncio

import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestServer


PAGE_HTML = """
<html>
  <head><title>Local</title></head>
  <body>
    <h1>Local page</h1>
    <a href="/get">Get</a>
    <img src="/img.png" alt="Local image">
  </body>
</html>
"""


async def handle_get(request: web.Request) -> web.Response:
    return web.json_response({"url": str(request.url)})


async def handle_status(request: web.Request) -> web.Response:
    code = int(request.match_info["code"])
    return web.Response(status=code, text=f"status {code}")


async def handle_delay(request: web.Request) -> web.Response:
    seconds = float(request.match_info["seconds"])
    await asyncio.sleep(seconds)
    return web.json_response({"delay": seconds, "ok": True})


async def handle_page(request: web.Request) -> web.Response:
    return web.Response(text=PAGE_HTML, content_type="text/html")


def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/get", handle_get)
    app.router.add_get("/status/{code}", handle_status)
    app.router.add_get("/delay/{seconds}", handle_delay)
    app.router.add_get("/page", handle_page)
    return app


@pytest_asyncio.fixture
async def base_url():
    server = TestServer(build_app())
    await server.start_server()
    try:
        yield str(server.make_url("")).rstrip("/")
    finally:
        await server.close()
