from __future__ import annotations

import os
from typing import Any
import nodriver as nd

from .log import log

async def start_browser() -> Any:
    """Start nodriver browser with basic options."""
    headless = os.getenv("HEADLESS", "1").strip() not in ("0", "false", "False")
    log("[boot] starting browser headless=", headless)

    # nodriver exposes `start()` returning a Browser instance.
    # Different versions may accept different kwargs — keep it minimal.
    browser = await nd.start(headless=headless)
    return browser

async def new_tab(browser: Any, url: str) -> Any:
    """Open a new tab and navigate to url."""
    tab = await browser.get(url)
    return tab
