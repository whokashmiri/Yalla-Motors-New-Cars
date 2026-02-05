# src/main.py
from __future__ import annotations

import asyncio
import traceback
from dotenv import load_dotenv

from .core.browser import start_browser
from .core.scrape_new import scrape_forever
from .core.log import log

async def main():
    load_dotenv()
    browser = await start_browser()
    try:
       await scrape_forever(browser)

    except Exception as e:
        log("[FATAL]", repr(e))
        traceback.print_exc()
        raise
    finally:
        try:
            await browser.stop()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Interrupted")
