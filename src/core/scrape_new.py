# src/scrape_new.py

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any, Dict, List, Set
from urllib.parse import urljoin


from .log import log
from .selectors import RESULT_ANCHOR_SEL
from ..db.mongo import get_db, get_collection_name

BASE = "https://ksa.yallamotor.com"


# -------------------------
# Helpers
# -------------------------




def to_arabic_url(url: str) -> str:
    """
    Convert:
      https://ksa.yallamotor.com/new-cars/...
    to:
      https://ksa.yallamotor.com/ar/new-cars/...
    If already /ar, keep it.
    """
    if not url:
        return url
    if "://ksa.yallamotor.com/ar/" in url:
        return url
    return url.replace("://ksa.yallamotor.com/", "://ksa.yallamotor.com/ar/", 1)


def unwrap_js_value(x):
    """
    nodriver sometimes returns RemoteObject-like dicts:
    {"type":"string","value":"..."} instead of plain strings.
    """
    if isinstance(x, dict):
        if "value" in x:
            return x["value"]
        if "result" in x and isinstance(x["result"], dict) and "value" in x["result"]:
            return x["result"]["value"]
    return x


def abs_url(href) -> str:
    href = unwrap_js_value(href)
    if not href:
        return ""
    href = str(href).strip()
    return href if href.startswith("http") else urljoin(BASE, href)


async def navigate_in_tab(tab: Any, url: str) -> None:
    """
    Navigate inside an existing tab without creating new targets.
    This avoids nodriver StopIteration / CDP target issues on Windows.
    """
    if hasattr(tab, "get"):
        try:
            await tab.get(url)
            return
        except Exception:
            pass
    await tab.evaluate(f"window.location.href = {json.dumps(url)}")


async def wait_for_any_anchor(tab: Any, timeout: float = 45.0) -> bool:
    end = time.time() + timeout

    try:
        await tab.sleep(1)
    except Exception:
        await asyncio.sleep(1)

    try:
        await tab.evaluate("window.scrollTo(0, 800)")
    except Exception:
        pass

    while time.time() < end:
        try:
            n = await tab.evaluate(
                f"document.querySelectorAll({RESULT_ANCHOR_SEL!r}).length"
            )
            if int(n) > 0:
                return True
        except Exception:
            pass
        await asyncio.sleep(0.5)

    return False


async def extract_listing_hrefs(tab: Any) -> List[str]:
    js = f"""(() => {{
      const sel = {RESULT_ANCHOR_SEL!r};
      const links = Array.from(document.querySelectorAll(sel))
        .map(a => a.getAttribute('href') || '')
        .filter(Boolean);

      const seen = new Set();
      const out = [];
      for (const h of links) {{
        if (!seen.has(h)) {{
          seen.add(h);
          out.push(h);
        }}
      }}
      return out;
    }})()"""

    hrefs = await tab.evaluate(js)
    if not isinstance(hrefs, list):
        return []

    cleaned: List[str] = []
    for h in hrefs:
        h = unwrap_js_value(h)
        if h:
            cleaned.append(str(h))
    return cleaned


async def wait_for_detail_ready(tab: Any, timeout: float = 35.0) -> bool:
    """
    Wait for any of: H1 / breadcrumbs / SAR price / car_model images.
    (Arabic pages + Next hydration can be slow.)
    """
    end = time.time() + timeout

    # small scrolls help lazy content
    try:
        await tab.evaluate("window.scrollTo(0, 200)")
    except Exception:
        pass

    while time.time() < end:
        try:
            ok = await tab.evaluate(
                """(() => {
                  const h1 = document.querySelector('#overview-panel h1, h1');
                  const hasH1 = !!(h1 && (h1.innerText || '').trim().length > 3);

                  const bc = document.querySelector("nav[aria-label='Breadcrumb'], nav.breadcrumbs");
                  const hasBC = !!(bc && bc.querySelectorAll('ol > li').length >= 2);

                  // price: match SAR anywhere, not only at start, due to RTL markers/spaces
                  const sarRe = /SAR\\s*[\\d,.]+/;
                  const hasPrice = Array.from(document.querySelectorAll("p, span, div"))
                    .some(el => sarRe.test((el.innerText || '').replace(/\\s+/g,' ').trim()));

                  // images: car_model bucket
                  const hasImgs = Array.from(document.querySelectorAll("img"))
                    .some(img => {
                      const s = (img.currentSrc || img.src || img.getAttribute("src") || img.getAttribute("data-src") || "");
                      return s.includes("/resized/car_model/");
                    });

                  return hasH1 || hasBC || hasPrice || hasImgs;
                })()"""
            )
            ok = unwrap_js_value(ok)
            if bool(ok):
                return True
        except Exception:
            pass

        await asyncio.sleep(0.4)

        try:
            await tab.evaluate("window.scrollTo(0, 600)")
        except Exception:
            pass

    return False

async def extract_detail_basic(tab: Any) -> Dict:
    js = """(() => {
      const cleanText = (s) => (s || "").replace(/\\s+/g, " ").trim();

      // ----------------------------
      // Title (H1)
      // ----------------------------
      const h1 =
        cleanText(document.querySelector('#overview-panel h1')?.innerText) ||
        cleanText(document.querySelector('h1')?.innerText) ||
        null;

      // ----------------------------
      // Breadcrumbs
      // ----------------------------
      const bcNav =
        document.querySelector("nav[aria-label='Breadcrumb']") ||
        document.querySelector("nav.breadcrumbs") ||
        null;

      let breadcrumbs = [];
      let breadcrumbLinks = [];

      if (bcNav) {
        const lis = Array.from(bcNav.querySelectorAll("ol > li"));
        breadcrumbs = lis.map(li => cleanText(li.innerText)).filter(Boolean);

        breadcrumbLinks = lis.map(li => {
          const a = li.querySelector("a");
          const text = cleanText(li.innerText);
          const href = a ? (a.getAttribute("href") || "").trim() : "";
          return { text, href: href || null };
        }).filter(x => x.text);
      }

      const breadcrumbText = breadcrumbs.length ? breadcrumbs.join(" > ") : null;

      // ----------------------------
      // PRICE: match SAR anywhere
      // ----------------------------
      const sarRe = /SAR\\s*[\\d,.]+/;

      const priceNode = Array.from(document.querySelectorAll("p, span, div"))
        .find(el => sarRe.test(cleanText(el.innerText))) || null;

      const priceText = priceNode
        ? (cleanText(priceNode.innerText).match(sarRe)?.[0] || cleanText(priceNode.innerText))
        : null;

      const priceNumber = priceText
        ? (Number(priceText.replace(/[^\d]/g, "")) || null)
        : null;

      // ----------------------------
      // IMAGES: currentSrc/src/data-src/srcset
      // ----------------------------
      const imgs = Array.from(document.querySelectorAll("img"));

      const collect = [];
      for (const img of imgs) {
        const cur = (img.currentSrc || "").trim();
        const src = (img.getAttribute("src") || img.src || "").trim();
        const data = (img.getAttribute("data-src") || "").trim();
        const srcset = (img.getAttribute("srcset") || "").trim();

        if (cur) collect.push(cur);
        if (src) collect.push(src);
        if (data) collect.push(data);

        if (srcset) {
          const parts = srcset.split(",").map(s => s.trim()).filter(Boolean);
          if (parts.length) {
            const last = parts[parts.length - 1].split(" ")[0].trim();
            if (last) collect.push(last);
          }
        }
      }

      const allImgs = collect.filter(Boolean);

      const preferred = allImgs.filter(s =>
        s.includes("/resized/car_model/") &&
        (s.includes("webp_listing_main") || s.includes("webp_slide_show"))
      );

      const fallback = allImgs.filter(s => s.includes("/resized/car_model/"));
      const pick = preferred.length ? preferred : fallback;

      const seen = new Set();
      const images = [];
      for (const s of pick) {
        if (!seen.has(s)) { seen.add(s); images.push(s); }
      }

      const thumbsRaw = allImgs.filter(s => s.includes("webp_thumb"));
      const seenT = new Set();
      const imageThumbs = [];
      for (const s of thumbsRaw) {
        if (!seenT.has(s)) { seenT.add(s); imageThumbs.push(s); }
      }

      // ----------------------------
      // Vehicle highlights (specs grid under #vehicle-highlights)
      // Each item has a label (gray) + value (bold)
      // We'll return as:
      //   highlightsPairs: [{label, value}]
      //   highlights: { [label]: value }
      // ----------------------------
      const highlightsRoot =
        document.querySelector("#vehicle-highlights")?.closest("[data-slot='card-content']") ||
        document.querySelector("#vehicle-highlights")?.parentElement ||
        null;

      let highlightsPairs = [];
      if (highlightsRoot) {
        // first grid right after the h2
        const grid = highlightsRoot.querySelector("div[role='list'][aria-labelledby='vehicle-highlights']");
        const items = grid ? Array.from(grid.querySelectorAll("[role='listitem']")) : [];

        highlightsPairs = items.map(it => {
          const label = cleanText(it.querySelector(".text-sm")?.innerText);
          const value = cleanText(it.querySelector(".text-base")?.innerText);
          return { label: label || null, value: value || null };
        }).filter(x => x.label && x.value);
      }

      const highlights = {};
      for (const p of highlightsPairs) highlights[p.label] = p.value;

      // ----------------------------
      // Measurements (grid with aria-label="Vehicle measurements")
      // Return as:
      //   measurementsPairs: [{label, value}]
      //   measurements: { [label]: value }
      // ----------------------------
      const measList = document.querySelector("div[role='list'][aria-label='Vehicle measurements']");
      const measItems = measList ? Array.from(measList.querySelectorAll("[role='listitem']")) : [];
      const measurementsPairs = measItems.map(it => {
        const label = cleanText(it.querySelector(".text-sm")?.innerText);
        const value = cleanText(it.querySelector(".text-base")?.innerText);
        return { label: label || null, value: value || null };
      }).filter(x => x.label && x.value);

      const measurements = {};
      for (const p of measurementsPairs) measurements[p.label] = p.value;

      // ----------------------------
      // Description / overview prose card
      // We want the long Arabic paragraph (line-clamp-3)
      // We'll store:
      //   descriptionText (plain text)
      //   descriptionHtml (innerHTML) optional
      // ----------------------------
      const prose = document.querySelector("div[data-slot='card-content'].prose");
      const clamp = prose ? prose.querySelector(".line-clamp-3") : null;

      const descriptionText = clamp ? cleanText(clamp.innerText) : null;
      const descriptionHtml = clamp ? (clamp.innerHTML || null) : null;

      const canonical = document.querySelector("link[rel='canonical']")?.href || null;
      const ogUrl = document.querySelector("meta[property='og:url']")?.content || null;

      // ----------------------------
// FEATURES (bullets)
// <div data-slot="card-content" class="px-6">
//   <div class="grid ...">
//     <div class="flex ..."><span class="text-base capitalize" title="...">...</span></div>
//   </div>
//   <button aria-label="الميزات">عرض المزيد (57)</button>
// </div>
// We'll collect:
//  - features: string[]
//  - featuresCountHint: number | null   (from "عرض المزيد (57)")
// ----------------------------
const featureSpans = Array.from(document.querySelectorAll("span.text-base.capitalize[title]"));

const features = [];
const seenF = new Set();
for (const s of featureSpans) {
  const t = cleanText(s.getAttribute("title") || s.innerText || "");
  if (t && !seenF.has(t)) { seenF.add(t); features.push(t); }
}

// Count hint from the button text: "عرض المزيد (57)"
let featuresCountHint = null;
const featuresBtn =
  document.querySelector("button[aria-label='الميزات']") ||
  Array.from(document.querySelectorAll("button")).find(b => cleanText(b.innerText).includes("عرض المزيد") && cleanText(b.innerText).includes("(")) ||
  null;

if (featuresBtn) {
  const m = cleanText(featuresBtn.innerText).match(/\\((\\d+)\\)/);
  if (m) featuresCountHint = Number(m[1]) || null;
}


            return JSON.stringify({
        h1,
        breadcrumbs,
        breadcrumbLinks,
        breadcrumbText,

        priceText,
        priceNumber,

        images,
        imageThumbs,

        highlightsPairs,
        highlights,

        measurementsPairs,
        measurements,

        descriptionText,
        descriptionHtml,

        // ✅ ADD THESE
        features,
        featuresCountHint,

        canonical,
        ogUrl,
        html: document.documentElement ? document.documentElement.outerHTML : null
      });

    })()"""

    raw = await tab.evaluate(js)
    raw = unwrap_js_value(raw)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}





import inspect

async def save_doc(doc: Dict) -> None:
    db = get_db()
    col = db[get_collection_name()]

    url = str(doc.get("url") or "").strip()
    if not url:
        return

    created_at = doc.get("createdAt")
    if "createdAt" in doc:
        doc = dict(doc)
        doc.pop("createdAt", None)

    update = {"$set": doc, "$setOnInsert": {"createdAt": created_at}}

    res = col.update_one({"_id": url}, update, upsert=True)
    if inspect.isawaitable(res):
        await res



# Add these helpers + call expand_features_if_needed() before extract_detail_basic()

async def safe_click(tab: Any, js_selector: str) -> bool:
    """
    Click via JS to avoid nodriver click flakiness.
    Returns True if element existed + clicked.
    """
    js = f"""(() => {{
      const el = document.querySelector({json.dumps(js_selector)});
      if (!el) return false;
      el.scrollIntoView({{block:'center', inline:'center'}});
      el.click();
      return true;
    }})()"""
    try:
        ok = await tab.evaluate(js)
        return bool(unwrap_js_value(ok))
    except Exception:
        return False


async def expand_features_if_needed(tab: Any, timeout: float = 12.0) -> None:
    """
    Clicks the 'الميزات' (features) expand button if present, then waits until
    the features list grows / button disappears / or timeout.
    Safe no-op if section isn't present.
    """
    # count current feature chips
    async def _count() -> int:
        try:
            n = await tab.evaluate(
                """(() => document.querySelectorAll("span.text-base.capitalize[title]").length)()"""
            )
            return int(unwrap_js_value(n) or 0)
        except Exception:
            return 0

    before = await _count()

    # try to locate button robustly (aria-label is best)
    clicked = await safe_click(tab, "button[aria-label='الميزات']")
    if not clicked:
        # fallback: any button containing "عرض المزيد" and "(number)"
        clicked = await tab.evaluate(
            """(() => {
              const clean = (s) => (s||"").replace(/\\s+/g," ").trim();
              const btns = Array.from(document.querySelectorAll("button"));
              const b = btns.find(x => {
                const t = clean(x.innerText);
                return t.includes("عرض المزيد") && /\\(\\d+\\)/.test(t);
              });
              if (!b) return false;
              b.scrollIntoView({block:'center', inline:'center'});
              b.click();
              return true;
            })()"""
        )
        clicked = bool(unwrap_js_value(clicked))

    if not clicked:
        return  # no features expand on this page

    # wait for the list to grow (or at least change)
    end = time.time() + timeout
    while time.time() < end:
        await asyncio.sleep(0.35)
        after = await _count()
        if after > before:
            return
        # if button vanished (expanded), also accept
        try:
            gone = await tab.evaluate(
                """(() => !document.querySelector("button[aria-label='الميزات']"))()"""
            )
            if bool(unwrap_js_value(gone)):
                return
        except Exception:
            pass


# ---- Update your scrape_detail_in_detail_tab to call it ----
async def scrape_detail_in_detail_tab(detail_tab: Any, detail_url: str) -> Dict:
    await navigate_in_tab(detail_tab, detail_url)
    await wait_for_detail_ready(detail_tab, timeout=35.0)

    try:
        await detail_tab.sleep(1.2)
    except Exception:
        await asyncio.sleep(1.2)

    # force render sections (gallery/price/features)
    for y in (0, 400, 900, 1400, 2000):
        try:
            await detail_tab.evaluate(f"window.scrollTo(0, {y})")
        except Exception:
            pass
        await asyncio.sleep(0.25)

    # ✅ expand features before extraction
    await expand_features_if_needed(detail_tab)

    return await extract_detail_basic(detail_tab)




# -------------------------
# Core flow
# -------------------------
async def scrape_listing_page(listing_tab: Any) -> List[str]:
    ok = await wait_for_any_anchor(listing_tab)
    if not ok:
        raise RuntimeError("No listing anchors found (layout changed?)")

    hrefs = await extract_listing_hrefs(listing_tab)
    urls = [abs_url(h) for h in hrefs]
    return [u for u in urls if u.startswith("http")]




SLEEP_HOURS = int(os.getenv("SLEEP_HOURS", "48"))  # default 48
MAX_PAGES = int(os.getenv("MAX_PAGES", "50"))      # safety cap per run

async def scrape_forever(browser):
    run_no = 0

    while True:
        run_no += 1
        log(f"[run] start #{run_no}")

        seen: Set[str] = set()

        listing_tab = await browser.get("about:blank")
        detail_tab = await browser.get("about:blank")

        try:
            for page in range(1, MAX_PAGES + 1):
                url = f"https://ksa.yallamotor.com/ar/new-cars/search?page={page}"
                log("[list] open", url)

                await navigate_in_tab(listing_tab, url)
                urls = await scrape_listing_page(listing_tab)

                if not urls:
                    log("[list] no ads found on page", page, "=> end run")
                    break

                log("[list] found", len(urls), "detail urls on page", page)

                for detail_url in urls:
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)

                    log("[detail] open", detail_url)

                    try:
                        ar_url = to_arabic_url(detail_url)
                        detail = await scrape_detail_in_detail_tab(detail_tab, ar_url)

                        now = __import__("datetime").datetime.utcnow()

                        doc = {
                            "_id": detail_url,
                            "url": detail_url,
                            # "arUrl": ar_url,
                            "source": "yallamotor",
                            "type": "NEW_CAR",
                            "status": "OK",

                            "title": detail.get("h1"),
                            "breadcrumbs": detail.get("breadcrumbs") or [],
                            # "breadcrumbLinks": detail.get("breadcrumbLinks") or [],
                            # "breadcrumbText": detail.get("breadcrumbText"),

                            "priceText": detail.get("priceText"),
                            "priceNumber": detail.get("priceNumber"),

                            "images": detail.get("images") or [],
                            # "imageThumbs": detail.get("imageThumbs") or [],

                            # "highlightsPairs": detail.get("highlightsPairs") or [],
                            "highlights": detail.get("highlights") or {},

                            # "measurementsPairs": detail.get("measurementsPairs") or [],
                            "measurements": detail.get("measurements") or {},

                            "descriptionText": detail.get("descriptionText"),
                            "descriptionHtml": detail.get("descriptionHtml"),

                            "features": detail.get("features") or [],
                            # "featuresCountHint": detail.get("featuresCountHint"),

                            # "canonical": detail.get("canonical"),
                            # "ogUrl": detail.get("ogUrl"),
                            # "html": detail.get("html"),

                            "updatedAt": now,
                            "scrapedAt": now,
                            "createdAt": now,
                        }

                        await save_doc(doc)
                        log("[detail] saved", detail_url, "imgs=", len(doc["images"]), "price=", doc["priceText"])
                        await asyncio.sleep(0.25)

                    except Exception as e:
                        log("[detail] FAILED", detail_url, repr(e))
                        now = __import__("datetime").datetime.utcnow()
                        fail_doc = {
                            "_id": detail_url,
                            "url": detail_url,
                            "arUrl": to_arabic_url(detail_url),
                            "source": "yallamotor",
                            "type": "new_car",
                            "status": "FAILED",
                            "error": repr(e),
                            "updatedAt": now,
                            "scrapedAt": now,
                            "createdAt": now,
                        }
                        try:
                            await save_doc(fail_doc)
                        except Exception:
                            pass

            log(f"[run] finished #{run_no}. sleeping {SLEEP_HOURS} hours...")

        finally:
            try:
                await detail_tab.close()
            except Exception:
                pass
            try:
                await listing_tab.close()
            except Exception:
                pass

        await asyncio.sleep(SLEEP_HOURS * 60 * 60)







async def scrape_new_cars(browser: Any) -> None:
    start_url = os.getenv("START_URL", "https://ksa.yallamotor.com/ar/new-cars/search?page=1").strip()
    max_pages = int(os.getenv("MAX_PAGES", "1") or "1")

    seen: Set[str] = set()

    log("[list] open", start_url)
    listing_tab = await browser.get(start_url)
    detail_tab = await browser.get("about:blank")

    try:
        for page in range(1, max_pages + 1):
            url = start_url
            if "page=" in url:
                import re
                url = re.sub(r"page=\d+", f"page={page}", url)
            else:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}page={page}"

            log(f"[list] navigating to page {page}: {url}")
            await navigate_in_tab(listing_tab, url)

            urls = await scrape_listing_page(listing_tab)
            log("[list] found", len(urls), "detail urls")

            new_urls = []
            for u in urls:
                if u not in seen:
                    seen.add(u)
                    new_urls.append(u)

            for detail_url in new_urls:
                log("[detail] open", detail_url)
                try:
                    ar_url = to_arabic_url(detail_url)
                    detail = await scrape_detail_in_detail_tab(detail_tab, ar_url)

                    now = __import__("datetime").datetime.utcnow()

                    doc = {
                        "_id": detail_url,
                        "url": detail_url,
                        "arUrl": ar_url,
                        "source": "yallamotor",
                        "type": "new_car",
                        "status": "OK",

                        "title": detail.get("h1"),
                        "breadcrumbs": detail.get("breadcrumbs") or [],
                        "breadcrumbLinks": detail.get("breadcrumbLinks") or [],
                        "breadcrumbText": detail.get("breadcrumbText"),

                        "priceText": detail.get("priceText"),
                        "priceNumber": detail.get("priceNumber"),

                        "images": detail.get("images") or [],
                        "imageThumbs": detail.get("imageThumbs") or [],

                        "highlightsPairs": detail.get("highlightsPairs") or [],
"highlights": detail.get("highlights") or {},

"measurementsPairs": detail.get("measurementsPairs") or [],
"measurements": detail.get("measurements") or {},

"descriptionText": detail.get("descriptionText"),
"descriptionHtml": detail.get("descriptionHtml"),

"features": detail.get("features") or [],
"featuresCountHint": detail.get("featuresCountHint"),



                        "canonical": detail.get("canonical"),
                        "ogUrl": detail.get("ogUrl"),
                        "html": detail.get("html"),

                        "updatedAt": now,
                        "scrapedAt": now,
                        "createdAt": now,
                    }

                    await save_doc(doc)
                    log("[detail] saved", detail_url, "imgs=", len(doc["images"]), "price=", doc["priceText"])
                    await asyncio.sleep(0.25)

                except Exception as e:
                    log("[detail] FAILED", detail_url, repr(e))
                    now = __import__("datetime").datetime.utcnow()
                    fail_doc = {
                        "_id": detail_url,
                        "url": detail_url,
                        "arUrl": to_arabic_url(detail_url),
                        "source": "yallamotor",
                        "type": "new_car",
                        "status": "FAILED",
                        "error": repr(e),
                        "updatedAt": now,
                        "scrapedAt": now,
                        "createdAt": now,
                    }
                    try:
                        await save_doc(fail_doc)
                    except Exception:
                        pass

        log("[done] total unique detail urls:", len(seen))

    finally:
        try:
            await detail_tab.close()
        except Exception:
            pass
        try:
            await listing_tab.close()
        except Exception:
            pass
