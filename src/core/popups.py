# src/core/popups.py
"""
Popup / overlay handling for yallamotor.

Two known blockers:
  1. Onboarding modal  "دعنا نساعدك في إيجاد سيارتك المثالية"  with buttons
     "لنبدأ" / "ربما لاحقاً" and an X (aria-label="Close").
  2. Convertful full-screen welcome widget (classes "conv-*", images from
     app.convertful.com).

Strategy (layered, every step is best-effort and never raises):
  - Block convertful network requests for the tab (widget never loads).
  - Inject a guard script on every new document: CSS that hides convertful
    nodes + a MutationObserver that dismisses the onboarding modal as soon
    as it appears (it often shows up a few seconds after load).
  - dismiss_popups(tab) runs the same guard immediately; call it after
    navigation and inside wait loops.
"""

from __future__ import annotations

from typing import Any

from .log import log

BLOCKED_URL_PATTERNS = [
    "*convertful.com*",
    "*app.convertful.com*",
]

POPUP_GUARD_JS = r"""
(() => {
  const clean = (s) => (s || "").replace(/\s+/g, " ").trim();

  // never match <html>/<body> (convertful may add conv-* classes there)
  const CONV_SEL = [
    "[class^='conv-']", "[class*=' conv-']",
    "[id^='conv-']", "[id*='convertful']",
    "img[src*='convertful.com']",
    "iframe[src*='convertful.com']",
  ].map((s) => `body ${s}`).join(",");

  const LATER_TEXTS = ["ربما لاحقاً", "ربما لاحقا", "maybe later", "later"];
  const MODAL_TEXTS = ["دعنا نساعدك في إيجاد سيارتك المثالية", "find your perfect car", "find your ideal car"];

  const injectCss = () => {
    if (document.getElementById("__popup_guard_css")) return;
    const st = document.createElement("style");
    st.id = "__popup_guard_css";
    st.textContent = `
      ${CONV_SEL} { display: none !important; visibility: hidden !important; pointer-events: none !important; }
    `;
    (document.head || document.documentElement).appendChild(st);
  };

  const unlockScroll = () => {
    for (const el of [document.documentElement, document.body]) {
      if (!el) continue;
      el.style.removeProperty("overflow");
      el.style.removeProperty("position");
      el.style.removeProperty("pointer-events");
      Array.from(el.classList).filter((c) => c.startsWith("conv-")).forEach((c) => el.classList.remove(c));
    }
    if (document.body) document.body.removeAttribute("data-scroll-locked");
  };

  // Walk up to the outermost fixed-position overlay that holds `el`
  const overlayRoot = (el) => {
    let cur = el, found = null;
    while (cur && cur !== document.body && cur !== document.documentElement) {
      const pos = getComputedStyle(cur).position;
      if (pos === "fixed") found = cur;
      cur = cur.parentElement;
    }
    return found;
  };

  const removeConvertful = () => {
    let n = 0;
    document.querySelectorAll(CONV_SEL).forEach((el) => {
      // try the widget's own close button first
      const closeBtn = el.querySelector?.(".conv-close, [class*='conv-close'], [aria-label='Close'], [aria-label='close']");
      try { closeBtn?.click(); } catch (e) {}
      const root = overlayRoot(el) || el;
      try { root.remove(); n++; } catch (e) {}
    });
    return n;
  };

  const dismissOnboarding = () => {
    // 1) "Maybe later" button
    const btns = Array.from(document.querySelectorAll("button"));
    const later = btns.find((b) => {
      const t = clean(b.innerText).toLowerCase();
      return t && LATER_TEXTS.some((x) => t === x.toLowerCase());
    });
    if (later) {
      try { later.click(); } catch (e) {}
    }

    // 2) Find the modal by its heading text; click X, else remove overlay
    const heads = Array.from(document.querySelectorAll("h2, h3"));
    const head = heads.find((h) => {
      const t = clean(h.innerText).toLowerCase();
      return MODAL_TEXTS.some((x) => t.includes(x.toLowerCase()));
    });
    if (!head && !later) return false;

    const card = (head || later).closest("div.rounded-2xl, [role='dialog'], [data-slot='dialog-content']");
    if (card) {
      const x = card.querySelector("button[aria-label='Close'], button[aria-label='close'], button[aria-label='إغلاق']");
      try { x?.click(); } catch (e) {}
    }

    // 3) If it's still in the DOM after clicking, remove the overlay root
    setTimeout(() => {
      const stillThere = head && document.contains(head);
      if (stillThere) {
        const root = overlayRoot(head);
        try { (root || card)?.remove(); } catch (e) {}
        // also drop any leftover full-screen backdrop
        document.querySelectorAll("[data-slot='dialog-overlay'], div.fixed.inset-0").forEach((b) => {
          if (!clean(b.innerText)) { try { b.remove(); } catch (e) {} }
        });
      }
      unlockScroll();
    }, 400);
    return true;
  };

  const runOnce = () => {
    let did = false;
    try { injectCss(); } catch (e) {}
    try { if (removeConvertful() > 0) did = true; } catch (e) {}
    try { if (dismissOnboarding()) did = true; } catch (e) {}
    if (did) { try { unlockScroll(); } catch (e) {} }
    return did;
  };

  // Install a long-lived observer once per document
  if (!window.__popupGuardInstalled) {
    window.__popupGuardInstalled = true;
    let scheduled = false;
    const schedule = () => {
      if (scheduled) return;
      scheduled = true;
      setTimeout(() => { scheduled = false; runOnce(); }, 150);
    };
    const start = () => {
      try {
        new MutationObserver(schedule).observe(document.documentElement, { childList: true, subtree: true });
      } catch (e) {}
      runOnce();
    };
    if (document.documentElement) start();
    else document.addEventListener("DOMContentLoaded", start);
  }

  return runOnce();
})()
"""


async def install_popup_guard(tab: Any) -> None:
    """
    Call once per tab right after creating it. Survives navigations within the tab.
    """
    try:
        from nodriver import cdp
    except Exception:
        cdp = None

    if cdp is not None:
        try:
            await tab.send(cdp.network.enable())
            _block = getattr(cdp.network, "set_blocked_urls", None) or getattr(cdp.network, "set_blocked_ur_ls")
            await tab.send(_block(urls=BLOCKED_URL_PATTERNS))
        except Exception as e:
            log("[popup] could not block convertful urls:", repr(e))
        try:
            await tab.send(cdp.page.enable())
            await tab.send(cdp.page.add_script_to_evaluate_on_new_document(source=POPUP_GUARD_JS))
        except Exception as e:
            log("[popup] could not register guard script:", repr(e))

    await dismiss_popups(tab)


async def dismiss_popups(tab: Any) -> bool:
    """
    Dismiss any visible popup right now (and make sure the in-page observer
    is running). Returns True if something was dismissed. Never raises.
    """
    try:
        res = await tab.evaluate(POPUP_GUARD_JS)
        if isinstance(res, dict):
            res = res.get("value", res)
        did = bool(res)
        if did:
            log("[popup] dismissed overlay")
        return did
    except Exception:
        return False
