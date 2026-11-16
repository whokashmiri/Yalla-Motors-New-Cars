# Keep selectors minimal & robust.
# New layout (2026-11): section[aria-label] > div > div[data-ad-id] > article > ... a
# Old layout kept as fallback.
RESULT_ANCHOR_SEL = (
    "article a[href*='/new-cars/'], "
    "[data-ad-id] a[href*='/new-cars/'], "
    "div.group.rounded-lg.border [data-filter-href*='/new-cars/'], "
    "a.hover\\:text-main[href*='/new-cars/']"
)
# Some pages have multiple result containers; we don't rely on them heavily.
RESULT_CONTAINER_SEL = "div.mt-4.flex.w-full.flex-col.flex-wrap"
