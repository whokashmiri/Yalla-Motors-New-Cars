# YallaMotor (KSA) New Cars Scraper — Python + nodriver

Scrapes **new cars** listing pages like:
- https://ksa.yallamotor.com/ar/new-cars/search?page=1

Flow (current phase):
1) Open the listing page.
2) Inside the results container(s), find all anchors:
   - `a.hover:text-main[href^="/new-cars/"]`
3) For each href: open in a new tab and save **raw page snapshot** + extracted basics to MongoDB.

> You said you’ll provide Mongo details in `.env` later — this repo reads them from env vars.

## Setup

```bash
python -m venv .venv
# Windows:
.\.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env
# edit .env
python -m src.main
```

## Environment

See `.env.example`.

## Notes

- Selectors on YallaMotor change; this scraper avoids brittle Tailwind class chains and primarily relies on:
  - `a.hover:text-main[href^="/new-cars/"]`
- The listing page appears in a few layout variants (lg:block vs w-full). We treat them all the same by scanning anchors.
