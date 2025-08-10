# main.py
import os
import re
import time
import uuid
from datetime import datetime, timezone, date
from typing import Optional, Tuple, Dict
import json, hashlib
from uuid import uuid4

import cloudscraper
from bs4 import BeautifulSoup
from supabase import create_client, Client  # pip install supabase
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("URL")
SUPABASE_KEY = os.getenv("KEY")

JITTER_MS_MIN = 5000
JITTER_MS_MAX = 10000
RETRY_LIMIT   = 1
BACKOFF_SEC   = 45

WATCH_URLS = [
    "https://watchcharts.com/watch_model/35441-cartier-santos-large-wssa0018/overview",
    "https://watchcharts.com/watch_model/1525-rolex-gmt-master-ii-batgirl-126710blnr/overview",
    "https://watchcharts.com/watch_model/727-rolex-datejust-41-126334/overview",
    "https://watchcharts.com/watch_model/46426-rolex-cosmograph-daytona-126500/overview",
    "https://watchcharts.com/watch_model/22871-patek-philippe-nautilus-5711-stainless-steel-5711-1a/overview",
    "https://watchcharts.com/watch_model/22557-patek-philippe-aquanaut-5167-stainless-steel-5167a/overview",
    "https://watchcharts.com/watch_model/30921-omega-speedmaster-professional-moonwatch-310-30-42-50-01-002/overview",
    "https://watchcharts.com/watch_model/869-omega-seamaster-diver-300m-210-30-42-20-01-001/overview",
    "https://watchcharts.com/watch_model/403-omega-seamaster-300m-chronometer-2254-50/overview",
    "https://watchcharts.com/watch_model/2700-omega-seamaster-aqua-terra-150m-master-chronometer-41-220-10-41-21-10-001/overview",
    "https://watchcharts.com/watch_model/36333-tudor-black-bay-pro-79470/overview",
    "https://watchcharts.com/watch_model/36318-vacheron-constantin-historiques-222-4200h-222j-b935/overview",
    "https://watchcharts.com/watch_model/1748-grand-seiko-shunbun-sbga413/overview",
    "https://watchcharts.com/watch_model/46344-iwc-ingenieur-automatic-40-328903/overview"
]

# ---------- Supabase ----------
def connect_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError(
            "Missing Supabase credentials. Set URL/KEY (or SUPABASE_URL/SUPABASE_ANON_KEY)."
        )
    client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return client

def quick_supabase_check(sb: Client) -> None:
    try:
        resp = sb.table("watch_specs").select("url").limit(1).execute()
        count = len(resp.data or [])
        if count == 0:
            resp2 = sb.table("watch_prices").select("url").limit(1).execute()
            count = len(resp2.data or [])
        print(f"[supabase] connection OK (sample rows found: {count})")
    except Exception as e:
        raise RuntimeError(f"[supabase] connection/test query failed: {e}")

# ---------- Fetcher ----------
def make_session() -> cloudscraper.CloudScraper:
    return cloudscraper.create_scraper()  # default works best with CF here

def is_interstitial(html: str) -> bool:
    t = (html or "").lower()
    return ("attention required" in t) or ("just a moment" in t)

def jitter_sleep():
    import random
    ms = random.randint(JITTER_MS_MIN, JITTER_MS_MAX)
    time.sleep(ms / 1000)

def polite_get_per_url(url: str) -> Optional[str]:
    """
    Fresh cloudscraper session per URL.
    One retry with backoff if blocked.
    """
    jitter_sleep()
    try:
        sess = make_session()
        r = sess.get(url, timeout=30)
        if r.status_code == 200 and not is_interstitial(r.text):
            return r.text

        print(f"[fetch] blocked/err ({r.status_code}); backoff {BACKOFF_SEC}s then try fresh session again: {url}")
        time.sleep(BACKOFF_SEC)

        sess2 = make_session()
        r2 = sess2.get(url, timeout=30)
        if r2.status_code == 200 and not is_interstitial(r2.text):
            return r2.text

        print(f"[fetch] still blocked ({r2.status_code}) for {url}")
        return None
    except Exception as e:
        print(f"[fetch] error on {url}: {e}")
        return None

# ---------- Parsing (prices) ----------
_money_re = re.compile(r'([$€£])\s*([0-9][\d,]*(?:\.\d{1,2})?)')

def _find_price_near_any_label(soup: BeautifulSoup, labels: Tuple[str, ...]) -> Optional[Tuple[str, float]]:
    for label_text in labels:
        for node in soup.find_all(string=True):
            if not node:
                continue
            if label_text.lower() in node.strip().lower():
                for nxt in node.parent.find_all(string=True):
                    m = _money_re.search(nxt)
                    if m:
                        symbol, amt = m.group(1), m.group(2)
                        try:
                            return symbol, float(amt.replace(",", ""))
                        except ValueError:
                            pass
                for nxt in node.find_all_next(string=True):
                    m = _money_re.search(nxt)
                    if m:
                        symbol, amt = m.group(1), m.group(2)
                        try:
                            return symbol, float(amt.replace(",", ""))
                        except ValueError:
                            pass
    return None

def parse_prices(html: str) -> Optional[Dict[str, float]]:
    soup = BeautifulSoup(html, "html.parser")
    retail = _find_price_near_any_label(soup, ("Retail Price", "Retail"))
    market = _find_price_near_any_label(soup, ("Market Price", "Market Average", "Market"))

    if not (retail and market):
        text = soup.get_text(" ", strip=True)
        if not retail:
            m = re.search(r"Retail(?:\s+Price)?[^$€£]{0,80}([$€£]\s*[0-9][\d,]*(?:\.\d{1,2})?)", text, re.I)
            if m:
                val = re.sub(r"[^\d.]", "", m.group(1))
                retail = (m.group(1)[0], float(val)) if val else None
        if not market:
            m = re.search(r"Market(?:\s+(?:Price|Average))?[^$€£]{0,80}([$€£]\s*[0-9][\d,]*(?:\.\d{1,2})?)", text, re.I)
            if m:
                val = re.sub(r"[^\d.]", "", m.group(1))
                market = (m.group(1)[0], float(val)) if val else None

    out: Dict[str, float] = {}
    currency = None
    if retail:
        currency = retail[0]
        out["retail_price"] = int(round(retail[1]))
    if market:
        currency = currency or market[0]
        out["market_price"] = int(round(market[1]))
    if not out:
        return None
    out["currency"] = {"$": "USD", "€": "EUR", "£": "GBP"}.get(currency, "UNKNOWN")
    return out

# ---------- Parsing (specs) ----------
LABEL_MAP = {
    "brand": "brand",
    "collection": "collection",
    "model": "model_name",
    "model name": "model_name",
    "reference": "reference",
    "reference(s)": "reference",
    "style": "style",
    "complications": "complications",
    "features": "features",
    "crystal": "crystal",
    "bezel material": "bezel_material",
    "case material": "case_material",
    "case thickness": "case_thickness",
    "case diameter": "case_diameter",
    "dial color": "dial_color",
    "dial numerals": "dial_numerals",
    "lug width": "lug_width",
    "water resistance": "water_resistance",
    "movement": "movement_type",
    "movement type": "movement_type",
    "movement caliber": "movement_caliber",
    "caliber": "movement_caliber",
    "power reserve": "power_reserve",
    "frequency": "frequency",
    "jewels": "number_of_jewels",
    "number of jewels": "number_of_jewels",
}

EXPECTED_COLS = {
    "model_id","model_name","bezel_material","brand","case_diameter","case_material",
    "case_thickness","collection","complications","crystal","dial_color","dial_numerals",
    "features","frequency","image_url","lug_width","movement_caliber","movement_type",
    "number_of_jewels","power_reserve","reference","style","water_resistance","url"
}

def _norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _to_mm(val: str) -> Optional[str]:
    m = re.search(r"([\d.]+)\s*mm", val, re.I)
    return m.group(1) if m else None

def _to_meters(val: str) -> Optional[str]:
    m = re.search(r"(\d+)\s*m\b", val, re.I)
    if m: return m.group(1)
    bar = re.search(r"(\d+)\s*bar\b", val, re.I)
    if bar: return str(int(bar.group(1)) * 10)
    return None

def _to_hours(val: str) -> Optional[str]:
    m = re.search(r"(\d+)\s*h(ours)?", val, re.I)
    return m.group(1) if m else None

def _digits(val: str) -> Optional[str]:
    d = re.sub(r"\D", "", val or "")
    return d or None

def _normalize_specs(raw: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in raw.items():
        key = k.lower()
        col = LABEL_MAP.get(key)
        if not col:
            continue
        val = _norm_text(v)

        if col in ("case_diameter", "lug_width"):
            out[col] = _to_mm(val) or val
        elif col == "water_resistance":
            out[col] = _to_meters(val) or val
        elif col == "power_reserve":
            out[col] = _to_hours(val) or val
        elif col in ("frequency", "number_of_jewels"):
            out[col] = _digits(val) or val
        elif col == "movement_type":
            mt = val.lower()
            for token in ("automatic", "manual", "quartz", "solar", "spring drive", "hybrid"):
                if token in mt:
                    mt = token
                    break
            out[col] = mt
        else:
            out[col] = val
    return out

def _collect_pairs_whole_page(soup: BeautifulSoup) -> Dict[str, str]:
    pairs: Dict[str, str] = {}

    # tables
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) >= 2:
                label = _norm_text(cells[0].get_text(" ", strip=True))
                value = _norm_text(cells[1].get_text(" ", strip=True))
                if label and value and len(label) <= 40:
                    pairs.setdefault(label.lower(), value)

    # definition lists
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            label = _norm_text(dt.get_text(" ", strip=True))
            value = _norm_text(dd.get_text(" ", strip=True))
            if label and value and len(label) <= 40:
                pairs.setdefault(label.lower(), value)

    # shallow two-column rows
    for row in soup.find_all(["div", "li", "p"]):
        children = row.find_all(["div", "span"], recursive=False)
        if len(children) >= 2:
            label = _norm_text(children[0].get_text(" ", strip=True))
            value = _norm_text(children[1].get_text(" ", strip=True))
            if label and value and len(label) <= 40:
                pairs.setdefault(label.lower(), value)

    return pairs

def _extract_header_fields(soup: BeautifulSoup) -> Dict[str, str]:
    """
    Prefer header block for model_name (H2) and model_id/reference (H1 'Ref. XXXX').
    Also try to fetch brand from the breadcrumb.
    """
    out = {}

    # Model name from H2 header
    h2 = soup.select_one("h2.h4, h2.font-weight-bolder, h2.h4.font-weight-bolder")
    if h2:
        name_text = re.sub(r"\s+", " ", h2.get_text(" ", strip=True)).strip()
        if name_text:
            out["model_name"] = name_text

    # model_id / reference from H1 "Ref. ..."
    h1 = soup.find(["h1", "h3"], string=re.compile(r"^\s*Ref\.", re.I))
    if h1:
        ref_text = re.sub(r"^\s*Ref\.?\s*", "", h1.get_text(" ", strip=True), flags=re.I).strip()
        if ref_text:
            out["reference"] = ref_text
            out["model_id"] = f"Ref. {ref_text}"

    # brand from breadcrumb if available
    bc = soup.select_one('nav[aria-label="breadcrumb"] li.breadcrumb-item a')
    if bc and bc.get_text(strip=True):
        out["brand"] = bc.get_text(strip=True)

    return out

def _token_variants(ref: str) -> set:
    r = ref.strip()
    return {r, r.replace("-", ""), r.replace(".", ""), r.upper(), r.lower()}

def _strip_refs_from_name(name: str, refs_csv: Optional[str]) -> str:
    if not name or not refs_csv:
        return name or ""
    out = name
    refs = [t.strip() for t in refs_csv.split(",") if t.strip()]
    for ref in refs:
        for v in _token_variants(ref):
            out = re.sub(rf'[\s,()\-\u2013\u2014]*\b{re.escape(v)}\b', '', out, flags=re.I)
    out = re.sub(r'\s{2,}', ' ', out).strip(" ,()-")
    return out

def _pretty_name_from_url(page_url: str, ref: Optional[str]) -> Optional[str]:
    if not page_url:
        return None
    m = re.search(r"/watch_model/\d+-([a-z0-9\-]+)/overview", page_url, re.I)
    if not m:
        return None
    slug = m.group(1)
    tokens = slug.split("-")
    if ref:
        last = tokens[-1].lower()
        ref_norm = re.sub(r"[^a-z0-9]", "", ref.lower())
        last_norm = re.sub(r"[^a-z0-9]", "", last)
        if last_norm == ref_norm:
            tokens = tokens[:-1]
    def tc(t: str) -> str:
        return t.upper() if t in {"ii","iii","iv","vi","vii","viii","ix","x"} else t.capitalize()
    return " ".join(tc(t) for t in tokens if t)

def parse_specs(html: str, page_url: Optional[str] = None) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")

    # Image
    image_url = None
    og_img = soup.find("meta", attrs={"property": "og:image"})
    if og_img and og_img.get("content"):
        image_url = og_img["content"]

    # Header-derived fields first (preferred)
    hdr = _extract_header_fields(soup)

    # Collect pairs and normalize
    raw_pairs = _collect_pairs_whole_page(soup)
    specs = _normalize_specs(raw_pairs)

    # Merge header fields over normalized pairs (header wins)
    specs.update(hdr)

    # Ensure model_id from reference if missing
    if "model_id" not in specs and specs.get("reference"):
        first_ref = re.sub(r"^\s*Ref\.?\s*", "", specs["reference"].split(",")[0], flags=re.I).strip()
        if first_ref:
            specs["model_id"] = f"Ref. {first_ref}"

    # If model_name still missing, derive from og:title / h1 / URL and strip refs
    if not specs.get("model_name"):
        # try og:title
        og = soup.find("meta", attrs={"property": "og:title"})
        name = None
        if og and og.get("content"):
            name = re.sub(r"\s*\|\s*WatchCharts.*$", "", og["content"]).strip()
        # try generic h1
        if not name and soup.find("h1"):
            name = _norm_text(soup.find("h1").get_text())
        # try URL
        if not name and page_url:
            name = _pretty_name_from_url(page_url, specs.get("reference"))
        # strip any reference codes from derived name
        name = _strip_refs_from_name(name or "", specs.get("reference"))
        if name:
            specs["model_name"] = name

    # Attach image url
    if image_url:
        specs["image_url"] = image_url

    # Keep only expected columns
    specs = {k: v for k, v in specs.items() if k in EXPECTED_COLS}
    return specs

# ---------- DB helpers ----------
def specs_hash(specs: Dict[str, str]) -> str:
    s = json.dumps({k: specs[k] for k in sorted(specs)}, separators=(",", ":"), ensure_ascii=False)
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def db_specs_exists(sb: Client, url: str) -> bool:
    res = sb.table("watch_specs").select("id").eq("url", url).limit(1).execute()
    return bool(res.data)

def db_insert_specs_if_absent(sb: Client, url: str, specs: Dict[str, str]) -> str:
    if db_specs_exists(sb, url):
        return "skipped (already exists)"
    allowed = {
        "model_id","model_name","bezel_material","brand","case_diameter","case_material",
        "case_thickness","collection","complications","crystal","dial_color","dial_numerals",
        "features","frequency","image_url","lug_width","movement_caliber","movement_type",
        "number_of_jewels","power_reserve","reference","style","water_resistance"
    }
    payload = {k: v for k, v in specs.items() if k in allowed and v is not None}
    payload.update({
        "id": str(uuid4()),
        "url": url,
        "specs_hash": specs_hash(payload),
        "updated_at": datetime.now(timezone.utc).isoformat()
    })
    sb.table("watch_specs").insert(payload).execute()
    return "inserted"

def db_get_today_price_row(sb: Client, url: str):
    start = date.today().isoformat() + "T00:00:00Z"
    end   = date.today().isoformat() + "T23:59:59.999999Z"
    res = (sb.table("watch_prices")
            .select("id")
            .eq("url", url)
            .gte("timestamp", start)
            .lte("timestamp", end)
            .limit(1)
            .execute())
    return res.data[0] if res.data else None

def db_upsert_price_today(sb: Client, url: str, prices: Dict[str, int], model_id: Optional[str], model_name: Optional[str]) -> str:
    now_iso = datetime.now(timezone.utc).isoformat()
    existing = db_get_today_price_row(sb, url)
    payload = {
        "url": url,
        "retail_price": prices.get("retail_price"),
        "market_price": prices.get("market_price"),
        "timestamp": now_iso,
        "model_id": model_id,
        "model_name": model_name,
    }
    if existing:
        sb.table("watch_prices").update(payload).eq("id", existing["id"]).execute()
        return "updated (today)"
    else:
        payload["id"] = str(uuid4())
        sb.table("watch_prices").insert(payload).execute()
        return "inserted (today)"

# ---------- Run ----------
def main():
    sb = connect_supabase()
    quick_supabase_check(sb)

    for url in WATCH_URLS:
        print(f"\n== {url}")
        html = polite_get_per_url(url)
        if not html:
            print("  fetch: FAILED (blocked or error)")
            continue

        prices = parse_prices(html)
        specs = parse_specs(html, page_url=url)

        # ---- DB writes (write-once specs, daily price snapshot) ----
        if specs:
            s_action = db_insert_specs_if_absent(sb, url, specs)
            print(f"  specs write: {s_action}")

        if prices:
            model_id = specs.get("model_id") if specs else None
            model_name = specs.get("model_name") if specs else None
            p_action = db_upsert_price_today(sb, url, prices, model_id, model_name)
            print(f"  price write: {p_action}")

if __name__ == "__main__":
    main()
