import asyncio
import csv
import random
import re
import logging
import uuid
import pandas as pd
from datetime import datetime, date
from playwright.async_api import async_playwright
from supabase import create_client, Client
from dotenv import load_dotenv
import os

load_dotenv()

supabase_url = os.getenv("URL")
supabase_key = os.getenv("KEY")
supabase: Client = create_client(supabase_url, supabase_key)

# ------------------ Logging Setup ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("scraper.log", mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

watch_urls = [
    "https://watchcharts.com/watch_model/35441-cartier-santos-large-wssa0018/overview",
    "https://watchcharts.com/watch_model/1525-rolex-gmt-master-ii-batgirl-126710blnr/overview",
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

def clean_price(text):
    cleaned = re.sub(r'[^\d.]', '', text)
    return cleaned if cleaned else "N/A"

def normalize_key(key):   
    key = key.lower().strip()
    key = re.sub(r'[^\w\s]', '', key) # Remove special characters
    key = key.replace(" ", "_")
    if key == "references":
        key = "reference"
    return key

async def scrape_spec_table(page):
    specs = {}
    try:
        rows = await page.query_selector_all("table.spec-table tr")
        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) == 2:
                raw_key = (await cells[0].inner_text()).strip()
                value = (await cells[1].inner_text()).strip()
                key = normalize_key(raw_key)
                specs[key] = value
    except Exception as e:
        logging.error(f"Error extracting spec table: {e}")
    return specs

async def scrape_watch_data(browser, url, timestamp):
    try:
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        )
        await context.route("**/*", lambda route, request: route.abort() if request.resource_type in ["image", "stylesheet", "font"] else route.continue_())
        page = await context.new_page()

        logging.info(f"Scraping started for {url}")
        try:
            await page.goto(url, timeout=60000)
            await page.wait_for_selector("h1.mb-0.font-weight-bolder.text-break", timeout=60000)
        except Exception as e:
            logging.error(f"Timeout or load error for {url}: {e}")
            await page.close()
            await context.close()
            return {
                "model_id": "Error",
                "model_name": "Error",
                "retail_price": "Error",
                "market_price": "Error",
                "timestamp": timestamp
            }, {
                "model_id": "Error",
                "model_name": "Error"
            }

        model_name = "N/A"
        model_id = "N/A"
        market_price = "N/A"
        retail_price = "N/A"
        image_url = "N/A"

        try:
            model_element = await page.query_selector("h2.h4.font-weight-bolder") or await page.query_selector("h2")
            if model_element:
                model_name = (await model_element.inner_text()).strip()
        except Exception as e:
            logging.warning(f"Could not extract model name for {url}: {e}")

        try:
            id_element = await page.query_selector("h1.mb-0.font-weight-bolder.text-break")
            if id_element:
                model_id = (await id_element.inner_text()).strip()
        except:
            pass

        try:
            market_price_div = await page.query_selector("div.market-price")
            if market_price_div:
                raw_market_price = (await market_price_div.inner_text()).strip()
                market_price = clean_price(raw_market_price)
        except:
            pass
        try:
            retail_label = await page.query_selector("text=Retail Price")
            if retail_label:
                price_container = await retail_label.evaluate_handle(
                    "el => el.closest('div.mb-4').querySelector('div.h2.mb-0.font-weight-bolder.text-secondary')"
                )
                if price_container:
                    raw_retail_price = (await price_container.inner_text()).strip()
                    retail_price = clean_price(raw_retail_price)
        except:
            pass
        try:
            image_container = await page.query_selector("div.mx-0.mx-lg-3.mx-xl-5 img")
            if image_container:
                image_url = await image_container.get_attribute("src")
        except Exception as e:
            pass
        specs = await scrape_spec_table(page)

        await page.close()
        await context.close()

        logging.info(f"Scraping completed for {url}")

        return {
            "model_id": model_id,
            "model_name": model_name,
            "retail_price": retail_price,
            "market_price": market_price,
            "timestamp": timestamp
        }, {
            "model_id": model_id,
            "model_name": model_name,
            "image_url": image_url,
            **specs
        }

    except Exception as e:
        logging.error(f"Unexpected error for {url}: {e}")
        return {
            "model_id": "Error",
            "model_name": "Error",
            "retail_price": "Error",
            "market_price": "Error",
            "timestamp": timestamp
        }, {
            "model_id": "Error",
            "model_name": "Error"
        }

async def scrape_with_retry(browser, url, timestamp, retries=2):
    for attempt in range(retries):
        price_data, specs_data = await scrape_watch_data(browser, url, timestamp)
        if price_data["model_id"] != "Error":
            return price_data, specs_data
        logging.warning(f"Retrying {url} (attempt {attempt + 1})")
        await asyncio.sleep(3)
    return price_data, specs_data

async def scrape_limited_concurrency(browser, urls, timestamp, max_concurrent=3):
    semaphore = asyncio.Semaphore(max_concurrent)

    async def limited_task(url):
        async with semaphore:
            await asyncio.sleep(random.uniform(5, 10))
            return await scrape_with_retry(browser, url, timestamp)

    tasks = [limited_task(url) for url in urls]
    return await asyncio.gather(*tasks)

async def main():
    start_time = datetime.now()
    logging.info("Scraping session started.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        results = await scrape_limited_concurrency(browser, watch_urls, timestamp)

        await browser.close()

        price_results = [r[0] for r in results]
        specs_results = [r[1] for r in results]

        with open(f"watch_prices_{date.today()}.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=price_results[0].keys())
            writer.writeheader()
            writer.writerows(price_results)

        all_spec_keys = set()
        for spec in specs_results:
            all_spec_keys.update(spec.keys())
        all_spec_keys = sorted(all_spec_keys)

        with open("watch_specs.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_spec_keys)
            writer.writeheader()
            writer.writerows(specs_results)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    successful_watches = sum(1 for r in price_results if r["model_id"] != "Error")

    # Create log entry
    log_entry = {
     "id": str(uuid.uuid4()),
     "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
     "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
     "duration_seconds": duration,
     "total_watches": len(watch_urls),
     "successful_watches": successful_watches
    }
    # Insert watch_prices
    for price in price_results:
     supabase.table("watch_prices").insert(price).execute()

    # Insert watch_specs
    for spec in specs_results:
     supabase.table("watch_specs").insert(spec).execute()

    # Insert scrape_logs
    supabase.table("scrape_logs").insert(log_entry).execute()

    # Save to Excel
    log_df = pd.DataFrame([log_entry])
    # log_df.to_excel("scrape_logs.xlsx", index=False)

    logging.info(f"Scraping session completed in {duration:.2f} seconds.")

if __name__ == "__main__":
    asyncio.run(main())
