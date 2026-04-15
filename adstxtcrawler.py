import asyncio
import aiohttp
import time
import sys
import os
import glob
import random
import logging
import warnings
from tqdm import tqdm

# Hard disable any system error or warning output to the console
logging.getLogger('asyncio').setLevel(logging.CRITICAL)
warnings.filterwarnings("ignore")

# Settings
OUTPUT_ADS = 'has_ads.txt'
OUTPUT_APP_ADS = 'has_app_ads.txt'
CONCURRENCY_LIMIT = 30  # Number of concurrent checks for network stability
TIMEOUT_SECONDS = 15    # Timeout for each request to allow slower servers to respond

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

async def check_url(session, url):
    try:
        async with session.get(url, timeout=TIMEOUT_SECONDS, ssl=False, allow_redirects=True) as response:
            if response.status == 200:
                content_type = response.headers.get('Content-Type', '').lower()
                if 'text/html' not in content_type:
                    return True
    except Exception:
        pass
    return False

def load_existing_results(filepath):
    if not os.path.exists(filepath):
        return set()
    with open(filepath, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

async def worker(queue, session, file_lock, pbar, existing_ads, existing_app_ads):
    while True:
        domain = await queue.get()
        clean_domain = domain.replace('http://', '').replace('https://', '').split('/')[0]

        # If the domain is already in both result files, skip the network check entirely
        if clean_domain in existing_ads and clean_domain in existing_app_ads:
            pbar.update(1)
            queue.task_done()
            continue

        ads_url = f"https://{clean_domain}/ads.txt"
        app_ads_url = f"https://{clean_domain}/app-ads.txt"

        # Check only what is not yet in the result files
        has_ads = True if clean_domain in existing_ads else await check_url(session, ads_url)
        has_app_ads = True if clean_domain in existing_app_ads else await check_url(session, app_ads_url)

        async with file_lock:
            # Write to file only if found now and it was not in the file before
            if has_ads and clean_domain not in existing_ads:
                with open(OUTPUT_ADS, 'a', encoding='utf-8') as f:
                    f.write(f"{clean_domain}\n")
                existing_ads.add(clean_domain)
            
            if has_app_ads and clean_domain not in existing_app_ads:
                with open(OUTPUT_APP_ADS, 'a', encoding='utf-8') as f:
                    f.write(f"{clean_domain}\n")
                existing_app_ads.add(clean_domain)

        pbar.update(1)
        queue.task_done()

async def main():
    # Mute the internal event loop logger
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda loop, context: None)

    start_time = time.time()

    # Find all files starting with "domains" and ending with ".txt"
    input_files = glob.glob('domains*.txt')
    
    if not input_files:
        print("Error: No files found matching the pattern domains*.txt")
        return

    print(f"Found input files: {len(input_files)}")
    
    # Collect all domains into a set (set automatically removes source duplicates)
    all_domains = set()
    for file in input_files:
        with open(file, 'r', encoding='utf-8') as f:
            for line in f:
                d = line.strip()
                if d:
                    all_domains.add(d)

    # Convert to a list and shuffle randomly
    domains_list = list(all_domains)
    random.shuffle(domains_list)
    print(f"Total unique domains to check: {len(domains_list)}")

    # Load already checked domains from previous runs
    existing_ads = load_existing_results(OUTPUT_ADS)
    existing_app_ads = load_existing_results(OUTPUT_APP_ADS)
    
    print(f"Already in cache: ads.txt ({len(existing_ads)}), app-ads.txt ({len(existing_app_ads)})")
    print("Starting check...\n")

    queue = asyncio.Queue()
    for domain in domains_list:
        queue.put_nowait(domain)

    file_lock = asyncio.Lock()
    connector = aiohttp.TCPConnector(limit=0)
    
    with tqdm(total=len(domains_list), desc="Progress", unit="dom") as pbar:
        async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
            workers = [
                asyncio.create_task(worker(queue, session, file_lock, pbar, existing_ads, existing_app_ads))
                for _ in range(CONCURRENCY_LIMIT)
            ]
            
            await queue.join()
            
            for w in workers:
                w.cancel()

    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed:.2f} sec.")
    print(f"File {OUTPUT_ADS} contains unique records: {len(existing_ads)}")
    print(f"File {OUTPUT_APP_ADS} contains unique records: {len(existing_app_ads)}")

if __name__ == '__main__':
    # Fix for correct asyncio operation in Windows
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
