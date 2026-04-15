import asyncio
import aiohttp
from aiohttp import ClientTimeout
import time
import sys
import os
import glob
import random
import logging
import warnings
from tqdm import tqdm

# Disable asyncio logging and warnings for cleaner output
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

async def check_url_and_count(session, url, max_retries=3):
    """
    Checks the URL for ads.txt/app-ads.txt content and counts valid lines.
    Includes a retry mechanism for failed requests.
    """
    for attempt in range(max_retries):
        try:
            # Using ClientTimeout instead of passing timeout integer directly
            async with session.get(url, timeout=ClientTimeout(total=TIMEOUT_SECONDS), ssl=False, allow_redirects=True) as response:
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    # Ensure the response is not an HTML page (like a 404 disguised as 200)
                    if 'text/html' not in content_type:
                        text = await response.text(errors='ignore')
                        line_count = len([line for line in text.split('\n') if line.strip()])
                        return True, line_count
                
                # If the file definitely doesn't exist or access is forbidden permanently, do not retry
                elif response.status in (404, 403, 400):
                    return False, 0
                    
        except asyncio.TimeoutError:
            # Handle specific timeout exception if needed
            pass
        except Exception:
            # Handle connection errors or other unexpected issues
            pass
            
        # If it's not the last attempt, wait 1-2 seconds before the next request
        if attempt < max_retries - 1:
            await asyncio.sleep(1 + random.uniform(0, 1)) 

    # If all attempts are exhausted, mark as failed
    return False, 0

def load_existing_results(filepath):
    """
    Loads already processed domains from the output file to skip them on restart.
    """
    if not os.path.exists(filepath):
        return set()
    with open(filepath, 'r', encoding='utf-8') as f:
        return set(line.split(',')[0].strip() for line in f if line.strip())

async def worker(queue, session, file_lock, pbar, existing_ads, existing_app_ads):
    """
    Worker function that processes domains from the queue.
    """
    while True:
        domain = await queue.get()
        # Clean the domain in case the input contains protocols or paths
        clean_domain = domain.replace('http://', '').replace('https://', '').split('/')[0]

        # Skip if domain is already fully processed in both files
        if clean_domain in existing_ads and clean_domain in existing_app_ads:
            pbar.update(1)
            queue.task_done()
            continue

        ads_url = f"https://{clean_domain}/ads.txt"
        app_ads_url = f"https://{clean_domain}/app-ads.txt"

        # Check ads.txt (skip network request if already in cache)
        has_ads, ads_lines = (True, 0) if clean_domain in existing_ads else await check_url_and_count(session, ads_url)
        # Check app-ads.txt (skip network request if already in cache)
        has_app_ads, app_ads_lines = (True, 0) if clean_domain in existing_app_ads else await check_url_and_count(session, app_ads_url)

        # Write results using an async lock to prevent race conditions
        async with file_lock:
            if has_ads and clean_domain not in existing_ads:
                with open(OUTPUT_ADS, 'a', encoding='utf-8') as f:
                    f.write(f"{clean_domain},{ads_lines}\n")
                existing_ads.add(clean_domain)
            
            if has_app_ads and clean_domain not in existing_app_ads:
                with open(OUTPUT_APP_ADS, 'a', encoding='utf-8') as f:
                    f.write(f"{clean_domain},{app_ads_lines}\n")
                existing_app_ads.add(clean_domain)

        pbar.update(1)
        queue.task_done()

async def main():
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda loop, context: None)

    start_time = time.time()

    # Find all input files matching the pattern
    input_files = glob.glob('domains*.txt')
    
    if not input_files:
        print("Error: No files found matching the pattern domains*.txt")
        return

    print(f"Found input files: {len(input_files)}")
    
    # Collect all unique domains from the input files
    all_domains = set()
    for file in input_files:
        with open(file, 'r', encoding='utf-8') as f:
            for line in f:
                d = line.strip()
                if d:
                    all_domains.add(d)

    domains_list = list(all_domains)
    random.shuffle(domains_list)
    print(f"Total unique domains to check: {len(domains_list)}")

    # Load previously processed domains
    existing_ads = load_existing_results(OUTPUT_ADS)
    existing_app_ads = load_existing_results(OUTPUT_APP_ADS)
    
    print(f"Already in cache: ads.txt ({len(existing_ads)}), app-ads.txt ({len(existing_app_ads)})")
    print("Starting check...\n")

    # Populate the queue
    queue = asyncio.Queue()
    for domain in domains_list:
        queue.put_nowait(domain)

    file_lock = asyncio.Lock()
    connector = aiohttp.TCPConnector(limit=0)
    
    # Run the processing with a progress bar
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
    # Fix for Windows asyncio loop issues
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
