import asyncio
import aiohttp
from aiohttp import ClientTimeout
import time
import sys
import os
import glob
import random
import re
import signal
import logging
import warnings
from tqdm import tqdm

# Hard disable any system error or warning output to the console
logging.getLogger('asyncio').setLevel(logging.CRITICAL)
warnings.filterwarnings("ignore")

# Settings - Adjusted for maximum stability and minimal false negatives
OUTPUT_ADS = 'has_ads.txt'
OUTPUT_APP_ADS = 'has_app_ads.txt'
CONCURRENCY_LIMIT = 30   # Reduced concurrent checks for network stability
TIMEOUT_SECONDS = 15     # Increased wait time for slower servers
FLUSH_INTERVAL = 100     # Write results to disk every N domains
CONTENT_PEEK_SIZE = 2048 # Bytes to read for content validation
DNS_CACHE_TTL = 300      # DNS cache lifetime in seconds

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# Simple pattern: at least one line that looks like an ads.txt entry or a comment
# Examples: "google.com, pub-1234567890, DIRECT, abc123def"  or  "# comment"
ADS_TXT_PATTERN = re.compile(
    r'(?m)^(?:#.*|[a-zA-Z0-9][\w.\-]*\.[a-zA-Z]{2,}\s*,\s*.+)$'
)

# Graceful shutdown flag
shutdown_event = asyncio.Event()


async def check_url(session, url, original_domain, max_retries=3):
    """Check if a valid ads.txt / app-ads.txt exists at the URL and count lines with retries."""
    for attempt in range(max_retries):
        try:
            timeout = ClientTimeout(total=TIMEOUT_SECONDS)
            async with session.get(url, timeout=timeout, allow_redirects=True) as response:
                
                # If the file definitely doesn't exist or access is forbidden, do not retry
                if response.status in (404, 403, 400):
                    return False, 0
                
                if response.status == 200:
                    # Check that we didn't get redirected to a completely different domain
                    final_host = response.url.host or ''
                    if original_domain not in final_host and final_host not in original_domain:
                        # Allow common patterns like www.example.com -> example.com and vice versa
                        stripped_final = final_host.lstrip('www.')
                        stripped_original = original_domain.lstrip('www.')
                        if stripped_final != stripped_original:
                            return False, 0

                    content_type = response.headers.get('Content-Type', '').lower()

                    # Reject if server returned an HTML page (likely a custom 404)
                    if 'text/html' in content_type:
                        return False, 0

                    # Read the full content to count lines
                    text = await response.text(errors='replace')
                    if not text:
                        return False, 0

                    # Must contain at least one ads.txt-like line or comment
                    if not ADS_TXT_PATTERN.search(text):
                        return False, 0

                    # Count valid lines
                    line_count = len([line for line in text.split('\n') if line.strip()])
                    return True, line_count
                    
                # If status is 500, 502, 503, 504, etc., it will skip the above blocks 
                # and proceed to the sleep step below to retry.

        except asyncio.TimeoutError:
            # Handle specific timeout exception
            pass
        except Exception:
            # Handle connection errors or other unexpected issues
            pass
            
        # If it's not the last attempt, wait 1 second before the next request
        if attempt < max_retries - 1:
            await asyncio.sleep(1)

    # If all attempts are exhausted
    return False, 0


def load_existing_results(filepath):
    """Load already found domains from a previous run."""
    if not os.path.exists(filepath):
        return set()
    with open(filepath, 'r', encoding='utf-8') as f:
        return set(line.split(',')[0].strip() for line in f if line.strip())


def flush_buffer(buffer_ads, buffer_app_ads):
    """Append buffered results to disk."""
    if buffer_ads:
        with open(OUTPUT_ADS, 'a', encoding='utf-8') as f:
            f.writelines(buffer_ads)
        buffer_ads.clear()
    if buffer_app_ads:
        with open(OUTPUT_APP_ADS, 'a', encoding='utf-8') as f:
            f.writelines(buffer_app_ads)
        buffer_app_ads.clear()


async def worker(queue, session, results_lock, pbar,
                 existing_ads, existing_app_ads,
                 buffer_ads, buffer_app_ads, flush_counter):
    """Worker coroutine: pick domains from the queue and check them."""
    while not shutdown_event.is_set():
        try:
            domain = queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        clean_domain = domain.replace('http://', '').replace('https://', '').split('/')[0]

        ads_url = f"https://{clean_domain}/ads.txt"
        app_ads_url = f"https://{clean_domain}/app-ads.txt"

        has_ads, ads_lines = await check_url(session, ads_url, clean_domain)
        has_app_ads, app_ads_lines = await check_url(session, app_ads_url, clean_domain)

        async with results_lock:
            if has_ads and clean_domain not in existing_ads:
                existing_ads.add(clean_domain)
                buffer_ads.append(f"{clean_domain},{ads_lines}\n")

            if has_app_ads and clean_domain not in existing_app_ads:
                existing_app_ads.add(clean_domain)
                buffer_app_ads.append(f"{clean_domain},{app_ads_lines}\n")

            flush_counter[0] += 1
            if flush_counter[0] >= FLUSH_INTERVAL:
                flush_buffer(buffer_ads, buffer_app_ads)
                flush_counter[0] = 0

        pbar.update(1)
        queue.task_done()


async def main():
    # Mute the internal event loop logger
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda loop, context: None)

    # Graceful shutdown on Ctrl+C
    def _signal_handler():
        if not shutdown_event.is_set():
            print("\n\nShutting down gracefully, writing remaining results...")
            shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler; fall back to signal.signal
            signal.signal(sig, lambda s, f: _signal_handler())

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

    # Load already checked domains from previous runs
    existing_ads = load_existing_results(OUTPUT_ADS)
    existing_app_ads = load_existing_results(OUTPUT_APP_ADS)

    # Skip domains that are already in BOTH result files — nothing new to check
    already_done = existing_ads & existing_app_ads
    domains_to_check = [
        d.replace('http://', '').replace('https://', '').split('/')[0]
        for d in all_domains
    ]
    # Deduplicate after cleaning
    domains_to_check = list(set(domains_to_check) - already_done)
    random.shuffle(domains_to_check)

    skipped = len(all_domains) - len(domains_to_check)
    print(f"Total unique domains: {len(all_domains)}")
    print(f"Already in both caches (skipped): {skipped}")
    print(f"Domains to check: {len(domains_to_check)}")
    print(f"Cache: ads.txt ({len(existing_ads)}), app-ads.txt ({len(existing_app_ads)})")
    print("Starting check...\n")

    if not domains_to_check:
        print("Nothing to check — all domains are already processed.")
        return

    queue = asyncio.Queue()
    for domain in domains_to_check:
        queue.put_nowait(domain)

    results_lock = asyncio.Lock()
    buffer_ads = []
    buffer_app_ads = []
    flush_counter = [0]  # mutable counter shared across workers

    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY_LIMIT + 20,  # slightly above worker count
        ttl_dns_cache=DNS_CACHE_TTL,
        ssl=False,
    )

    try:
        with tqdm(total=len(domains_to_check), desc="Progress", unit="dom") as pbar:
            async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
                workers = [
                    asyncio.create_task(
                        worker(queue, session, results_lock, pbar,
                               existing_ads, existing_app_ads,
                               buffer_ads, buffer_app_ads, flush_counter)
                    )
                    for _ in range(CONCURRENCY_LIMIT)
                ]

                await queue.join()

                for w in workers:
                    w.cancel()
    finally:
        # Always flush remaining buffered results on exit (normal or interrupted)
        flush_buffer(buffer_ads, buffer_app_ads)

    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed:.2f} sec.")
    print(f"File {OUTPUT_ADS} contains unique records: {len(existing_ads)}")
    print(f"File {OUTPUT_APP_ADS} contains unique records: {len(existing_app_ads)}")


if __name__ == '__main__':
    # Fix for correct asyncio operation in Windows
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
