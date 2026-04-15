"""Async ads.txt / app-ads.txt crawler for bulk domain validation.
github.com/OstinUA/AdVerify-adstxt-appadstxt-Crawler
Reads domain lists from ``domains*.txt`` files in the working directory,
checks each domain for the presence of valid ``/ads.txt`` and
``/app-ads.txt`` files via HTTPS, and writes positive results to
``has_ads.txt`` and ``has_app_ads.txt`` respectively.
Features:
    - Concurrent HTTP checks with configurable parallelism.
    - Batched file I/O to minimize disk overhead.
    - Content-level validation (regex match against IAB ads.txt format).
    - Cross-domain redirect detection to reject parking pages.
    - DNS response caching to reduce resolver pressure.
    - Resume support: previously found domains are loaded from output
      files and skipped on subsequent runs.
    - Graceful shutdown on SIGINT / SIGTERM with guaranteed buffer flush.
Typical usage::
    $ ls domains*.txt
    domains.txt  domains_extra.txt
    $ python check_ads.py
    Found input files: 2
    Total unique domains: 150000
    ...
    Done in 312.47 sec.
Attributes:
    OUTPUT_ADS (str): Path to the output file for domains that have
        a valid ``ads.txt``. Defaults to ``'has_ads.txt'``.
    OUTPUT_APP_ADS (str): Path to the output file for domains that have
        a valid ``app-ads.txt``. Defaults to ``'has_app_ads.txt'``.
    CONCURRENCY_LIMIT (int): Maximum number of worker coroutines
        executing HTTP checks in parallel. Defaults to ``50``.
    TIMEOUT_SECONDS (int): Per-request timeout in seconds. No retries
        are attempted on failure. Defaults to ``5``.
    FLUSH_INTERVAL (int): Number of processed domains between
        consecutive buffer-to-disk flushes. Defaults to ``100``.
    CONTENT_PEEK_SIZE (int): Maximum number of bytes read from the
        response body for content validation. Defaults to ``2048``.
    DNS_CACHE_TTL (int): Time-to-live for cached DNS resolutions
        inside the ``aiohttp.TCPConnector``, in seconds.
        Defaults to ``300``.
    HEADERS (dict[str, str]): Default HTTP headers sent with every
        request. Contains a desktop Chrome ``User-Agent`` string.
    ADS_TXT_PATTERN (re.Pattern): Compiled multiline regex that
        matches at least one IAB-compliant ads.txt data record
        (``domain, account-id, relationship[, cert-authority]``)
        or a comment line starting with ``#``.
    shutdown_event (asyncio.Event): Module-level flag set by the
        signal handler to request a cooperative shutdown of all
        worker coroutines.
"""

import asyncio
import aiohttp
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

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------
OUTPUT_ADS = 'has_ads.txt'
OUTPUT_APP_ADS = 'has_app_ads.txt'
CONCURRENCY_LIMIT = 30   # Number of concurrent checks
TIMEOUT_SECONDS = 15      # Wait time, no retries will be made if it fails
FLUSH_INTERVAL = 100     # Write results to disk every N domains
CONTENT_PEEK_SIZE = 2048 # Bytes to read for content validation
DNS_CACHE_TTL = 300      # DNS cache lifetime in seconds

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# Matches an IAB ads.txt data record or a comment line.
# Data record example: "google.com, pub-1234567890, DIRECT, f08c47fec0942fa0"
# Comment example:     "# This is a comment"
ADS_TXT_PATTERN = re.compile(
    r'(?m)^(?:#.*|[a-zA-Z0-9][\w.\-]*\.[a-zA-Z]{2,}\s*,\s*.+)$'
)

# Module-level cooperative shutdown flag, set by the signal handler.
shutdown_event = asyncio.Event()


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

async def check_url(session, url, original_domain):
    """Validate that a URL serves a genuine ads.txt or app-ads.txt file.
    Performs an HTTP GET request and applies three layers of validation
    before accepting the response as a legitimate ads.txt resource:
    1. **Redirect guard** — rejects responses whose final hostname
       differs from ``original_domain`` (allowing ``www.`` prefix
       variations).
    2. **Content-Type filter** — rejects ``text/html`` responses,
       which typically indicate custom 404 pages.
    3. **Content probe** — reads the first ``CONTENT_PEEK_SIZE`` bytes
       and checks for at least one line matching the IAB ads.txt
       record format or a comment line via ``ADS_TXT_PATTERN``.
    Args:
        session (aiohttp.ClientSession): Reusable HTTP session with
            pre-configured headers and connector settings.
        url (str): Fully qualified URL to check, e.g.
            ``'https://example.com/ads.txt'``.
        original_domain (str): The bare domain that was used to
            construct ``url`` (e.g. ``'example.com'``). Used to
            detect cross-domain redirects.
    Returns:
        bool: ``True`` if the URL responds with HTTP 200, is not an
        HTML page, was not redirected to a foreign domain, and
        contains at least one ads.txt-like line. ``False`` otherwise
        (including on any network or decoding error).
    Examples:
        >>> import aiohttp, asyncio
        >>> async def demo():
        ...     async with aiohttp.ClientSession() as s:
        ...         result = await check_url(s, 'https://google.com/ads.txt', 'google.com')
        ...         print(result)  # True (google.com serves a valid ads.txt)
        >>> asyncio.run(demo())
        True
    """
    try:
        timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
        async with session.get(url, timeout=timeout, allow_redirects=True) as response:
            if response.status != 200:
                return False

            # --- Redirect guard ---
            # Verify the final URL still belongs to the same domain.
            # Allows trivial www-prefix variations (www.x.com ↔ x.com).
            final_host = response.url.host or ''
            if original_domain not in final_host and final_host not in original_domain:
                stripped_final = final_host.lstrip('www.')
                stripped_original = original_domain.lstrip('www.')
                if stripped_final != stripped_original:
                    return False

            # --- Content-Type filter ---
            content_type = response.headers.get('Content-Type', '').lower()
            if 'text/html' in content_type:
                return False

            # --- Content probe ---
            # Read only a small prefix to keep memory and bandwidth low.
            chunk = await response.content.read(CONTENT_PEEK_SIZE)
            if not chunk:
                return False

            try:
                text = chunk.decode('utf-8', errors='replace')
            except Exception:
                return False

            if ADS_TXT_PATTERN.search(text):
                return True

            return False
    except Exception:
        return False


def load_existing_results(filepath):
    """Load a set of domain names from a newline-delimited text file.
    Used to restore results from a previous crawler run so that
    already-discovered domains can be skipped on resume.
    Args:
        filepath (str): Path to the results file (e.g.
            ``'has_ads.txt'``). If the file does not exist, an
            empty set is returned without raising an error.
    Returns:
        set[str]: Unique, stripped, non-empty lines from the file.
            Returns an empty ``set`` when the file is missing.
    Examples:
        >>> load_existing_results('nonexistent_file.txt')
        set()
        >>> # Given has_ads.txt containing "example.com\\ngoogle.com\\n"
        >>> load_existing_results('has_ads.txt')
        {'example.com', 'google.com'}
    """
    if not os.path.exists(filepath):
        return set()
    with open(filepath, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())


def flush_buffer(buffer_ads, buffer_app_ads):
    """Append buffered domain names to the on-disk result files.
    Opens each output file in append mode, writes every entry from
    the corresponding buffer, and clears the buffer in place.
    Does nothing for a buffer that is already empty.
    This function performs **synchronous** file I/O and is intended
    to be called either inside an ``asyncio.Lock`` section or after
    all workers have finished.
    Args:
        buffer_ads (list[str]): Mutable list of domain names to
            append to ``OUTPUT_ADS`` (``'has_ads.txt'``).
            Cleared in place after writing.
        buffer_app_ads (list[str]): Mutable list of domain names to
            append to ``OUTPUT_APP_ADS`` (``'has_app_ads.txt'``).
            Cleared in place after writing.
    Returns:
        None
    Examples:
        >>> buf_ads = ['example.com', 'test.org']
        >>> buf_app = []
        >>> flush_buffer(buf_ads, buf_app)
        >>> buf_ads
        []
    """
    if buffer_ads:
        with open(OUTPUT_ADS, 'a', encoding='utf-8') as f:
            f.writelines(f"{d}\n" for d in buffer_ads)
        buffer_ads.clear()
    if buffer_app_ads:
        with open(OUTPUT_APP_ADS, 'a', encoding='utf-8') as f:
            f.writelines(f"{d}\n" for d in buffer_app_ads)
        buffer_app_ads.clear()


async def worker(queue, session, results_lock, pbar,
                 existing_ads, existing_app_ads,
                 buffer_ads, buffer_app_ads, flush_counter):
    """Consume domains from a shared queue and validate their ad files.
    Each invocation of this coroutine runs in a loop, pulling domains
    from ``queue`` until it is empty or ``shutdown_event`` is set.
    For every domain it constructs the ``/ads.txt`` and
    ``/app-ads.txt`` URLs, delegates validation to :func:`check_url`,
    and — under a shared lock — records positive results in the
    in-memory sets and write buffers. When the accumulated count
    reaches ``FLUSH_INTERVAL``, the buffers are flushed to disk.
    Designed to be launched as an ``asyncio.Task``; multiple workers
    run concurrently against the same queue and shared state.
    Args:
        queue (asyncio.Queue): Pre-filled queue of domain name
            strings to process.
        session (aiohttp.ClientSession): Shared HTTP session used
            for all outgoing requests.
        results_lock (asyncio.Lock): Guards concurrent writes to
            ``existing_ads``, ``existing_app_ads``, the write
            buffers, and the flush counter.
        pbar (tqdm.tqdm): Progress bar instance; incremented by 1
            after each domain is fully processed.
        existing_ads (set[str]): In-memory set of domains already
            known to have ``ads.txt``. Read and mutated under
            ``results_lock``.
        existing_app_ads (set[str]): In-memory set of domains already
            known to have ``app-ads.txt``. Read and mutated under
            ``results_lock``.
        buffer_ads (list[str]): Shared mutable buffer accumulating
            new ``ads.txt`` hits before they are flushed to disk.
        buffer_app_ads (list[str]): Shared mutable buffer
            accumulating new ``app-ads.txt`` hits before they are
            flushed to disk.
        flush_counter (list[int]): Single-element mutable list used
            as a shared counter. ``flush_counter[0]`` is incremented
            after every domain; when it reaches ``FLUSH_INTERVAL``,
            buffers are flushed and the counter resets to ``0``.
    Returns:
        None: The coroutine exits silently when the queue is empty
        or ``shutdown_event`` is set.
    """
    while not shutdown_event.is_set():
        try:
            domain = queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        # Strip protocol and path fragments that may be present in
        # the source domain list, keeping only the hostname.
        clean_domain = domain.replace('http://', '').replace('https://', '').split('/')[0]

        ads_url = f"https://{clean_domain}/ads.txt"
        app_ads_url = f"https://{clean_domain}/app-ads.txt"

        has_ads = await check_url(session, ads_url, clean_domain)
        has_app_ads = await check_url(session, app_ads_url, clean_domain)

        async with results_lock:
            if has_ads and clean_domain not in existing_ads:
                existing_ads.add(clean_domain)
                buffer_ads.append(clean_domain)

            if has_app_ads and clean_domain not in existing_app_ads:
                existing_app_ads.add(clean_domain)
                buffer_app_ads.append(clean_domain)

            # Periodically flush accumulated results to disk to limit
            # data loss if the process is killed unexpectedly.
            flush_counter[0] += 1
            if flush_counter[0] >= FLUSH_INTERVAL:
                flush_buffer(buffer_ads, buffer_app_ads)
                flush_counter[0] = 0

        pbar.update(1)
        queue.task_done()


async def main():
    """Orchestrate the full crawl lifecycle.
    Performs the following steps in order:
    1. Registers signal handlers for graceful shutdown.
    2. Discovers and reads all ``domains*.txt`` input files.
    3. Loads cached results from prior runs (``has_ads.txt``,
       ``has_app_ads.txt``) and subtracts fully-resolved domains.
    4. Populates an ``asyncio.Queue`` with the remaining domains.
    5. Spawns ``CONCURRENCY_LIMIT`` worker tasks sharing a single
       ``aiohttp.ClientSession`` with DNS caching and a bounded
       connection pool.
    6. Waits for the queue to drain, cancels workers, and flushes
       any remaining buffered results to disk (guaranteed by a
       ``try/finally`` block).
    7. Prints summary statistics.
    The function is intended to be the sole entry point via
    ``asyncio.run(main())``.
    Returns:
        None
    Raises:
        SystemExit: Indirectly, if the user sends SIGINT/SIGTERM.
            The signal handler sets ``shutdown_event`` which causes
            workers to stop, and the ``finally`` block ensures
            buffered data is persisted before exit.
    Examples:
        >>> # From the command line:
        >>> # $ python check_ads.py
        >>> #
        >>> # Programmatically:
        >>> import asyncio
        >>> asyncio.run(main())  # requires domains*.txt in cwd
    """
    # Mute the internal event loop exception logger to suppress
    # noisy tracebacks from cancelled coroutines and reset connections.
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda loop, context: None)

    # --- Graceful shutdown on Ctrl+C / kill ---
    def _signal_handler():
        """Set the cooperative shutdown flag on first signal receipt."""
        if not shutdown_event.is_set():
            print("\n\nShutting down gracefully, writing remaining results...")
            shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows does not support loop.add_signal_handler;
            # fall back to the synchronous signal module instead.
            signal.signal(sig, lambda s, f: _signal_handler())

    start_time = time.time()

    # --- Discover input files ---
    input_files = glob.glob('domains*.txt')

    if not input_files:
        print("Error: No files found matching the pattern domains*.txt")
        return

    print(f"Found input files: {len(input_files)}")

    # --- Load and deduplicate raw domain list ---
    all_domains = set()
    for file in input_files:
        with open(file, 'r', encoding='utf-8') as f:
            for line in f:
                d = line.strip()
                if d:
                    all_domains.add(d)

    # --- Load cached results from previous runs ---
    existing_ads = load_existing_results(OUTPUT_ADS)
    existing_app_ads = load_existing_results(OUTPUT_APP_ADS)

    # Domains present in BOTH output files have been fully resolved
    # on a prior run — no additional network requests are needed.
    already_done = existing_ads & existing_app_ads

    # Normalize raw entries (strip protocol/path) and remove
    # already-completed domains before entering the work queue.
    domains_to_check = [
        d.replace('http://', '').replace('https://', '').split('/')[0]
        for d in all_domains
    ]
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

    # --- Prepare async infrastructure ---
    queue = asyncio.Queue()
    for domain in domains_to_check:
        queue.put_nowait(domain)

    results_lock = asyncio.Lock()
    buffer_ads = []
    buffer_app_ads = []
    flush_counter = [0]  # mutable counter shared across workers

    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY_LIMIT + 20,  # headroom above worker count
        ttl_dns_cache=DNS_CACHE_TTL,
        ssl=False,
    )

    # --- Run workers ---
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
        # Guarantee that any domains discovered but not yet flushed
        # are written to disk, even after Ctrl+C or an exception.
        flush_buffer(buffer_ads, buffer_app_ads)

    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed:.2f} sec.")
    print(f"File {OUTPUT_ADS} contains unique records: {len(existing_ads)}")
    print(f"File {OUTPUT_APP_ADS} contains unique records: {len(existing_app_ads)}")


if __name__ == '__main__':
    # asyncio on Windows requires the Selector event loop policy;
    # the default Proactor policy does not support all socket operations
    # used by aiohttp.
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
