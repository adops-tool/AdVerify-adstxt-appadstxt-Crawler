# Ads.txt & App-ads.txt Async Checker

Fast, asynchronous Python script designed to verify the presence of `ads.txt` and `app-ads.txt` files across hundreds of thousands of domains. 

---

### Requirements
* Python 3.8+
* Install required libraries using command prompt or terminal:
  ```bash
  pip install aiohttp tqdm
  ```

### How to Use
1. Place your lists of domains in the same folder as the script.
2. Ensure the text files start with the word `domains` and end with `.txt` (e.g., `domains1.txt`, `domains_new.txt`).
3. Run the script:
   ```bash
   python adstxtcrawler.py
   ```
4. The script will generate two files: `has_ads.txt` and `has_app_ads.txt` containing the domains that have the respective files.

### Key Features & Nuances
* **Multiple Input Files:** The script automatically finds and merges all files matching the `domains*.txt` pattern.
* **Auto-Deduplication:** Duplicate domains within your input lists are automatically removed before the check begins.
* **Randomization:** The combined list is shuffled randomly before processing to avoid hitting the same infrastructure sequentially.
* **Safe Resuming (No Duplicates):** If the script is stopped and restarted, it reads the existing `has_ads.txt` and `has_app_ads.txt` files. It will **not** duplicate entries and will skip network requests for domains that are already verified.
* **Streaming Write:** Results are written to the disk instantly upon discovery, preventing data loss in case of a crash or manual interruption.
* **Silent Mode:** Internal network errors (like DNS lookup failures for dead sites) are suppressed to keep the console clean, displaying only the progress bar.
