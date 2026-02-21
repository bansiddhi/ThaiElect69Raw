import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import time
import argparse
import logging
import re
from pathlib import Path
import gdown

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains

import concurrent.futures
import threading
import shutil
import uuid
import json
import io
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Lock for thread-safe logging or shared resources if needed
# gdown output might be messy, but that's acceptable.
print_lock = threading.Lock()

# Semaphore to limit concurrent Selenium instances
# Running too many headless browsers at once can cause resource exhaustion and GPU errors
selenium_semaphore = threading.Semaphore(1)

# Load environment variables
load_dotenv()

def get_drive_service():
    """Authenticates and returns a Google Drive API service."""
    try:
        service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
        if not service_account_json:
            return None
        
        # Parse the JSON string
        creds_dict = json.loads(service_account_json)
        
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        # Disable discovery cache to avoid 'file_cache is only supported with oauth2client<4.0.0' warning
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        return service
    except Exception as e:
        with print_lock:
            logger.error(f"Failed to initialize Google Drive API: {e}")
        return None

def download_from_drive_api(service, folder_url, output_dir, dry_run=False, force=False):
    """
    Downloads files from a Google Drive folder using the official API (Recursive).
    Returns True if successful, False if failed (e.g. permission denied).
    """
    try:
        # Extract folder ID
        # URL formats:
        # https://drive.google.com/drive/folders/FOLDER_ID
        # https://drive.google.com/drive/u/0/folders/FOLDER_ID
        match = re.search(r'/folders/([a-zA-Z0-9_-]+)', folder_url)
        if not match:
            with print_lock:
                logger.warning(f"Could not extract folder ID from {folder_url}")
            return False
            
        root_folder_id = match.group(1)
        
        # Recursive function to handle subfolders
        def process_folder(folder_id, current_output_dir):
            # Check if we can access the folder
            try:
                # Query files in the folder
                query = f"'{folder_id}' in parents and trashed = false"
                
                # Pagination loop
                page_token = None
                items_found = []
                
                while True:
                    results = service.files().list(
                        q=query,
                        pageSize=100, # Can go up to 1000
                        fields="nextPageToken, files(id, name, mimeType, size)",
                        pageToken=page_token
                    ).execute()
                    
                    files = results.get('files', [])
                    items_found.extend(files)
                    
                    page_token = results.get('nextPageToken')
                    if not page_token:
                        break
                
                # If it's the root folder and empty, we log a warning but return success (maybe it's just empty)
                # But if it's a subfolder, we just return.
                if not items_found:
                    return True
                    
                # Ensure output directory exists
                Path(current_output_dir).mkdir(exist_ok=True, parents=True)
                
                for item in items_found:
                    file_id = item.get('id')
                    name = item.get('name')
                    mime_type = item.get('mimeType')
                    
                    safe_name = sanitize_filename(name)
                    
                    if mime_type == 'application/vnd.google-apps.folder':
                        # It's a folder, recurse!
                        sub_dir = Path(current_output_dir) / safe_name
                        with print_lock:
                            logger.info(f"  📂 Entering subfolder: {safe_name}")
                        
                        if not process_folder(file_id, sub_dir):
                            return False # Propagate error
                            
                    else:
                        # It's a file, download!
                        if dry_run:
                            continue

                        filepath = Path(current_output_dir) / safe_name
                        
                        if not force and filepath.exists():
                            continue
                            
                        request = service.files().get_media(fileId=file_id)
                        
                        fh = io.FileIO(filepath, 'wb')
                        downloader = MediaIoBaseDownload(fh, request)
                        
                        done = False
                        while done is False:
                            status, done = downloader.next_chunk()
                        
                        with print_lock:
                            logger.info(f"  ✓ (API) Downloaded: {filepath.name}")
                            
                return True

            except HttpError as error:
                # If 404 or 403, it means the Service Account cannot access this folder.
                if error.resp.status in [403, 404]:
                    with print_lock:
                        logger.warning(f"Service Account cannot access folder {folder_id} (Error {error.resp.status}).")
                    return False
                else:
                    raise error

        # Start recursion from root
        with print_lock:
            logger.info(f"Drive API scanning {os.path.basename(output_dir)}...")
            
        success = process_folder(root_folder_id, output_dir)
        
        if not success:
             # Trigger fallback if the ROOT folder was inaccessible
             return False
             
        return True

    except Exception as e:
        with print_lock:
            logger.error(f"Drive API Error for {folder_url}: {e}")
        return False

def sanitize_filename(filename):
    """Sanitize the filename by removing invalid characters."""
    # Remove invalid characters for Windows/Linux filenames
    filename = re.sub(r'[\\/*?:"<>|]', '_', filename)
    # Ensure filename isn't too long
    return filename[:200]

def get_headless_driver():
    """Configures and returns a headless Chrome or Edge driver."""
    # Try Chrome first
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--silent")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        with print_lock:
            logger.info("Using Chrome browser")
        return driver
    except Exception as chrome_error:
        with print_lock:
            logger.warning(f"Chrome not available: {chrome_error}")
            logger.info("Trying Edge browser as fallback...")
        
        # Try Edge as fallback
        try:
            from selenium.webdriver.edge.options import Options as EdgeOptions
            from selenium.webdriver.edge.service import Service as EdgeService
            from webdriver_manager.microsoft import EdgeChromiumDriverManager
            
            edge_options = EdgeOptions()
            edge_options.add_argument("--headless")
            edge_options.add_argument("--no-sandbox")
            edge_options.add_argument("--disable-dev-shm-usage")
            edge_options.add_argument("--disable-gpu")
            edge_options.add_argument("--window-size=1920,1080")
            edge_options.add_argument("--log-level=3")
            edge_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            edge_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            service = EdgeService(EdgeChromiumDriverManager().install())
            driver = webdriver.Edge(service=service, options=edge_options)
            with print_lock:
                logger.info("Using Edge browser")
            return driver
        except Exception as edge_error:
            with print_lock:
                logger.error(f"Failed to initialize both Chrome and Edge: {edge_error}")
            return None

def fetch_page_source_selenium(url):
    """Fetch page source using Selenium in headless mode."""
    # Acquire semaphore to ensure we don't spawn too many browsers
    with selenium_semaphore:
        with print_lock:
            logger.info(f"Launching Headless Chrome to fetch {url}...")
        
        driver = get_headless_driver()
        if not driver:
            return None

        try:
            driver.get(url)
            
            # Wait for the content to load (specifically looking for table cells or drive links)
            with print_lock:
                logger.info("Waiting for page content to render...")
            try:
                # Wait up to 20 seconds for at least one table cell to appear
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, "td"))
                )
                with print_lock:
                    logger.info("Table content detected!")
            except Exception:
                with print_lock:
                    logger.warning("Timeout waiting for table cells. The page might differ or be empty.")
            
            # Give a little extra time for any lazy-loaded JS
            time.sleep(3)
            
            page_source = driver.page_source
            return page_source

        except Exception as e:
            with print_lock:
                logger.error(f"Selenium Error: {e}")
            return None
        finally:
            if driver:
                driver.quit()

def scrape_google_drive_folder_selenium(url):
    """
    Scrapes a Google Drive folder URL for file links using Selenium.
    Returns a list of dicts: [{'url': file_url, 'name': filename_hint}, ...]
    """
    # Acquire semaphore to ensure we don't spawn too many browsers
    with selenium_semaphore:
        with print_lock:
            logger.info(f"Using Selenium to scrape file list from Drive folder: {url}")
        
        driver = get_headless_driver()
        if not driver:
            return []

        files_found = []
        try:
            driver.get(url)
            time.sleep(5) # Initial load

            # Scroll to bottom to load all files
            last_height = driver.execute_script("return document.body.scrollHeight")
            # Limit scrolls to avoid infinite loops in weird cases
            for _ in range(10): 
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            # Find all links
            # Google Drive file links typically contain /file/d/
            # Folders contain /folders/
            elements = driver.find_elements(By.TAG_NAME, 'a')
            
            processed_ids = set()

            for elem in elements:
                href = elem.get_attribute('href')
                if not href:
                    continue
                    
                # Check if it looks like a file link
                # Format: https://drive.google.com/file/d/FILE_ID/view
                if '/file/d/' in href:
                    file_id_match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', href)
                    if file_id_match:
                        file_id = file_id_match.group(1)
                        if file_id in processed_ids:
                            continue
                        
                        processed_ids.add(file_id)
                        
                        # Try to get filename
                        text = elem.text.strip()
                        aria_label = elem.get_attribute('aria-label')
                        name = text or aria_label or f"file_{file_id}"
                        
                        # Construct direct download link (gdown handles ID, but we store URL)
                        # gdown.download uses the ID usually.
                        files_found.append({
                            'id': file_id,
                            'url': href,
                            'name': name
                        })

        except Exception as e:
            with print_lock:
                logger.error(f"Selenium Drive Scrape Error: {e}")
        finally:
            if driver:
                driver.quit()
                
        return files_found

def download_file(session, url, filepath, headers):
    """Download a file with retry logic."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = session.get(url, headers=headers, timeout=30, stream=True)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            file_size = filepath.stat().st_size
            logger.info(f"  ✓ Downloaded: {filepath.name} ({file_size:,} bytes)")
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"  ! Attempt {attempt + 1} failed for {filepath.name}: {e}. Retrying...")
                time.sleep(1)
            else:
                logger.error(f"  ✗ Failed to download {filepath.name} after {max_retries} attempts: {e}")
                if filepath.exists():
                    filepath.unlink()
                return False

def download_from_google_drive(url, output_dir, dry_run=False, error_log=None, force=False):
    """Download files from a Google Drive folder URL using gdown."""
    try:
        if dry_run:
            with print_lock:
                logger.info(f"[DRY RUN] Would download content from Google Drive URL: {url}")
            return

        # Check for completion marker
        marker_file = os.path.join(output_dir, '.completed')
        if not force and os.path.exists(marker_file):
            # Verify if there are actual files (excluding .completed)
            existing_files = [f for f in os.listdir(output_dir) if f != '.completed']
            
            # Simple heuristic: if we have more than 0 files, it's likely valid.
            # If user claims it's missing, they should check the folder content.
            # But if it's truly 0, we should re-try.
            if len(existing_files) > 0:
                with print_lock:
                    logger.info(f"Skipping {os.path.basename(output_dir)} - already completed ({len(existing_files)} files).")
                return
            else:
                with print_lock:
                    logger.warning(f"Found completion marker for {os.path.basename(output_dir)} but folder is empty. Re-downloading...")
                # Remove invalid marker
                try:
                    os.remove(marker_file)
                except Exception:
                    pass

        with print_lock:
            logger.info(f"Detected Google Drive URL for {os.path.basename(output_dir)}. Attempting to download folder...")
        
        # Ensure output directory exists
        Path(output_dir).mkdir(exist_ok=True, parents=True)

        # 1. Try Google Drive API first (if configured)
        drive_service = get_drive_service()
        if drive_service:
            if download_from_drive_api(drive_service, url, output_dir, dry_run=dry_run, force=force):
                with print_lock:
                    logger.info(f"Successfully downloaded using Google Drive API for {os.path.basename(output_dir)}.")
                # Write completion marker
                try:
                    with open(marker_file, 'w') as f:
                        f.write(f"Completed at {time.ctime()}")
                except Exception:
                    pass
                return

        # 2. Fallback to gdown if API not configured or failed
        
        # gdown.download_folder automatically handles folder structure
        # We pass the URL directly.
        # Use quiet=True to reduce spam in threads, we log manually
        try:
            files = gdown.download_folder(url, output=str(output_dir), quiet=True, use_cookies=False)
        except Exception as e:
            error_msg = str(e)
            # Check for known limits or parsing errors that imply scraping failure
            if "more than 50 files" in error_msg or "Expecting value" in error_msg:
                with print_lock:
                    logger.error(f"gdown failed: {error_msg}")
                    logger.error(f"Folder has more than 50 files. Please use Google Drive API by setting GOOGLE_SERVICE_ACCOUNT_JSON in .env file")
                    logger.error(f"Skipping {os.path.basename(output_dir)} - requires API access")
                
                # Don't try Selenium if we know it's a 50+ file folder
                # Just log the error and continue
                raise e
            else:
                raise e

        if files:
            with print_lock:
                logger.info(f"Successfully downloaded {len(files)} files for {os.path.basename(output_dir)}.")
            # Create marker file
            try:
                with open(marker_file, 'w') as f:
                    f.write(f"Completed at {time.ctime()}")
            except Exception as e:
                with print_lock:
                    logger.warning(f"Could not write completion marker: {e}")
        else:
            with print_lock:
                logger.warning(f"No files downloaded for {os.path.basename(output_dir)}. The folder might be empty or not public.")
            
    except Exception as e:
        with print_lock:
            logger.error(f"Google Drive download failed for {os.path.basename(output_dir)}: {e}")
        if error_log is not None:
            error_str = str(e)
            suggested_url = None
            # Extract browser access URL if present
            match = re.search(r'https://drive\.google\.com/uc\?id=[\w-]+', error_str)
            if match:
                suggested_url = match.group(0)
            
            # Append to shared list (list.append is thread-safe in CPython, but lock is safer)
            # However, we'll use a local list in the worker wrapper to be sure.
            error_log.append({
                'url': url,
                'province': os.path.basename(output_dir),
                'error_message': error_str,
                'suggested_url': suggested_url
            })

def clean_google_drive_url(url):
    """Fix common typos in Google Drive URLs found on the ECT website."""
    if not url:
        return None
        
    # Remove whitespace
    url = url.strip()
    
    # Fix domain typos
    url = re.sub(r'goo+gle', 'google', url) # gooogle -> google
    url = re.sub(r'goog\s+gle', 'google', url) # goog gle -> google
    url = re.sub(r'\.com\s+m', '.com', url) # .com m -> .com
    
    # Fix path typos
    url = re.sub(r'/ddrive/', '/drive/', url) # /ddrive/ -> /drive/
    url = re.sub(r'//drive/', '/drive/', url) # //drive/ -> /drive/
    
    return url

def _process_single_province_download(province_name, url, target_dir, dry_run, force):
    """Worker function to process a single province download."""
    local_error_log = []
    status = 'pending'
    
    try:
        download_from_google_drive(url, target_dir, dry_run=dry_run, error_log=local_error_log, force=force)
        
        # Check errors
        current_errors = [e for e in local_error_log if e['url'] == url]
        if current_errors:
            status = 'failed'
        elif dry_run:
            status = 'pending'
        else:
            status = 'completed'
            
    except Exception as e:
        status = 'failed'
        # Log generic error if not captured by download_from_google_drive
        if not local_error_log:
             local_error_log.append({
                'url': url,
                'province': province_name,
                'error_message': str(e),
                'suggested_url': None
            })

    return {
        'province': province_name,
        'status': status,
        'drive_url': url,
        'reason': None,
        'error_log': local_error_log
    }

def process_soup_for_drive_links(soup, base_output_dir, source_name="Unknown Source", dry_run=False, force=False, workers=4):
    """Parses BeautifulSoup object for Google Drive links and downloads them."""
    error_log = []
    scan_results = [] # List of all provinces found with their status
    
    # Iterate over table cells (td) to find provinces and their links
    cells = soup.find_all('td')
    logger.info(f"Found {len(cells)} table cells to scan in {source_name}.")
    
    found_count = 0
    missing_count = 0
    other_link_count = 0
    
    download_tasks = []

    for cell in cells:
        # Extract province name
        province_name = "Unknown_Province"
        text_content = cell.get_text(strip=True)
        
        # Skip empty cells
        if not text_content:
            continue

        # Try to find the province name in <p> tags
        p_tags = cell.find_all('p')
        if p_tags:
            for p in p_tags:
                t = p.get_text(strip=True)
                if t:
                    province_name = t
                    break
        
        if province_name == "Unknown_Province":
            province_name = text_content[:50] # Fallback
        
        province_name = sanitize_filename(province_name)
        
        # Check for Google Drive link (loose regex to catch typos)
        drive_link = cell.find('a', href=re.compile(r'drive\.go+.*gle\.com'))
        
        if drive_link:
            raw_url = drive_link['href']
            url = clean_google_drive_url(raw_url)
            
            target_dir = os.path.join(base_output_dir, province_name)
            
            # Record status logic for skipping is now inside the worker/download function
            # But we can do a quick check to log discovery
            # logger.info(f"Found link for {province_name}: {url}")
            
            download_tasks.append((province_name, url, target_dir))
            found_count += 1
            
        else:
            # Check for other links
            other_link = cell.find('a', href=True)
            if other_link:
                other_link_count += 1
                scan_results.append({
                    'province': province_name,
                    'status': 'non_drive_link',
                    'drive_url': other_link['href'],
                    'reason': 'Non-Drive Link'
                })
            else:
                missing_count += 1
                scan_results.append({
                    'province': province_name,
                    'status': 'no_link',
                    'drive_url': None,
                    'reason': 'No Link'
                })
    
    # Execute download tasks in parallel
    if download_tasks:
        logger.info(f"Starting {len(download_tasks)} download tasks with {workers} workers...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            future_to_province = {
                executor.submit(_process_single_province_download, p, u, t, dry_run, force): p 
                for p, u, t in download_tasks
            }
            
            for future in concurrent.futures.as_completed(future_to_province):
                province = future_to_province[future]
                try:
                    result = future.result()
                    
                    # Add to scan results (excluding internal error_log key)
                    scan_results.append({
                        'province': result['province'],
                        'status': result['status'],
                        'drive_url': result['drive_url'],
                        'reason': result['reason']
                    })
                    
                    # Add any errors to main log
                    if result['error_log']:
                        error_log.extend(result['error_log'])
                        
                except Exception as exc:
                    logger.error(f"Task for {province} generated an exception: {exc}")
                    scan_results.append({
                        'province': province,
                        'status': 'failed',
                        'drive_url': 'N/A', # Should have been captured
                        'reason': f"Executor Error: {exc}"
                    })

    logger.info("-" * 30)
    logger.info(f"Summary for {source_name}:")
    logger.info(f"  - Provinces with Drive links: {found_count}")
    logger.info(f"  - Provinces with OTHER links: {other_link_count}")
    logger.info(f"  - Provinces MISSING links:    {missing_count}")
            
    return error_log, found_count, scan_results

def process_local_html_file(file_path, base_output_dir, dry_run=False, force=False, workers=4):
    """Parses a local HTML file for Google Drive links and downloads them."""
    try:
        logger.info(f"Processing local file: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        soup = BeautifulSoup(content, 'html.parser')
        error_log, found_count, scan_results = process_soup_for_drive_links(soup, base_output_dir, source_name=file_path, dry_run=dry_run, force=force, workers=workers)
        return error_log, scan_results

    except Exception as e:
        logger.error(f"Failed to process local file {file_path}: {e}")
        return [], []

def download_files_from_ect(base_url, output_dir, extensions=None, dry_run=False, force=False, workers=4):
    """Main function to crawl and download files."""
    
    # Check if base_url is a local file
    if os.path.isfile(base_url):
        return process_local_html_file(base_url, output_dir, dry_run, force=force, workers=workers)

    # Check if it's a Google Drive URL
    if 'drive.google.com' in base_url:
        error_log = []
        download_from_google_drive(base_url, output_dir, dry_run, error_log=error_log, force=force)
        return error_log, []


    if extensions is None:
        extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.zip', '.rar', '.txt', '.jpg', '.jpeg', '.png', '.gif']
    
    # Create download directory
    download_path = Path(output_dir)
    if not dry_run:
        download_path.mkdir(exist_ok=True, parents=True)
    
    # Headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9,th;q=0.8',
        'Referer': 'https://www.google.com/'
    }
    
    session = requests.Session()
    
    try:
        logger.info(f"Accessing {base_url}...")
        response = session.get(base_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Try to find Google Drive links using the specific structure first
        drive_log, found_count, scan_results = process_soup_for_drive_links(soup, output_dir, source_name=base_url, dry_run=dry_run, force=force, workers=workers)
        
        if found_count == 0:
            logger.warning("No Google Drive links found with standard crawler. The website uses JavaScript/Dynamic content.")
            logger.info("Switching to Headless Browser (Selenium) to fetch dynamic content...")
            
            # Use Selenium to fetch the rendered page
            selenium_content = fetch_page_source_selenium(base_url)
            
            if selenium_content:
                logger.info("Successfully fetched content with Selenium. Processing...")
                soup_selenium = BeautifulSoup(selenium_content, 'html.parser')
                drive_log, found_count, scan_results = process_soup_for_drive_links(soup_selenium, output_dir, source_name=f"Selenium: {base_url}", dry_run=dry_run, force=force, workers=workers)
            
            # If still 0, check for local fallback
            if found_count == 0:
                logger.warning("Selenium also found 0 links (or failed). Checking for local fallback 'raw_element.txt'...")
                if os.path.exists("raw_element.txt"):
                     logger.info("Found 'raw_element.txt' in current directory. Using it as fallback...")
                     return process_local_html_file("raw_element.txt", output_dir, dry_run, force=force, workers=workers)

        # Also look for direct file links (legacy logic, might not be needed but keeping for safety)
        # ... legacy loop omitted for brevity in thought, but must be preserved ...
        # Actually I need to preserve the rest of the function.
        # Let's check where the replacement ends.
        
        links = soup.find_all('a', href=True)
        
        downloaded_count = 0
        failed_count = 0
        skipped_count = 0
        potential_files = 0
        
        # If we found Drive links, we probably don't need to do the generic crawl, but let's keep it safe
        # Only do generic crawl if extensions are specified OR if no drive links found
        if found_count == 0 or extensions:
            logger.info(f"Found {len(links)} total links. Scanning for downloadable files...")
        
        for link in links:
            href = link['href']
            
            if not href or href.startswith(('javascript:', '#', 'mailto:')):
                continue
            
            full_url = urljoin(base_url, href)
            parsed_url = urlparse(full_url)
            file_extension = Path(parsed_url.path).suffix.lower()
            
            if file_extension in extensions:
                potential_files += 1
                # Determine filename
                filename = Path(parsed_url.path).name
                if not filename:
                    link_text = link.get_text().strip()
                    if link_text:
                        filename = f"{link_text[:50]}{file_extension}"
                    else:
                        filename = f"file_{int(time.time())}{file_extension}"
                
                filename = sanitize_filename(filename)
                filepath = download_path / filename
                
                if dry_run:
                    logger.info(f"[DRY RUN] Would download: {filename} from {full_url}")
                    downloaded_count += 1
                    continue

                # Handle duplicates
                counter = 1
                original_stem = filepath.stem
                while filepath.exists():
                    filepath = download_path / f"{original_stem}_{counter}{file_extension}"
                    counter += 1
                
                logger.info(f"Downloading: {filename}...")
                if download_file(session, full_url, filepath, headers):
                    downloaded_count += 1
                    time.sleep(0.5)  # Respectful delay
                else:
                    failed_count += 1
            else:
                skipped_count += 1
        
        logger.info(f"\n{'='*50}")
        logger.info("Download Summary:")
        if dry_run:
            logger.info(f"Potential files found: {potential_files}")
            logger.info(f"Dry run complete. No files were downloaded.")
        else:
            logger.info(f"Successfully downloaded: {downloaded_count} files")
            logger.info(f"Failed downloads: {failed_count} files")
            logger.info(f"Output directory: {download_path.absolute()}")
        logger.info(f"{'='*50}")
        
        return drive_log, scan_results # Return the log from drive downloads
        
    except Exception as e:
        logger.error(f"An error occurred during crawling: {e}")
        return [], []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download files from ECT website or local HTML file containing Google Drive links")
    parser.add_argument("--url", default="https://www.ect.go.th/ect_th/th/election-2026", help="Target URL or local file path to crawl")
    parser.add_argument("--output", default="ect_election_2026_downloads", help="Directory to save downloads")
    parser.add_argument("--ext", nargs="+", help="File extensions to download (e.g. .pdf .xlsx)")
    parser.add_argument("--dry-run", action="store_true", help="List files without downloading")
    parser.add_argument("--force", action="store_true", help="Force re-download of already completed folders")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel downloads (default: 4)")
    
    args = parser.parse_args()
    
    download_files_from_ect(args.url, args.output, args.ext, args.dry_run, args.force, args.workers)