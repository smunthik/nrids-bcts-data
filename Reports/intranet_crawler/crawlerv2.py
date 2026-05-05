import os
import time
import glob
import json
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse
import hashlib

import requests
from requests.adapters import HTTPAdapter, Retry

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException

# -----------------------------
# Config
# -----------------------------
START_URL = "https://www2.gov.bc.ca/gov/content/sports-culture/recreation/fishing-hunting/hunting/frequently-asked-questions"
owning_business_area = "Fisheries Management Branch" 
MAX_DEPTH = 1

OUTPUT_FILE = "intranet_full_crawl.txt"
HIERARCHY_FILE = "intranet_url_tree.txt"

DOWNLOAD_FOLDER = "downloads"
METADATA_FOLDER = "metadata"

visited = set()

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs(METADATA_FOLDER, exist_ok=True)

# Optional: provide fallback support contacts by business area (if you want)
DEFAULT_SUPPORT_CONTACT_BY_AREA = {
    # "BCTS": {"email": "bcts.support@gov.bc.ca", "role": "Business Support"},
}

# -----------------------------
# Selenium setup
# -----------------------------
chrome_options = Options()
prefs = {
    "download.default_directory": os.path.abspath(DOWNLOAD_FOLDER),
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
    "plugins.always_open_pdf_externally": True,
    "profile.default_content_settings.popups": 0
}
chrome_options.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(options=chrome_options)
driver.get(START_URL)

input("🔐 Log in via IDIR (if needed), then press Enter to start crawling...")

# -----------------------------
# Helpers
# -----------------------------
def now_utc_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def clean_and_extract_text(html):
    soup = BeautifulSoup(html, "html.parser")
    return " ".join(soup.stripped_strings)


def safe_filename_from_url(url, max_length=80):
    parsed = urlparse(url)

    # Build short readable base
    base = (parsed.netloc + parsed.path).strip("/")
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", base)

    # Trim to max_length
    base = base[:max_length]

    # Add hash to guarantee uniqueness
    hash_part = hashlib.md5(url.encode()).hexdigest()[:10]

    return f"{base}__{hash_part}"


def normalize_date_to_yyyy_mm_dd(dt):
    if not dt:
        return None
    try:
        if isinstance(dt, str):
            # try parse common formats
            # YYYY-MM-DD
            m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", dt)
            if m:
                return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

            # Month name formats (e.g., April 13, 2026)
            try:
                from dateutil import parser as dateparser  # optional dependency
                parsed_dt = dateparser.parse(dt, fuzzy=True)
                return parsed_dt.date().isoformat()
            except Exception:
                return None

        # datetime
        return dt.date().isoformat()
    except Exception:
        return None

def find_last_modified_in_html(soup):
    """
    Try to find a last modified date from common meta tags or visible text.
    Returns YYYY-MM-DD or None.
    """
    # Meta tags commonly used
    meta_candidates = [
        ("meta", {"property": "article:modified_time"}),
        ("meta", {"name": "last-modified"}),
        ("meta", {"name": "Last-Modified"}),
        ("meta", {"property": "og:updated_time"}),
    ]
    for tagname, attrs in meta_candidates:
        tag = soup.find(tagname, attrs=attrs)
        if tag and tag.get("content"):
            return normalize_date_to_yyyy_mm_dd(tag["content"])

    # Look for visible "Last updated" patterns
    text = " ".join(soup.stripped_strings)
    patterns = [
        r"last\s+updated[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
        r"last\s+modified[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
        r"updated[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
        r"last\s+updated[:\s]+(\d{4}-\d{2}-\d{2})",
        r"last\s+modified[:\s]+(\d{4}-\d{2}-\d{2})",
    ]
    for p in patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            return normalize_date_to_yyyy_mm_dd(m.group(1))

    return None

def extract_document_title(soup):
    # Prefer OG title, then HTML title, then H1
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        return og["content"].strip()

    if soup.title and soup.title.string:
        return soup.title.string.strip()

    h1 = soup.find("h1")
    if h1:
        t = h1.get_text(" ", strip=True)
        if t:
            return t

    return None

def extract_support_contact(soup):
    """
    Try to find a support contact in the main body from mailto: links.
    Returns {"email": "...", "role": "..."} or None.
    """
    mailto = soup.select_one('a[href^="mailto:"]')
    if not mailto:
        return None

    href = mailto.get("href", "")
    email = href.replace("mailto:", "").split("?")[0].strip()
    if not email:
        return None

    # Try to infer role from nearby text (best-effort)
    role = None
    parent_text = mailto.parent.get_text(" ", strip=True) if mailto.parent else ""
    # e.g., "Contact: Business Support - someone@gov.bc.ca"
    m = re.search(r"(support|business support|helpdesk|service desk|contact)\b", parent_text, re.IGNORECASE)
    if m:
        role = m.group(1)

    return {"email": email, "role": role} if role else {"email": email, "role": None}

def extract_security_classification(soup):
    """
    Best-effort detection of sensitivity labels from visible text.
    Only returns a value if a clear label is found.
    """
    text = " ".join(soup.stripped_strings).lower()

    # look for explicit phrases like "Security classification: Internal"
    m = re.search(r"security\s+classification[:\s]+(public|internal|confidential|protected)", text)
    if m:
        return m.group(1).capitalize()

    # Otherwise: cautious scan for standalone labels near "classification"
    for label in ["public", "internal", "confidential", "protected"]:
        if re.search(rf"\b{label}\b", text) and "classification" in text:
            return label.capitalize()

    return None


def infer_topic_from_url(url):
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p][2:]  # skip domain and first path segment (often a section)
    parts = [part.split(".")[0] for part in parts]  # remove extensions

    if not parts:
        return None

    # Convert to readable format
    topics = [part.replace("-", " ").replace("_", " ").title() for part in parts]
    print(topics)
    return topics


def infer_document_status(url, soup):
    # Conservative: only mark Archived when clearly indicated
    if "archive" in url.lower() or "archived" in url.lower():
        return "Archived"
    text = " ".join(soup.stripped_strings).lower()
    if re.search(r"\barchived\b", text) and re.search(r"\b(status|document status)\b", text):
        return "Archived"
    return "Active"

def build_metadata_record(url, soup, owning_business_area, content_kind="page", file_last_modified=None):
    """
    Creates a metadata dict with required fields.
    Any unknown fields are set to None (or False for pi_reviewed).
    """
    document_title = extract_document_title(soup) if soup else None
    print(f"Extracted title: {document_title}")
    document_title = document_title.split('.')[0] if document_title else None  # remove file extension if present
    print(f"Cleaned title: {document_title}")
    topic_category = infer_topic_from_url(url) 

    last_modified = None
    if soup:
        last_modified = find_last_modified_in_html(soup)
    if not last_modified and file_last_modified:
        last_modified = normalize_date_to_yyyy_mm_dd(file_last_modified)

    security_classification = extract_security_classification(soup) if soup else None

    support_contact = extract_support_contact(soup) if soup else None
    if not support_contact and owning_business_area and owning_business_area in DEFAULT_SUPPORT_CONTACT_BY_AREA:
        support_contact = DEFAULT_SUPPORT_CONTACT_BY_AREA[owning_business_area]

    record = {
        "source_url": url,
        "document_title": document_title,
        "owning_business_area": owning_business_area,
        "topic_category": topic_category,
        "document_status": infer_document_status(url, soup) if soup else "Active",
        "security_classification": security_classification,
        "last_modified_at_source": last_modified,
        "ingested_at": now_utc_iso(),
        "support_contact": support_contact,
        "pi_reviewed": False,
        "pi_review_date": None,
        "pi_review_notes": None,
        "_content_kind": content_kind  # internal helper (optional)
    }
    return record

def write_metadata_json(record):
    fn = safe_filename_from_url(record["source_url"]) + ".metadata.json"
    path = os.path.join(METADATA_FOLDER, fn)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)
    return path

def is_internal_link(link, base_url, start_url):
    href = link.get("href")
    if not href or href.startswith("#"):
        return False

    try:
        full_url = urljoin(base_url, href)
        parsed_full = urlparse(full_url)
        parsed_base = urlparse(start_url)

        # Same domain OR same root domain (gov.bc.ca variants)
        if parsed_full.netloc == parsed_base.netloc:
            return True

        # allow other subdomains within the same parent domain (best-effort)
        if parsed_full.netloc.endswith("gov.bc.ca") and parsed_base.netloc.endswith("gov.bc.ca"):
            return True

        return False
    except Exception:
        return False

def is_downloadable_file(link):
    href = link.get("href", "")
    return href.lower().split("?")[0].endswith((".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"))

def wait_for_download_to_finish(existing_files, timeout=20):
    start = time.time()
    while time.time() - start < timeout:
        current_files = set(os.listdir(DOWNLOAD_FOLDER))
        downloading = glob.glob(os.path.join(DOWNLOAD_FOLDER, "*.crdownload"))
        new_files = current_files - existing_files
        if new_files and not downloading:
            return True
        time.sleep(1)
    return False

def requests_session_with_retries(max_retries=2):
    session = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def download_file_with_requests(file_url, indent, max_retries=2):
    try:
        print(f"📥 Downloading file: {file_url}")

        # Get cookies from Selenium for authenticated downloads
        cookies = {c["name"]: c["value"] for c in driver.get_cookies()}

        session = requests_session_with_retries(max_retries=max_retries)

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/117.0.0.0 Safari/537.36"
            )
        }

        # HEAD for Last-Modified if available
        file_last_modified = None
        try:
            h = session.head(file_url, cookies=cookies, headers=headers, verify=False, timeout=10, allow_redirects=True)
            lm = h.headers.get("Last-Modified")
            if lm:
                try:
                    file_last_modified = parsedate_to_datetime(lm)
                except Exception:
                    file_last_modified = None
        except Exception:
            pass

        r = session.get(file_url, cookies=cookies, headers=headers, stream=True, verify=False, timeout=20)
        r.raise_for_status()

        filename = os.path.basename(file_url.split("?")[0]) or "downloaded_file"
        filepath = os.path.join(DOWNLOAD_FOLDER, filename)

        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        print(f"✅ Saved: {filepath}")

        with open(HIERARCHY_FILE, "a", encoding="utf-8") as f:
            f.write(f"{'    ' * indent}📎 Downloaded: {file_url}\n")

        # Write metadata for the file (HTML soup not applicable)
        record = build_metadata_record(file_url, None, owning_business_area, content_kind="file", file_last_modified=file_last_modified)
        record["document_title"] = filename.split('.')[0] if filename else None  # for files, use filename if nothing else
        write_metadata_json(record)

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to download {file_url}: {e}")

# def wait_for_login_if_needed():
#     page_text = driver.page_source.lower()
#     if "idir" in page_text or "password" in page_text:
#         input("🔒 Login prompt detected. Please complete login and press Enter to continue...")

def extract_main_content(soup):
    for tag in soup.select("header, nav, footer, aside, .sidebar, .menu, .navbar"):
        tag.decompose()

    main = soup.find("main")
    if main:
        return main

    candidates = soup.find_all(
        lambda tag: tag.name in ["div", "section", "article"]
        and any(k in (tag.get("id", "") + " " + " ".join(tag.get("class", []))).lower()
                for k in ["content", "main", "body", "article"])
    )
    if candidates:
        return max(candidates, key=lambda t: len(t.get_text(strip=True)))

    return soup.body or soup

# -----------------------------
# Crawl
# -----------------------------
def crawl(url, depth):
    visited.add(url)
    print(f"URL: {url}, DEPTH: {depth}")
    if depth > MAX_DEPTH:
        return

    try:
        driver.get(url)
        # wait_for_login_if_needed()

        html = driver.page_source
        text = clean_and_extract_text(html)

        # Save extracted text (as before)
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(f"\n\n--- URL: {url} ---\n{text}")

        # Parse main content
        soup = BeautifulSoup(html, "html.parser")
        main_content = extract_main_content(soup)

        # Log hierarchy
        with open(HIERARCHY_FILE, "a", encoding="utf-8") as f:
            f.write(f"{'    ' * depth}- {url}\n")

        # Build and write metadata for the page
        record = build_metadata_record(url, soup, owning_business_area, content_kind="page")
        metadata_path = write_metadata_json(record)
        print(f"🧾 Metadata written: {metadata_path}")

        if not main_content:
            return

        # Traverse links in main content
        for link in main_content.find_all("a", href=True):
            href = link.get("href")
            if not href or href.startswith("#"):
                continue

            full_url = urljoin(url, href)

            # Downloadable files
            if is_downloadable_file(link) and full_url not in visited:
                download_file_with_requests(full_url, depth + 1)
                visited.add(full_url)

            # Internal links
            elif depth < MAX_DEPTH and is_internal_link(link, url, START_URL) and full_url not in visited:
                crawl(full_url, depth + 1)

            else:
                pass

    except WebDriverException as e:
        print(f"⚠️ Error loading {url}: {e}")

# -----------------------------
# Run
# -----------------------------
with open(HIERARCHY_FILE, "w", encoding="utf-8") as f:
    f.write(f"📂 Crawl Hierarchy for: {START_URL}\n\n")

crawl(START_URL, depth=0)
driver.quit()

print("\n✅ Done.")
print(f"📄 Text saved to: '{OUTPUT_FILE}'")
print(f"🗂️ URL hierarchy saved to: '{HIERARCHY_FILE}'")
print(f"📎 Files downloaded to: '{DOWNLOAD_FOLDER}/'")
print(f"🧾 Metadata JSON saved to: '{METADATA_FOLDER}/'")
