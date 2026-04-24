"""
SAP CDP Scraper - FINAL (Stable Extraction + Supabase Sync)
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import StaleElementReferenceException

from supabase import create_client
from dotenv import load_dotenv

import pandas as pd
import time
from datetime import datetime
from dateutil import parser
import logging
import os
import sys
sys.stdout.reconfigure(encoding='utf-8')

# ================== LOAD ENV ==================
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Supabase credentials missing")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ================== LOGGING ==================
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f"cdp_scraper_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# ================== SCRAPER ==================
class SAPCDPScraper:

    def __init__(self, url):
        self.url = url
        self.all_candidates = []
        self.seen_candidates = set()
        self.failed_indices = []

        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--log-level=3")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        driver_path = os.getenv("CHROMEDRIVER_PATH", "/usr/local/bin/chromedriver")
        self.driver = webdriver.Chrome(service=Service(driver_path), options=options)
        self.wait = WebDriverWait(self.driver, 15)

    # ================== LOGIN ==================
    def login(self):

        company_id = os.getenv("SAP_COMPANY_ID")
        agency_id = os.getenv("SAP_AGENCY_ID")
        email = os.getenv("SAP_EMAIL")
        password = os.getenv("SAP_PASSWORD")

        if not all([company_id, agency_id, email, password]):
            raise Exception("Missing SAP credentials")

        self.driver.get(self.url)
        time.sleep(2)

        self.wait.until(EC.presence_of_element_located((By.NAME, "companyId"))).send_keys(company_id)
        self.driver.find_element(By.CSS_SELECTOR, "button[id*='continueButton']").click()

        time.sleep(3)

        self.wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[contains(@placeholder,'Agency')]"))).send_keys(agency_id)
        self.driver.find_element(By.XPATH, "//input[contains(@placeholder,'Email')]").send_keys(email)
        self.driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(password)
        self.driver.find_element(By.CSS_SELECTOR, "button[id*='login']").click()

        time.sleep(5)

        if "login" in self.driver.current_url.lower():
            self.driver.save_screenshot("login_error.png")
            with open("login_error.html", "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            raise Exception("Login failed")

        logging.info("Logged in")
        self.switch_to_candidates()

    # ================== TAB SWITCH ==================
    def switch_to_candidates(self):
        """Robust SAP UI5 tab switch"""

        logging.info("Navigating to Candidates tab...")

        clicked = False

        for attempt in range(4):
            try:
                try:
                    elem = self.driver.find_element(By.ID, "__xmlview0--candidateListSplitView-text")
                    self.driver.execute_script("arguments[0].click();", elem)
                    time.sleep(2)
                except:
                    pass

                if "Search Candidate" in self.driver.page_source:
                    clicked = True
                    break

                try:
                    elem = self.driver.find_element(By.ID, "__xmlview0--candidateListSplitView")
                    self.driver.execute_script("arguments[0].click();", elem)
                    time.sleep(2)
                except:
                    pass

                if "Search Candidate" in self.driver.page_source:
                    clicked = True
                    break

                self.driver.execute_script("""
                    var tab = sap.ui.getCore().byId('__xmlview0--candidateListSplitView');
                    if (tab && tab.firePress) {
                        tab.firePress();
                    }
                """)
                time.sleep(2)

                if "Search Candidate" in self.driver.page_source:
                    clicked = True
                    break

                self.driver.execute_script("""
                    var tabBar = sap.ui.getCore().byId('__xmlview0--pageTabBar');
                    var tab = sap.ui.getCore().byId('__xmlview0--candidateListSplitView');
                    if (tabBar && tab) {
                        tabBar.setSelectedItem(tab);
                        tabBar.fireSelect({item: tab});
                    }
                """)
                time.sleep(2)

                if "Search Candidate" in self.driver.page_source:
                    clicked = True
                    break

            except Exception as e:
                logging.warning(f"Attempt {attempt + 1} failed: {e}")

        if not clicked:
            self.driver.save_screenshot("tab_switch_error.png")
            with open("tab_switch_error.html", "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            raise Exception("Could NOT switch to Candidates tab")

        logging.info("Successfully switched to Candidates tab")

    # ================== SCROLL ==================
    def scroll_and_load_all(self, limit=100):
        """Scroll only until required number of candidates are loaded"""

        logging.info(f"Loading up to {limit} candidates...")

        if "Search Candidate" not in self.driver.page_source:
            raise Exception("Not in Candidates tab")

        container = self.driver.find_element(By.ID, "__xmlview2--candidateMaster-cont")

        last_count = 0
        no_change_count = 0

        while True:
            self.driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollHeight",
                container
            )
            time.sleep(2)

            candidates = self.driver.find_elements(By.CSS_SELECTOR, "li.sapMCLI")
            current_count = len(candidates)

            logging.info(f"Loaded: {current_count}")

            if current_count >= limit:
                logging.info(f"Reached limit: {limit}")
                break

            if current_count == last_count:
                no_change_count += 1
                if no_change_count >= 3:
                    logging.info("No more candidates loading")
                    break
            else:
                no_change_count = 0

            last_count = current_count

        return min(current_count, limit)

    def normalize_phone(self, phone):
        if not phone:
            return ""
        return ''.join(filter(str.isdigit, str(phone)))

    def extract_candidate_details(self, idx):
        """Extract details with better error handling"""
        try:
            info = {}

            # Name
            try:
                name = self.driver.find_element(By.XPATH,
                                                "//span[contains(@class, 'sapUxAPObjectPageHeaderTitleText')] | //h2//span").text
                info['Name'] = self.clean_text(self.clean_name(name))
            except:
                info['Name'] = ""
                logging.warning(f"Could not extract name for candidate {idx}")

            # Email
            try:
                email = self.driver.find_element(By.XPATH,
                                                 "//div[@id[contains(., 'emailAddress')]]//span[contains(@id, '__text')]").text
                info['Email'] = self.clean(email).lower()
            except:
                info['Email'] = ""

            # Phone
            try:
                phone = self.driver.find_element(By.XPATH,
                                                 "//div[@id[contains(., 'phoneNumber')]]//span[contains(@id, '__text')]").text
                info['Phone'] = self.normalize_phone(phone)
            except:
                info['Phone'] = ""

            info['Created_On'] = self.get_field_by_label("Created On")
            info['Rights_Expire'] = self.get_field_by_label("Rights Expire")

            # Job applications
            jobs = []
            try:
                rows = self.driver.find_elements(By.XPATH,
                                                 "//tbody[contains(@id, 'candJobReqTable')]//tr[@role='row']")

                for row in rows:
                    try:
                        cells = row.find_elements(By.CSS_SELECTOR, "td[role='gridcell']")
                        if len(cells) >= 4:
                            job = info.copy()
                            job['Requisition_ID'] = cells[0].text.strip()
                            job['Job_Title'] = cells[1].text.strip()
                            job['Status'] = cells[2].text.strip()
                            job['Forwarded_On'] = cells[3].text.strip()
                            jobs.append(job)
                    except:
                        continue
            except:
                pass

            if not jobs:
                info['Requisition_ID'] = ""
                info['Job_Title'] = ""
                info['Status'] = ""
                info['Forwarded_On'] = ""
                jobs.append(info)

            if info['Email']:
                self.seen_candidates.add(info['Email'])

            return jobs

        except Exception as e:
            logging.error(f"Error extracting candidate {idx}: {e}")
            self.driver.save_screenshot(f"error_extract_{idx}.png")
            with open(f"error_extract_{idx}.html", "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            return []

    def clean_name(self, name):
        if not name:
            return ""

        name = str(name).strip()

        words = name.split()
        if len(words) % 2 == 0:
            half = len(words) // 2
            first_half = " ".join(words[:half])
            second_half = " ".join(words[half:])
            if first_half.lower() == second_half.lower():
                return first_half

        parts = name.split("  ")
        if len(parts) == 2 and parts[0].strip().lower() == parts[1].strip().lower():
            return parts[0].strip()

        return name

    def extract_all_loaded(self):
        """FINAL: Stable extraction using real UI click (SAP-safe)"""

        logging.info("Extracting all loaded candidates...")

        candidates = self.driver.find_elements(By.CSS_SELECTOR, "li.sapMCLI")
        limit = min(500, len(candidates))

        logging.info(f"Processing {limit} candidates...")

        extracted_count = 0
        skipped_count = 0

        for idx in range(limit):
            try:
                if (idx + 1) % 50 == 0 or idx == 0:
                    logging.info(
                        f"Processing {idx + 1}/{limit} (Extracted: {extracted_count}, Skipped: {skipped_count})"
                    )

                prev_email = ""
                try:
                    prev_email = self.driver.find_element(
                        By.XPATH,
                        "//div[contains(@id,'emailAddress')]//span[contains(@id,'__text')]"
                    ).text.strip()
                except:
                    pass

                self.driver.execute_script("""
                    var items = document.querySelectorAll("li.sapMCLI");
                    if (items.length > arguments[0]) {
                        var el = items[arguments[0]];
                        el.scrollIntoView({block: 'center'});
                        el.dispatchEvent(new MouseEvent('mousedown', {bubbles: true}));
                        el.dispatchEvent(new MouseEvent('mouseup', {bubbles: true}));
                        el.click();
                    }
                """, idx)

                new_email = prev_email

                for _ in range(15):
                    time.sleep(0.6)
                    try:
                        new_email = self.driver.find_element(
                            By.XPATH,
                            "//div[contains(@id,'emailAddress')]//span[contains(@id,'__text')]"
                        ).text.strip()

                        if new_email and new_email != prev_email:
                            break
                    except:
                        continue

                try:
                    new_name = self.driver.find_element(
                        By.XPATH,
                        "//span[contains(@class,'sapUxAPObjectPageHeaderTitleText')]"
                    ).text.strip()
                except:
                    new_name = ""

                logging.info(f"Selected candidate {idx + 1}: {new_name}")
                logging.info(f"Email: {new_email}")

                if idx == 0:
                    if not new_email:
                        logging.warning("First candidate has no email, skipping")
                        skipped_count += 1
                        continue
                else:
                    if not new_email or new_email == prev_email:
                        logging.warning(f"Candidate {idx + 1} did not change, skipping")
                        skipped_count += 1
                        continue

                details = self.extract_candidate_details(idx + 1)

                if details:
                    self.all_candidates.extend(details)
                    extracted_count += 1
                else:
                    logging.warning(f"No details extracted for candidate {idx + 1}")
                    self.failed_indices.append(idx + 1)
                    skipped_count += 1

            except Exception as e:
                logging.error(f"Error at candidate {idx + 1}: {e}")
                self.driver.save_screenshot(f"error_candidate_{idx + 1}.png")
                with open(f"error_candidate_{idx + 1}.html", "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source)
                self.failed_indices.append(idx + 1)
                skipped_count += 1
                continue

        logging.info(f"\n{'=' * 60}")
        logging.info("Extraction complete!")
        logging.info(f"  candidates processed: {limit}")
        logging.info(f"  Successfully extracted: {extracted_count}")
        logging.info(f"  Skipped/Failed: {skipped_count}")
        logging.info(f"  Total records: {len(self.all_candidates)}")
        logging.info(f"  Unique emails: {len(self.seen_candidates)}")

        if self.failed_indices:
            logging.warning(
                f"  Failed indices: {self.failed_indices[:10]}..."
                if len(self.failed_indices) > 10
                else f"  Failed indices: {self.failed_indices}"
            )

        logging.info(f"{'=' * 60}")

    def get_field_by_label(self, label):
        try:
            container = self.driver.find_element(
                By.XPATH,
                f"//*[contains(text(), '{label}')]"
            )
            value = container.find_element(
                By.XPATH,
                ".//following::span[contains(@id,'__text')][1]"
            )
            return value.text.strip()
        except Exception as e:
            logging.warning(f"{label} not found: {e}")
            return ""

    def retry_failed_candidates(self):
        """Retry extraction for failed candidates"""
        if not self.failed_indices:
            return

        logging.info(f"\nRetrying {len(self.failed_indices)} failed candidates...")

        for idx in self.failed_indices[:]:
            try:
                logging.info(f"Retrying candidate {idx}...")

                candidates = self.driver.find_elements(By.CSS_SELECTOR, "li.sapMCLI")
                if idx - 1 >= len(candidates):
                    continue

                candidates = self.driver.find_elements(By.CSS_SELECTOR, "li.sapMCLI")

                self.driver.execute_script("""
                    var list = document.querySelectorAll('li.sapMCLI');
                    if (list[arguments[0]]) {
                        list[arguments[0]].click();
                    }
                """, idx)
                time.sleep(0.8)

                prev_name = self.driver.find_element(
                    By.XPATH,
                    "//span[contains(@class,'sapUxAPObjectPageHeaderTitleText')]"
                ).text

                for _ in range(15):
                    time.sleep(0.7)
                    try:
                        new_name = self.driver.find_element(
                            By.XPATH,
                            "//span[contains(@class,'sapUxAPObjectPageHeaderTitleText')]"
                        ).text

                        new_email = self.driver.find_element(
                            By.XPATH,
                            "//div[contains(@id,'emailAddress')]//span[contains(@id,'__text')]"
                        ).text

                        if new_name != prev_name or new_email:
                            break
                    except:
                        continue

                details = self.extract_candidate_details(idx)
                if details and len(details) > 0:
                    self.all_candidates.extend(details)
                    self.failed_indices.remove(idx)
                    logging.info(f"✓ Successfully retried candidate {idx}")

            except Exception as e:
                logging.error(f"Retry failed for candidate {idx}: {e}")
                continue

        if self.failed_indices:
            logging.warning(f"Still failed after retry: {self.failed_indices}")
        else:
            logging.info("✓ All retries successful!")

    # ================== FILTER ==================
    def get_existing_keys(self):
        response = supabase.table("candidates") \
            .select("email, phone, jr_no") \
            .limit(10000) \
            .execute()

        existing = set()
        for row in response.data:
            key = (
                row.get("email") or "",
                row.get("phone") or "",
                row.get("jr_no") or ""
            )
            existing.add(key)

        logging.info(f"Loaded {len(existing)} existing records")
        return existing

    def filter_new_candidates(self, existing_keys):
        new_data = []
        for row in self.all_candidates:
            key = (
                row.get("Email") or "",
                row.get("Phone") or "",
                row.get("Requisition_ID") or ""
            )
            if key not in existing_keys:
                new_data.append(row)

        logging.info(f"New records: {len(new_data)}")
        return new_data

    # ================== DATE ==================
    def parse_date(self, val):
        try:
            if not val:
                return None
            return parser.parse(val).date().isoformat()
        except:
            return None

    # ================== SUPABASE ==================
    def upload_supabase(self, data):
        if not data:
            logging.warning("No new data")
            return

        logging.info(f"Uploading {len(data)} records...")
        data = self.deduplicate_data(data)

        batch_size = 25

        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            formatted = []

            for row in batch:
                if not row.get("Requisition_ID"):
                    continue
                formatted.append({
                    "name": self.clean_text(row.get("Name")),
                    "email": self.clean(row.get("Email")).lower(),
                    "phone": self.normalize_phone(row.get("Phone")),
                    "date": self.parse_date(row.get("Created_On")),
                    "rights_expire": self.parse_date(row.get("Rights_Expire")),
                    "forwarded_on": self.parse_date(row.get("Forwarded_On")),
                    "jr_no": self.clean(row.get("Requisition_ID")),
                    "job_title": self.clean_text(row.get("Job_Title")),
                    "status": self.clean_text(row.get("Status")),
                    "company": "BS",
                    "created_by": "bot",
                    "modified_by": "bot"
                })

            if not formatted:
                continue

            for attempt in range(3):
                try:
                    supabase.table("candidates").upsert(
                        formatted,
                        on_conflict="email,phone,requisition_id",
                        ignore_duplicates=True
                    ).execute()
                    print(f"Inserted {len(formatted)}")
                    break
                except Exception as e:
                    logging.error(f"Retry {attempt + 1} failed: {e}")
                    time.sleep(2)

    # ================== SAVE ==================
    def save_excel(self):
        df = pd.DataFrame(self.all_candidates)
        file = f"output_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        df.to_excel(file, index=False)
        logging.info(f"Saved {file}")

    def close(self):
        self.driver.quit()

    def clean(self, val):
        if val is None:
            return ""
        return str(val).strip()

    def clean_text(self, val):
        if val is None:
            return ""
        return " ".join(str(val).strip().split())

    def deduplicate_data(self, data):
        unique = {}
        for row in data:
            key = (
                self.clean(row.get("Email")).lower(),
                self.normalize_phone(row.get("Phone")),
                self.clean(row.get("Requisition_ID"))
            )
            unique[key] = row
        return list(unique.values())


# ================== MAIN ==================
def main():
    scraper = SAPCDPScraper("https://agencysvc44.sapsf.com/login")

    try:
        scraper.login()
        scraper.scroll_and_load_all(limit=100)
        scraper.extract_all_loaded()
        scraper.save_excel()

        new_data = scraper.deduplicate_data(scraper.all_candidates)
        scraper.upload_supabase(new_data)

        print(f"DONE: {len(new_data)} new records")

    finally:
        scraper.close()


if __name__ == "__main__":
    main()
