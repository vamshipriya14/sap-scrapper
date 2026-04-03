"""
SAP Job Listings Scraper
Scrapes all job listings from the Job Listings tab in SAP SuccessFactors Agency portal.
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from supabase import create_client
from dotenv import load_dotenv

import pandas as pd
import time
import re
from datetime import datetime
from dateutil import parser as dateutil_parser
import logging
import os
import sys

sys.stdout.reconfigure(encoding='utf-8')

# ================== LOAD ENV ==================
# Looks for .env next to the script (works locally and in GitHub Actions)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
Limit = 1000
if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Supabase credentials missing")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ================== LOGGING ==================
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f"job_listings_scraper_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)


# ================== SCRAPER ==================
class SAPJobListingsScraper:

    def __init__(self, url):
        self.url = url
        self.all_jobs = []
        self.seen_requisition_ids = set()
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
        agency_id  = os.getenv("SAP_AGENCY_ID")
        email      = os.getenv("SAP_EMAIL")
        password   = os.getenv("SAP_PASSWORD")

        if not all([company_id, agency_id, email, password]):
            raise Exception("Missing SAP credentials")

        self.driver.get(self.url)
        time.sleep(2)

        self.wait.until(EC.presence_of_element_located((By.NAME, "companyId"))).send_keys(company_id)
        self.driver.find_element(By.CSS_SELECTOR, "button[id*='continueButton']").click()
        time.sleep(3)

        self.wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[contains(@placeholder,'Agency')]"))
        ).send_keys(agency_id)
        self.driver.find_element(By.XPATH, "//input[contains(@placeholder,'Email')]").send_keys(email)
        self.driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(password)
        self.driver.find_element(By.CSS_SELECTOR, "button[id*='login']").click()
        time.sleep(5)

        if "login" in self.driver.current_url.lower():
            self.driver.save_screenshot("login_error.png")
            with open("login_error.html", "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            raise Exception("Login failed")

        logging.info("Logged in successfully")

    # ================== SCROLL & LOAD ==================
    def scroll_and_load_all(self, limit=Limit):
        logging.info(f"Loading up to {limit} job listings...")

        container = None
        for view_idx in range(5):
            try:
                container = self.driver.find_element(By.ID, f"__xmlview{view_idx}--jobMaster-cont")
                logging.info(f"Job list container: __xmlview{view_idx}--jobMaster-cont")
                break
            except Exception:
                pass

        if container is None:
            try:
                container = self.driver.find_element(
                    By.XPATH,
                    "//section[contains(@class,'sapMPageEnableScrolling')]"
                    "[.//li[contains(@class,'sapMLIB')]]"
                )
                logging.info("Job list container found via section fallback")
            except Exception:
                logging.warning("No scroll container found — will use window scroll")

        last_count   = 0
        no_change_ct = 0

        while True:
            if container:
                self.driver.execute_script(
                    "arguments[0].scrollTop = arguments[0].scrollHeight", container
                )
            else:
                self.driver.execute_script("window.scrollBy(0, 600);")
            time.sleep(2)

            jobs = self.driver.find_elements(By.CSS_SELECTOR, "li.sapMLIB")
            current_count = len(jobs)
            logging.info(f"Jobs loaded: {current_count}")

            if current_count >= limit:
                logging.info(f"Reached limit: {limit}")
                break

            if current_count == last_count:
                no_change_ct += 1
                if no_change_ct >= 3:
                    logging.info("No more jobs loading — done scrolling")
                    break
            else:
                no_change_ct = 0

            last_count = current_count

        return min(current_count, limit)

    # ================== RIGHT-PANEL TEXT ==================
    def _extract_right_panel_text(self):
        return self.driver.execute_script(
            """
            function visible(el) {
                if (!el) return false;
                var s = window.getComputedStyle(el);
                if (s.display === 'none' || s.visibility === 'hidden') return false;
                var r = el.getBoundingClientRect();
                return r.width > 0 && r.height > 0;
            }
            function normalizeBlock(t) {
                return String(t || '')
                    .replace(/\\r/g, '')
                    .replace(/[ \\t]+\\n/g, '\\n')
                    .replace(/\\n[ \\t]+/g, '\\n')
                    .trim();
            }
            function summarize(el) {
                var r = el.getBoundingClientRect();
                var raw = normalizeBlock(el.innerText);
                var text = raw.replace(/\\s+/g, ' ').trim();
                return {el:el, text:text, rawText:raw,
                        top:r.top, left:r.left, width:r.width, height:r.height,
                        area:r.width*r.height};
            }
            var panels = Array.from(document.querySelectorAll('section, div'))
                .filter(visible)
                .filter(function(el){ return !el.closest('li.sapMLIB'); })
                .map(summarize)
                .filter(function(item){
                    return item.text
                        && item.text.indexOf('Requisition ID') >= 0
                        && item.text.indexOf('Posting') >= 0
                        && item.text.indexOf('JOB DETAILS') >= 0;
                })
                .sort(function(a, b){
                    var aR = a.left > 250 ? 0 : 1;
                    var bR = b.left > 250 ? 0 : 1;
                    if (aR !== bR) return aR - bR;
                    if (a.area !== b.area) return a.area - b.area;
                    if (a.top  !== b.top)  return a.top  - b.top;
                    return a.left - b.left;
                });
            return panels.length ? panels[0].rawText : '';
            """
        )

    def _scroll_right_panel_and_get_job_details(self):
        right_panel = None
        try:
            right_panel = self.driver.find_element(
                By.XPATH,
                "//div[contains(@class,'sapUxAPObjectPageContent') or "
                "contains(@class,'sapMPageEnableScrolling')]"
                "[not(.//li[contains(@class,'sapMLIB')])]"
            )
        except Exception:
            pass

        if right_panel:
            for _ in range(10):
                self.driver.execute_script("arguments[0].scrollTop += 400;", right_panel)
                time.sleep(0.8)
            self.driver.execute_script("arguments[0].scrollTop = 0;", right_panel)
        else:
            for _ in range(8):
                self.driver.execute_script("window.scrollBy(0, 400);")
                time.sleep(0.8)
            self.driver.execute_script("window.scrollTo(0, 0);")

        time.sleep(1.0)
        return self._extract_right_panel_text()

    # ================== RECRUITER EMAIL ==================
    def _open_recruiter_contact_card(self, recruiter_name):
        result = self.driver.execute_script(
            """
            try {
                function visible(el) {
                    if (!el) return false;
                    var s = window.getComputedStyle(el);
                    if (s.display === 'none' || s.visibility === 'hidden') return false;
                    var r = el.getBoundingClientRect();
                    return r.width > 0 && r.height > 0;
                }
                function fireOrClick(node) {
                    while (node) {
                        if (node.id && window.sap && sap.ui && sap.ui.getCore) {
                            var ctrl = sap.ui.getCore().byId(node.id);
                            if (ctrl) {
                                if (ctrl.firePress) { ctrl.firePress(); return {ok:true, method:'firePress', id:node.id}; }
                                if (ctrl.ontap)    { ctrl.ontap({srcControl:ctrl}); return {ok:true, method:'ontap', id:node.id}; }
                            }
                        }
                        node = node.parentElement;
                    }
                    return null;
                }

                var icons = Array.from(document.querySelectorAll(
                    '[data-sap-ui*="quickViewDetails"], [id*="quickViewDetails"]'
                )).filter(visible);
                for (var i = 0; i < icons.length; i++) {
                    var r = fireOrClick(icons[i]);
                    if (r) return r;
                    icons[i].scrollIntoView({block:'center'});
                    ['mouseenter','mouseover','mousedown','mouseup','click'].forEach(function(evt){
                        icons[i].dispatchEvent(new MouseEvent(evt,{bubbles:true,cancelable:true,view:window}));
                    });
                    return {ok:true, method:'icon_mouse_events'};
                }

                if (window.sap && sap.ui && sap.ui.getCore) {
                    var elems = Object.values(sap.ui.getCore().mElements || {});
                    for (var j = 0; j < elems.length; j++) {
                        var c = elems[j];
                        if (!c || !c.getId) continue;
                        var cid = c.getId();
                        if (cid.indexOf('quickViewDetails') < 0 && cid.indexOf('quickview') < 0) continue;
                        var dom = c.getDomRef ? c.getDomRef() : null;
                        if (!visible(dom)) continue;
                        if (c.firePress) { c.firePress(); return {ok:true, method:'sap_core_firePress', id:cid}; }
                        if (dom) {
                            dom.scrollIntoView({block:'center'});
                            ['mousedown','mouseup','click'].forEach(function(e){
                                dom.dispatchEvent(new MouseEvent(e,{bubbles:true,cancelable:true,view:window}));
                            });
                            return {ok:true, method:'sap_core_dom', id:cid};
                        }
                    }
                }

                var rName = arguments[0] || '';
                var firstName = rName.split(' ')[0];
                if (firstName) {
                    var links = Array.from(document.querySelectorAll(
                        'a, [role="link"], span.sapMLnk, .sapMLnk, .sapMLink'
                    ))
                    .filter(visible)
                    .filter(function(el){ return !el.closest('li.sapMLIB'); });
                    for (var k = 0; k < links.length; k++) {
                        var txt = (links[k].innerText || links[k].textContent || '').trim();
                        if (txt && txt.indexOf(firstName) >= 0) {
                            var r2 = fireOrClick(links[k]);
                            if (r2) return r2;
                            links[k].scrollIntoView({block:'center'});
                            links[k].dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true,view:window}));
                            return {ok:true, method:'recruiter_link_click', text:txt};
                        }
                    }
                }

                return {ok:false, reason:'no_contact_trigger_found'};
            } catch(e) {
                return {ok:false, reason:e.message};
            }
            """,
            recruiter_name
        )
        logging.info(f"Contact card open result: {result}")

        for _ in range(8):
            try:
                popovers = self.driver.find_elements(
                    By.XPATH,
                    "//div[contains(@class,'sapMPopover') or contains(@class,'sapMQuickView') "
                    "or contains(@class,'sapMQuickViewCard')]"
                    "[not(contains(@style,'display: none'))]"
                )
                if any(p.is_displayed() for p in popovers):
                    return True
            except Exception:
                pass
            time.sleep(0.5)

        icons = self.driver.find_elements(
            By.CSS_SELECTOR, "[data-sap-ui*='quickViewDetails'], [id*='quickViewDetails']"
        )
        for icon in icons:
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", icon)
                time.sleep(0.3)
                ActionChains(self.driver).move_to_element(icon).pause(0.2).click().perform()
                time.sleep(1.2)
                popovers = self.driver.find_elements(
                    By.XPATH,
                    "//div[contains(@class,'sapMPopover') or contains(@class,'sapMQuickView')]"
                )
                if any(p.is_displayed() for p in popovers):
                    return True
            except Exception:
                continue

        return False

    def _extract_contact_from_popover(self):
        time.sleep(1.0)

        try:
            raw = self.driver.execute_script(
                """
                var selectors = [
                    '.sapMQuickViewCard',
                    '.sapMQuickView',
                    '.sapMPopover',
                    '.sapMPopup',
                    '[role="dialog"]'
                ];
                for (var i = 0; i < selectors.length; i++) {
                    var els = Array.from(document.querySelectorAll(selectors[i]));
                    for (var j = 0; j < els.length; j++) {
                        var el = els[j];
                        var s = window.getComputedStyle(el);
                        var r = el.getBoundingClientRect();
                        if (s.display !== 'none' && s.visibility !== 'hidden'
                                && r.width > 0 && r.height > 0) {
                            return el.innerText || '';
                        }
                    }
                }
                return '';
                """
            )
        except Exception as e:
            logging.warning(f"Popover text extraction error: {e}")
            return {"name": "", "email": "", "text": ""}

        if not raw:
            logging.warning("Popover: no visible popover/QuickViewCard found")
            return {"name": "", "email": "", "text": ""}

        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        logging.info(f"Popover lines: {lines}")

        email = ""
        name  = ""

        for i, line in enumerate(lines):
            if re.match(r"^email\s*address\s*:?\s*$", line, re.IGNORECASE):
                for j in range(i + 1, len(lines)):
                    m = re.search(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", lines[j], re.IGNORECASE)
                    if m:
                        email = m.group(0)
                        break
            m_inline = re.match(r"^email\s*address\s*:?\s*(.+)$", line, re.IGNORECASE)
            if m_inline and not email:
                m2 = re.search(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", m_inline.group(1), re.IGNORECASE)
                if m2:
                    email = m2.group(0)
            if not email:
                m3 = re.search(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", line, re.IGNORECASE)
                if m3:
                    email = m3.group(0)

        skip = re.compile(
            r"^(contact\s*card|employee\s*details|business\s*card|email|mobile|phone|"
            r"address|recruiter|agency|[A-Z0-9._%+\-]+@[A-Z0-9.\-]+)",
            re.IGNORECASE,
        )
        for line in lines:
            if skip.match(line):
                continue
            if len(line) > 2:
                name = line
                break

        return {"name": name, "email": email, "text": raw}

    # ================== PARSE PANEL TEXT ==================
    def _parse_panel_text(self, raw_text):
        if not raw_text:
            return {}

        def clean(s):
            return re.sub(r'\s+', ' ', str(s or '')).strip()

        LABEL_SET = {
            'REQUISITION ID', 'POSTING START DATE', 'POSTING END DATE',
            'RECRUITER', 'CLIENT RECRUITER', 'AGENCY CONTACT',
            'JOB DETAILS', 'JOB TITLE', 'AGENCY ACCESS'
        }

        def next_non_label(lines, idx):
            for j in range(idx + 1, len(lines)):
                v = clean(lines[j])
                if v and v.upper() not in LABEL_SET:
                    return v
            return ''

        parts = re.split(r'JOB DETAILS', raw_text, maxsplit=1, flags=re.IGNORECASE)
        header_raw      = parts[0]
        job_details_raw = parts[1] if len(parts) > 1 else ''

        header_lines = [clean(l) for l in header_raw.splitlines() if clean(l)]

        data = {
            'job_title':          '',
            'requisition_id':     '',
            'posting_start_date': '',
            'posting_end_date':   '',
            'recruiter_name':     '',
            'recruiter_email':    '',
            'job_details':        job_details_raw,
        }

        for i, line in enumerate(header_lines):
            U = line.upper()

            if U == 'REQUISITION ID':
                data['requisition_id'] = data['requisition_id'] or next_non_label(header_lines, i)
            elif re.match(r'^Requisition ID\s*:?\s*(.+)$', line, re.I):
                m = re.match(r'^Requisition ID\s*:?\s*(.+)$', line, re.I)
                data['requisition_id'] = data['requisition_id'] or clean(m.group(1))

            if U == 'POSTING START DATE':
                data['posting_start_date'] = data['posting_start_date'] or next_non_label(header_lines, i)
            elif re.match(r'^Posting Start Date\s*:?\s*(.+)$', line, re.I):
                m = re.match(r'^Posting Start Date\s*:?\s*(.+)$', line, re.I)
                data['posting_start_date'] = data['posting_start_date'] or clean(m.group(1))

            if U == 'POSTING END DATE':
                data['posting_end_date'] = data['posting_end_date'] or next_non_label(header_lines, i)
            elif re.match(r'^Posting End Date\s*:?\s*(.+)$', line, re.I):
                m = re.match(r'^Posting End Date\s*:?\s*(.+)$', line, re.I)
                data['posting_end_date'] = data['posting_end_date'] or clean(m.group(1))

            if re.match(r'^(RECRUITER|CLIENT RECRUITER|AGENCY CONTACT)$', U):
                data['recruiter_name'] = data['recruiter_name'] or next_non_label(header_lines, i)
            elif re.match(r'^(Recruiter|Client Recruiter|Agency Contact)\s*:?\s*(.+)$', line, re.I):
                m = re.match(r'^(Recruiter|Client Recruiter|Agency Contact)\s*:?\s*(.+)$', line, re.I)
                data['recruiter_name'] = data['recruiter_name'] or clean(m.group(2))

            em = re.search(r'[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}', line, re.I)
            if em and not data['recruiter_email']:
                data['recruiter_email'] = em.group(0)

        for line in header_lines:
            if line.upper() not in LABEL_SET:
                data['job_title'] = data['job_title'] or line
                break

        data['recruiter_name'] = re.sub(r'[\uE000-\uF8FF]', '', data['recruiter_name']).strip()
        return data

    # ================== EXTRACT ONE JOB ==================
    def extract_job_details(self, idx):
        try:
            prev_req_id = ''
            try:
                prev_req_id = self.driver.execute_script(
                    """
                    var nodes = document.querySelectorAll('section, div, span, bdi');
                    for (var i = 0; i < nodes.length; i++) {
                        var el = nodes[i];
                        if (el.closest && el.closest('li.sapMLIB')) continue;
                        var t = (el.innerText || '').replace(/\\s+/g,' ').trim();
                        var m = t.match(/Requisition ID\\s*:?\\s*(\\d+)/i);
                        if (m) return m[1];
                    }
                    return '';
                    """
                )
            except Exception:
                pass

            self.driver.execute_script(
                """
                var items = document.querySelectorAll('li.sapMLIB');
                if (items.length > arguments[0]) {
                    var el = items[arguments[0]];
                    el.scrollIntoView({block: 'center'});
                    el.dispatchEvent(new MouseEvent('mousedown', {bubbles: true}));
                    el.dispatchEvent(new MouseEvent('mouseup',   {bubbles: true}));
                    el.click();
                }
                """,
                idx
            )

            new_req_id = prev_req_id
            for _ in range(20):
                time.sleep(0.6)
                try:
                    new_req_id = self.driver.execute_script(
                        """
                        var nodes = document.querySelectorAll('section, div, span, bdi');
                        for (var i = 0; i < nodes.length; i++) {
                            var el = nodes[i];
                            if (el.closest && el.closest('li.sapMLIB')) continue;
                            var t = (el.innerText || '').replace(/\\s+/g,' ').trim();
                            var m = t.match(/Requisition ID\\s*:?\\s*(\\d+)/i);
                            if (m) return m[1];
                        }
                        return '';
                        """
                    )
                    if new_req_id and new_req_id != prev_req_id:
                        break
                except Exception:
                    continue

            logging.info(f"Job {idx + 1}: prev_req={prev_req_id!r} → new_req={new_req_id!r}")

            if idx > 0 and (not new_req_id or new_req_id == prev_req_id):
                logging.warning(f"Job {idx + 1}: panel did not update — skipping")
                return None

            raw_text = self._scroll_right_panel_and_get_job_details()
            if not raw_text:
                logging.warning(f"Job {idx + 1}: empty panel text")
                return None

            info = self._parse_panel_text(raw_text)

            if not info.get('requisition_id') and new_req_id:
                info['requisition_id'] = new_req_id

            recruiter_name  = info.get('recruiter_name', '')
            recruiter_email = info.get('recruiter_email', '')

            if not recruiter_email:
                try:
                    opened = self._open_recruiter_contact_card(recruiter_name)
                    if opened:
                        contact = self._extract_contact_from_popover()
                        if contact.get('email'):
                            recruiter_email = contact['email']
                            logging.info(f"  Email from popover: {recruiter_email}")
                        if contact.get('name') and not recruiter_name:
                            recruiter_name = contact['name']
                except Exception as e:
                    logging.warning(f"  Recruiter popover failed: {e}")

            info['recruiter_name']  = recruiter_name
            info['recruiter_email'] = recruiter_email

            if info.get('requisition_id'):
                self.seen_requisition_ids.add(info['requisition_id'])

            logging.info(
                f"  ✓ title={info.get('job_title')!r}  req={info.get('requisition_id')!r}  "
                f"start={info.get('posting_start_date')!r}  end={info.get('posting_end_date')!r}  "
                f"recruiter={info.get('recruiter_name')!r}  email={info.get('recruiter_email')!r}"
            )
            return info

        except Exception as e:
            logging.error(f"Error extracting job {idx + 1}: {e}")
            self.driver.save_screenshot(f"error_job_{idx + 1}.png")
            with open(f"error_job_{idx + 1}.html", "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            return None

    # ================== EXTRACT ALL ==================
    def extract_all_loaded(self):
        logging.info("Extracting all loaded job listings...")

        jobs = self.driver.find_elements(By.CSS_SELECTOR, "li.sapMLIB")
        limit = len(jobs)
        logging.info(f"Processing {limit} jobs...")

        extracted_count = 0
        skipped_count   = 0

        for idx in range(limit):
            try:
                if (idx + 1) % 25 == 0 or idx == 0:
                    logging.info(
                        f"Progress: {idx + 1}/{limit} "
                        f"(Extracted: {extracted_count}, Skipped: {skipped_count})"
                    )

                details = self.extract_job_details(idx)

                if details and details.get('requisition_id'):
                    self.all_jobs.append(details)
                    extracted_count += 1
                else:
                    logging.warning(f"No details/req_id for job index {idx + 1} — queued for retry")
                    self.failed_indices.append(idx)
                    skipped_count += 1

            except Exception as e:
                logging.error(f"Outer error at job {idx + 1}: {e}")
                self.failed_indices.append(idx)
                skipped_count += 1

        logging.info("=" * 60)
        logging.info("Extraction complete!")
        logging.info(f"  Jobs in list          : {limit}")
        logging.info(f"  Successfully extracted : {extracted_count}")
        logging.info(f"  Skipped/Failed        : {skipped_count}")
        logging.info(f"  Total records         : {len(self.all_jobs)}")
        logging.info(f"  Unique req IDs        : {len(self.seen_requisition_ids)}")
        if self.failed_indices:
            logging.warning(f"  Failed indices (first 20): {self.failed_indices[:20]}")
        logging.info("=" * 60)

    # ================== RETRY FAILED ==================
    def retry_failed(self):
        if not self.failed_indices:
            return
        logging.info(f"Retrying {len(self.failed_indices)} failed jobs...")
        for idx in list(self.failed_indices):
            try:
                details = self.extract_job_details(idx)
                if details and details.get('requisition_id'):
                    self.all_jobs.append(details)
                    self.failed_indices.remove(idx)
                    logging.info(f"✓ Retry succeeded for job index {idx}")
            except Exception as e:
                logging.error(f"Retry failed for job index {idx}: {e}")
        if self.failed_indices:
            logging.warning(f"Still failed after retry: {self.failed_indices}")
        else:
            logging.info("✓ All retries successful!")

    # ================== HELPERS ==================
    def parse_date(self, val):
        try:
            return dateutil_parser.parse(val).date().isoformat() if val else None
        except Exception:
            return None

    def clean(self, val):
        return str(val).strip() if val is not None else ""

    def clean_text(self, val):
        return " ".join(str(val).strip().split()) if val is not None else ""

    def deduplicate_data(self, data):
        unique = {}
        for row in data:
            key = self.clean(row.get("requisition_id"))
            if key:
                unique[key] = row
        return list(unique.values())

    # ================== EXISTING KEYS ==================
    def get_existing_requisition_ids(self):
        response = supabase.table("jr_master") \
            .select("requisition_id").limit(10000).execute()
        existing = {r.get("requisition_id") for r in response.data if r.get("requisition_id")}
        logging.info(f"Loaded {len(existing)} existing jr_master records")
        return existing

    def filter_new_jobs(self, existing_ids):
        new_data = [r for r in self.all_jobs
                    if self.clean(r.get("requisition_id")) not in existing_ids]
        logging.info(f"New job listings: {len(new_data)}")
        return new_data

    # ================== SUPABASE UPLOAD ==================
    def upload_supabase(self, data):
        if not data:
            logging.warning("No data to upload")
            return

        data = self.deduplicate_data(data)
        logging.info(f"Uploading {len(data)} job listing records...")

        batch_size = 25
        for i in range(0, len(data), batch_size):
            formatted = []
            for row in data[i:i + batch_size]:
                req_id = self.clean(row.get("requisition_id"))
                if not req_id:
                    continue
                formatted.append({
                    "requisition_id":     req_id,
                    "job_title":          self.clean_text(row.get("job_title")),
                    "posting_start_date": self.parse_date(row.get("posting_start_date")),
                    "posting_end_date":   self.parse_date(row.get("posting_end_date")),
                    "recruiter_name":     self.clean_text(row.get("recruiter_name")),
                    "recruiter_email":    self.clean(row.get("recruiter_email")).lower(),
                    "job_details":        row.get("job_details"),
                    "company":            "BS",
                    "created_by":         "bot",
                    "created_at":         datetime.utcnow().isoformat(),
                })
            if not formatted:
                continue
            for attempt in range(3):
                try:
                    supabase.table("jr_master").upsert(
                        formatted,
                        on_conflict="requisition_id",
                        ignore_duplicates=False
                    ).execute()
                    logging.info(f"Upserted batch {i // batch_size + 1}: {len(formatted)} records")
                    break
                except Exception as e:
                    logging.error(f"Upload attempt {attempt + 1} failed: {e}")
                    time.sleep(2)

    # ================== SAVE EXCEL ==================
    def save_excel(self):
        df   = pd.DataFrame(self.all_jobs)
        file = f"job_listings_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        df.to_excel(file, index=False)
        logging.info(f"Saved Excel: {file}")

    # ================== CLOSE ==================
    def close(self):
        self.driver.quit()


# ================== MAIN ==================
def main():
    scraper = SAPJobListingsScraper("https://agencysvc44.sapsf.com/login")

    try:
        scraper.login()

        total = scraper.scroll_and_load_all(limit=Limit)
        logging.info(f"Total jobs visible after scrolling: {total}")

        scraper.extract_all_loaded()

        if scraper.failed_indices:
            scraper.retry_failed()

        scraper.save_excel()

        new_data = scraper.deduplicate_data(scraper.all_jobs)
        #scraper.upload_supabase(new_data)

        logging.info(f"DONE: {len(new_data)} records upserted to jr_master table")

    finally:
        scraper.close()


if __name__ == "__main__":
    main()
