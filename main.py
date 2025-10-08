# scrape_delaware_daily.py
import asyncio
import json
import os
import traceback
from datetime import datetime, timedelta
from pathlib import Path
import time
from collections import defaultdict
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from googleapiclient.discovery import build
from google.oauth2 import service_account

# -----------------------------
# Google Sheets Configuration
# -----------------------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")

# -----------------------------
# Google Sheets Helpers
# -----------------------------
def load_service_account_info():
    file_env = os.environ.get("GOOGLE_CREDENTIALS_FILE")
    if file_env:
        if os.path.exists(file_env):
            with open(file_env, "r", encoding="utf-8") as fh:
                return json.load(fh)
        raise ValueError(f"GOOGLE_CREDENTIALS_FILE set but not found: {file_env}")

    creds_raw = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_raw:
        raise ValueError("GOOGLE_CREDENTIALS or GOOGLE_CREDENTIALS_FILE is required.")

    txt = creds_raw.strip()
    if txt.startswith("{"):
        return json.loads(txt)

    if os.path.exists(creds_raw):
        with open(creds_raw, "r", encoding="utf-8") as fh:
            return json.load(fh)

    raise ValueError("GOOGLE_CREDENTIALS is neither valid JSON nor an existing file path.")

def sheets_client():
    info = load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds).spreadsheets()

def get_last_scraped_date(svc, spreadsheet_id):
    """Get the most recent filing date from all sheets in Google Sheets."""
    try:
        # Get all sheets in the spreadsheet
        spreadsheet = svc.get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get('sheets', [])
        
        max_date = None
        
        for sheet in sheets:
            sheet_name = sheet['properties']['title']
            # Skip non-date formatted sheets
            if not any(char.isdigit() for char in sheet_name):
                continue
                
            print(f"🔍 Checking sheet: {sheet_name}")
            
            try:
                # Get dates from column B (Filing Date)
                res = svc.values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{sheet_name}'!B2:B"
                ).execute()
                
                vals = [r[0] for r in res.get("values", []) if r]
                
                for date_str in vals:
                    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
                        try:
                            date_obj = datetime.strptime(date_str, fmt).date()
                            if max_date is None or date_obj > max_date:
                                max_date = date_obj
                            break
                        except ValueError:
                            continue
                            
            except Exception as e:
                print(f"⚠️ Could not read dates from sheet {sheet_name}: {e}")
                continue
        
        if max_date:
            print(f"📅 Last scraped date found: {max_date}")
        else:
            print("📅 No previous dates found in sheets")
            
        return max_date

    except Exception as e:
        print(f"❌ Error reading last date from Google Sheets: {e}")
        return None

def append_to_google_sheets(svc, spreadsheet_id, records):
    """Append new records to appropriate monthly sheets in Google Sheets."""
    if not records:
        print("📭 No new records to append to Google Sheets")
        return

    # Group records by month
    by_month = defaultdict(list)
    for record in records:
        filing_date = record.get("filing_date", "")
        month_key = "Unknown"
        
        # Parse date to determine sheet name
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
            try:
                dt_obj = datetime.strptime(filing_date, fmt)
                month_key = dt_obj.strftime("%Y-%m")
                break
            except ValueError:
                continue
        
        by_month[month_key].append(record)

    total_appended = 0
    
    for month, month_records in by_month.items():
        sheet_name = month
        print(f"📊 Processing {len(month_records)} records for sheet: {sheet_name}")
        
        # Ensure sheet exists
        ensure_sheet_exists(svc, spreadsheet_id, sheet_name)
        
        # Prepare data for Google Sheets
        values = []
        for record in month_records:
            values.append([
                record.get("case_file_no", ""),
                record.get("filing_date", ""),
                record.get("caseFileNum", ""),
                record.get("caseFileId", ""),
                record.get("decedent_address", ""),
                record.get("representative_name", ""),
                record.get("representative_address", "")
            ])

        try:
            # Append data to the sheet
            result = svc.values().append(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_name}'!A:G",
                valueInputOption="USER_ENTERED",
                body={"values": values}
            ).execute()
            
            print(f"✅ Appended {len(month_records)} records to sheet: {sheet_name}")
            total_appended += len(month_records)
            
        except Exception as e:
            print(f"❌ Error appending to sheet {sheet_name}: {e}")

    print(f"🎯 Total records appended to Google Sheets: {total_appended}")
    return total_appended

def ensure_sheet_exists(svc, spreadsheet_id, sheet_name):
    """Create a sheet if missing with proper headers."""
    try:
        # Get existing sheets
        spreadsheet = svc.get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get('sheets', [])
        existing_titles = [sheet['properties']['title'] for sheet in sheets]
        
        if sheet_name not in existing_titles:
            # Create new sheet
            body = {
                "requests": [{
                    "addSheet": {
                        "properties": {
                            "title": sheet_name
                        }
                    }
                }]
            }
            svc.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
            
            # Add headers
            headers = [
                "Case File No", "Filing Date", "Case File Num", "Case File ID", 
                "Decedent Address", "Representative Name", "Representative Address"
            ]
            svc.values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_name}'!A1:G1",
                valueInputOption="USER_ENTERED",
                body={"values": [headers]}
            ).execute()
            
            print(f"✅ Created new sheet: {sheet_name}")
            
    except Exception as e:
        print(f"⚠️ Error ensuring sheet exists: {e}")

class DelawareScraper:
    def __init__(self, page, browser=None, context=None,
                 base_url: str = "https://delcorowonlineservices.co.delaware.pa.us/countyweb/loginDisplay.action?countyname=DelawarePA&errormsg=error.sessiontimeout"):  # Fixed URL
        self.page = page
        self.browser = browser
        self.context = context
        self.base_url = base_url

    # === ADD FRAME LOCATOR METHODS FROM WORKING CODE ===
    def _res_list_loc(self):
        # bodyframe -> resultFrame -> resultListFrame
        return (
            self.page
            .frame_locator("iframe[name='bodyframe']")
            .frame_locator("iframe[name='resultFrame']")
            .frame_locator("iframe[name='resultListFrame']")
        )

    def _res_subnav_middle_loc(self):
        # middle subnav with pageNumber input and Go button
        return (
            self.page
            .frame_locator("iframe[name='bodyframe']")
            .frame_locator("iframe[name='resultFrame']")
            .frame_locator("iframe[src*='navbar.do?page=search.resultNav.middle']")
        )

    def _doc_loc(self):
        # bodyframe -> documentFrame -> docInfoFrame
        return (
            self.page
            .frame_locator("iframe[name='bodyframe']")
            .frame_locator("iframe[name='documentFrame']")
            .frame_locator("iframe[name='docInfoFrame']")
        )

    def _tabs_loc(self):
        return self._doc_loc().frame_locator("iframe[name='tabs']")

    async def ensure_decedent_tab(self):
        """Ensure the decedent panel is visible."""
        try:
            if await self._doc_loc().locator("text=Estate Info").first.is_visible():
                return
        except:
            pass
        
        tabs = self._tabs_loc()
        for label in ["Decedent & Estate Info", "Decedent", "Estate Info"]:
            try:
                await tabs.locator(f"span.tabs-title:has-text('{label}')").first.click(timeout=2000)
                await asyncio.sleep(0.6)
                return
            except:
                continue

    async def wait_for_frame_by_url_fragment(self, url_fragment: str, timeout: int = 60):
        """Wait for frame with URL containing fragment."""
        print(f"⏳ Waiting for frame with URL containing '{url_fragment}'...")
        for i in range(timeout):
            for f in self.page.frames:
                if f.url and url_fragment in f.url:
                    print(f"✅ Found frame: {f.url}")
                    return f
            await asyncio.sleep(1)
        raise PlaywrightTimeoutError(f"Frame with URL fragment '{url_fragment}' not found within {timeout}s")

    async def wait_for_frame_by_name(self, name: str, timeout: float = 30000, parent_frame=None):
        """Wait for frame with specific name."""
        start_time = time.time()
        while (time.time() - start_time) * 1000 < timeout:
            frames = parent_frame.child_frames if parent_frame else self.page.frames
            for frame in frames:
                if frame.name == name:
                    return frame
            await asyncio.sleep(0.1)
        raise PlaywrightTimeoutError(f"Frame with name '{name}' not found within {timeout}ms")

    # === FIXED NAVIGATION METHODS FROM WORKING CODE ===
    async def goto_login(self, retries: int = 3):
        """Go to login page and click 'Login as Guest'."""
        for attempt in range(1, retries + 1):
            try:
                print(f"🔐 Login attempt {attempt}/{retries}")
                await self.page.goto(self.base_url, timeout=60000)
                await self.page.wait_for_timeout(1500)
                
                possible_selectors = [
                    "input[value=' Login as Guest ']",
                    "input[value='Login as Guest']",
                    "input[type='button'][value*='Guest']",
                    "text='Login as Guest'"
                ]
                
                clicked = False
                for sel in possible_selectors:
                    try:
                        await self.page.wait_for_selector(sel, timeout=5000)
                        await self.page.locator(sel).click()
                        clicked = True
                        break
                    except PlaywrightTimeoutError:
                        continue
                
                if not clicked:
                    raise PlaywrightTimeoutError("Could not find 'Login as Guest' button")
                
                await self.page.wait_for_url("**/main.jsp?countyname=DelawarePA", timeout=60000)
                print("✅ Logged in as Guest")
                return
                
            except Exception as e:
                print(f"❌ Login attempt {attempt} failed: {e}")
                if attempt == retries:
                    raise
                await asyncio.sleep(3)

    async def accept_terms(self, retries: int = 3):
        """Accept terms and conditions."""
        for attempt in range(1, retries + 1):
            try:
                await asyncio.sleep(1)
                iframe_selectors = [
                    "iframe[name='bodyframe']",
                    "iframe#bodyframe",
                    "iframe[src*='blank.jsp']",
                    "iframe",
                ]
                
                clicked = False
                for ifsel in iframe_selectors:
                    try:
                        frame_locator = self.page.frame_locator(ifsel)
                        accept = frame_locator.locator("#accept")
                        await accept.wait_for(state="visible", timeout=10000)
                        await accept.click()
                        clicked = True
                        break
                    except PlaywrightTimeoutError:
                        continue

                if not clicked:
                    for f in self.page.frames:
                        try:
                            handle = await f.query_selector("#accept")
                            if handle:
                                await handle.click()
                                clicked = True
                                break
                        except Exception:
                            continue

                if not clicked:
                    raise PlaywrightTimeoutError("Could not locate Accept button")

                print(f"✅ Accepted terms (attempt {attempt})")
                return

            except Exception as e:
                print(f"❌ Accept terms attempt {attempt} failed: {e}")
                if attempt == retries:
                    raise
                await asyncio.sleep(2)

    async def click_search_public_records(self, retries: int = 3):
        """Click Search Public Records."""
        for attempt in range(1, retries + 1):
            try:
                await self.page.wait_for_timeout(1000)
                frame_locator = self.page.frame_locator("iframe[name='bodyframe']")
                selector = "#datagrid-row-r1-2-0"
                await frame_locator.locator(selector).wait_for(state="visible", timeout=20000)
                await frame_locator.locator(selector).click()
                print("✅ Clicked 'Search Public Records'")
                return
            except Exception as e:
                print(f"❌ Click search attempt {attempt} failed: {e}")
                if attempt == retries:
                    raise
                await asyncio.sleep(2)

    async def enter_filing_dates(self, from_date: str, to_date: str, retries: int = 3):
        """Enter filing date range - FIXED VERSION."""
        print(f"📅 Setting date range: {from_date} to {to_date}")

        for attempt in range(1, retries + 1):
            try:
                print("⏳ Waiting for dynamic frames to load...")
                await asyncio.sleep(3)

                criteria_frame = None
                try:
                    criteria_frame = await self.wait_for_frame_by_url_fragment("dynCriteria.do", timeout=30)
                except PlaywrightTimeoutError:
                    try:
                        criteria_frame = await self.wait_for_frame_by_url_fragment("blank.jsp", timeout=10)
                    except PlaywrightTimeoutError:
                        criteria_frame = None

                if not criteria_frame:
                    raise PlaywrightTimeoutError("Could not find criteria frame (dynCriteria.do or blank.jsp)")

                await criteria_frame.wait_for_load_state("domcontentloaded", timeout=15000)
                await asyncio.sleep(1)

                el = await criteria_frame.wait_for_selector("#elemDateRange", timeout=15000)
                if not el:
                    raise PlaywrightTimeoutError("Date range container '#elemDateRange' not found")

                from_input = await criteria_frame.wait_for_selector("#_easyui_textbox_input7", timeout=10000)
                to_input = await criteria_frame.wait_for_selector("#_easyui_textbox_input8", timeout=10000)

                await from_input.fill("")
                await from_input.type(from_date)
                await to_input.fill("")
                await to_input.type(to_date)

                print(f"✅ Date range entered: {from_date} to {to_date}")
                return

            except Exception as e:
                print(f"❌ Enter dates attempt {attempt} failed: {e}")
                if attempt == retries:
                    raise
                await asyncio.sleep(2)

    async def click_search_button(self, retries: int = 3):
        """Click search button - FIXED VERSION."""
        for attempt in range(1, retries + 1):
            try:
                body_frame = await self.wait_for_frame_by_name("bodyframe", timeout=30000)
                dyn_search_frame = await self.wait_for_frame_by_name("dynSearchFrame", timeout=30000, parent_frame=body_frame)
                await dyn_search_frame.wait_for_load_state("domcontentloaded", timeout=15000)
                search_selector = "a[onclick*='executeSearchCommand'][onclick*='search']"
                await dyn_search_frame.wait_for_selector(search_selector, timeout=15000)
                await dyn_search_frame.eval_on_selector(search_selector, "el => el.click()")
                print("✅ Clicked 'Search Public Records' button")
                return True
            except Exception as e:
                print(f"❌ Search button attempt {attempt} failed: {e}")
                if attempt == retries:
                    print("❌ All attempts to click search button failed")
                    return False
                await asyncio.sleep(2)

    # === FIXED EXTRACTION METHODS FROM WORKING CODE ===
    async def extract_decedent_info_atomic(self):
        """Extract decedent information with robust ZIP handling."""
        loc = self._doc_loc()
        await self.ensure_decedent_tab()

        # Extract filing date
        filing_date = ""
        for _ in range(30):
            try:
                fd_cell = loc.locator("#fieldFILING_DATEspan").locator("xpath=ancestor::tr/td[3]").first
                txt = await fd_cell.text_content()
                if txt and txt.strip():
                    filing_date = txt.strip()
                    break
            except:
                pass
            await asyncio.sleep(0.5)

        # Extract case file number
        case_file_no = ""
        try:
            cf_cell = loc.locator("#fieldCASENUMBERspan").locator("xpath=ancestor::tr/td[3]").first
            case_file_no = (await cf_cell.text_content() or "").strip()
        except:
            pass

        # Extract address components
        addr = city = state = zipc = ""

        # Street Address
        try:
            addr_cell = loc.locator("#fcaddrCORESPONDENT_ADDRESSspan").locator("xpath=ancestor::tr/td[3]").first
            addr = (await addr_cell.text_content() or "").strip()
        except:
            pass

        # City
        try:
            city_cell = loc.locator("#fccityCORESPONDENT_ADDRESSspan").locator("xpath=ancestor::tr/td[3]").first
            city = (await city_cell.text_content() or "").strip()
        except:
            pass

        # State + ZIP (nested table)
        try:
            state_zip_cell = loc.locator("#fcstateCORESPONDENT_ADDRESSspan").locator("xpath=ancestor::tr/td[3]").first
            state_locator = state_zip_cell.locator("table tr td").nth(0)
            zip_locator = state_zip_cell.locator("table tr td").nth(2)
            state = ((await state_locator.text_content()) or "").strip()
            zipc = ((await zip_locator.text_content()) or "").strip()
        except Exception as e:
            print(f"⚠️ Could not extract state/zip (primary): {e}")

        # Fallback ZIP extraction
        if not zipc:
            try:
                zip_fallback = loc.locator("xpath=//span[@id='fczipCORESPONDENT_ADDRESSspan']/ancestor::td[1]/following-sibling::td[1]").first
                zipc = ((await zip_fallback.text_content()) or "").strip()
            except Exception as e:
                print(f"📍 Fallback zip extraction failed: {e}")

        # Combine address components
        parts = [p for p in [addr, city, state, zipc] if p]
        decedent_address = ", ".join(parts) if parts else ""

        print(f"🏠 Final address: '{decedent_address}'")

        return {
            "case_file_no": case_file_no,
            "filing_date": filing_date,
            "decedent_address": decedent_address
        }

    async def extract_representatives_atomic(self):
        """Extract representatives information."""
        loc = self._doc_loc()
        
        try:
            await loc.locator("text=Personal Representative").first.wait_for(timeout=4000)
        except:
            await loc.locator("tr.evenrow, tr.oddrow").first.wait_for(timeout=6000)

        rows = await loc.locator("tr.evenrow, tr.oddrow").all_text_contents()
        reps = []
        current = {"name": "", "address": ""}

        def looks_like_address(t: str) -> bool:
            u = t.upper()
            return any(ch.isdigit() for ch in t) or any(k in u for k in [
                "AVE","ST","STREET","AVENUE","ROAD","RD","LANE","LN","DR","DRIVE",
                "APT","SUITE","PO BOX","BLVD","COURT","CT"
            ])

        for raw in rows:
            t = raw.strip()
            if not t:
                continue
            if not looks_like_address(t) and len(t) < 120:
                if current["name"]:
                    reps.append({
                        "representative_name": current["name"], 
                        "representative_address": current["address"]
                    })
                    current = {"name": "", "address": ""}
                current["name"] = t
            else:
                current["address"] = f"{current['address']} {t}".strip() if current["address"] else t

        if current["name"]:
            reps.append({
                "representative_name": current["name"], 
                "representative_address": current["address"]
            })

        return reps

    async def safe_click_tab(self, tab_text, retries=3):
        """Safely click a tab."""
        tabs = self._tabs_loc()
        for attempt in range(retries):
            try:
                for sel in [
                    f"span.tabs-title:has-text('{tab_text}')",
                    f"li:has-text('{tab_text}') span.tabs-inner",
                    f"li:has-text('{tab_text}')",
                    f"span:has-text('{tab_text}')",
                ]:
                    try:
                        await tabs.locator(sel).first.click(timeout=4000)
                        return True
                    except:
                        continue

                # Fallback: JS click
                try:
                    await tabs.evaluate(f"""
                        () => {{
                            const spans = Array.from(document.querySelectorAll('span.tabs-title'));
                            const t = spans.find(x => x.textContent.trim().includes('{tab_text}'));
                            if (t) {{ t.click(); return true; }}
                            const li = Array.from(document.querySelectorAll('li')).find(x => x.textContent.includes('{tab_text}'));
                            if (li) {{ li.click(); return true; }}
                            return false;
                        }}
                    """)
                    return True
                except:
                    pass
            except:
                if attempt < retries - 1:
                    await asyncio.sleep(0.8)
        return False

    async def click_result_link_by_index(self, index: int) -> bool:
        """Click result link by index."""
        res = self._res_list_loc()

        # Try primary selector
        a = res.locator(f"a.link#inst{index}").first
        if await a.count():
            try:
                await a.click(timeout=6000)
                return True
            except:
                pass

        # Fallback: Nth clickable link
        all_links = res.locator("a.link[onclick*='loadRecord']")
        if await all_links.count() > index:
            try:
                await all_links.nth(index).click(timeout=6000)
                return True
            except:
                pass

        return False

    async def click_back_to_results(self, retries=5):
        """Return to results page - FIXED VERSION."""
        for attempt in range(retries):
            try:
                await asyncio.sleep(0.8)
                bodyframe = await self.wait_for_frame_by_name("bodyframe", 10000)

                resnavframe = None
                for _ in range(10):
                    for f in bodyframe.child_frames:
                        if f.name == "resnavframe":
                            resnavframe = f
                            break
                    if resnavframe:
                        break
                    await asyncio.sleep(0.3)

                if not resnavframe:
                    resnavframe = await self.wait_for_frame_by_url_fragment("navbar.do?page=search.details", 10)

                await resnavframe.wait_for_load_state("domcontentloaded", timeout=10000)
                await asyncio.sleep(0.5)

                clicked = False
                for sel in [
                    "text='Back to Results'",
                    "a[onclick*='executeSearchNav'][onclick*='results']",
                    "img[alt='Back to Results']",
                ]:
                    try:
                        await resnavframe.click(sel, timeout=3000)
                        clicked = True
                        break
                    except:
                        pass

                if not clicked:
                    try:
                        await resnavframe.evaluate("""
                            () => {
                                const elements = Array.from(document.querySelectorAll('a, img'));
                                const backElement = elements.find(el =>
                                    el.textContent.includes('Back to Results') ||
                                    el.alt === 'Back to Results' ||
                                    (el.onclick && el.onclick.toString().includes('results'))
                                );
                                if (backElement) {
                                    if (backElement.onclick) backElement.onclick();
                                    else if (backElement.parentElement && backElement.parentElement.onclick) backElement.parentElement.onclick();
                                    else backElement.click();
                                    return true;
                                }
                                return false;
                            }
                        """)
                        clicked = True
                    except:
                        pass

                if clicked:
                    await asyncio.sleep(1.2)
                    try:
                        await self.wait_for_frame_by_name("bodyframe", 5000)
                        await self.wait_for_frame_by_url_fragment("SearchResultsView.jsp", 5)
                        print("✅ Returned to results page")
                        return True
                    except:
                        print(f"⚠️ Back button clicked but results verification failed, attempt {attempt + 1}")
                        continue
                else:
                    print(f"⚠️ Could not find back button, attempt {attempt + 1}")

            except Exception as e:
                print(f"❌ Back to results attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(1.2)
                continue

        print("❌ Failed to return to results after all attempts")
        return False

    async def goto_results_page(self, page_number: int, wait_timeout: int = 20000) -> bool:
        """Navigate to specific results page - FIXED VERSION."""
        print(f"🔄 Navigating to page {page_number}")
        
        # First try the known subnav locator
        try:
            subnav = self._res_subnav_middle_loc()
            input_locator = subnav.locator("input[name='pageNumber']").first
            await input_locator.wait_for(state="attached", timeout=6000)
            await input_locator.fill(str(page_number))

            go_link = subnav.locator("a[onclick*='goToResultPage'], a[onclick*='goToResultPage()']").first
            if await go_link.count():
                try:
                    await go_link.click()
                except Exception:
                    await subnav.evaluate("() => { if (window.goToResultPage) goToResultPage(); }")
            else:
                await subnav.evaluate("() => { if (window.goToResultPage) goToResultPage(); }")

            res = self._res_list_loc()
            await res.locator("a.link#inst0, a.link[onclick*='loadRecord']").first.wait_for(timeout=wait_timeout)
            await asyncio.sleep(0.6)
            print(f"✅ Navigated to page {page_number}")
            return True

        except Exception as fast_err:
            print(f"⚠️ Fast path navigation failed: {fast_err}")

        # Fallback: search frames for pageNumber input
        try:
            input_frame = None
            input_element = None

            for f in self.page.frames:
                try:
                    handles = await f.query_selector_all("input[name='pageNumber']")
                    if handles:
                        input_frame = f
                        input_element = handles[0]
                        break
                except Exception:
                    continue

            if not input_element:
                print("❌ Could not find pageNumber input")
                return False

            await input_element.fill(str(page_number))

            try:
                result = await input_frame.evaluate(
                    """() => {
                        try {
                            if (typeof goToResultPage === 'function') { goToResultPage(); return true; }
                            if (window.goToResultPage) { window.goToResultPage(); return true; }
                            const goBtn = Array.from(document.querySelectorAll('a, input, button')).find(el =>
                                (el.getAttribute && (el.getAttribute('onclick') || '').includes('goToResultPage')) ||
                                (el.textContent && /go\s*to\s*result/i.test(el.textContent))
                            );
                            if (goBtn) { goBtn.click(); return true; }
                            return false;
                        } catch (e) {
                            return false;
                        }
                    }"""
                )
                if not result:
                    print("⚠️ JS navigation did not report success")
            except Exception as eval_err:
                print(f"⚠️ JS evaluation failed: {eval_err}")

            try:
                res = self._res_list_loc()
                await res.locator("a.link#inst0, a.link[onclick*='loadRecord']").first.wait_for(timeout=wait_timeout)
                await asyncio.sleep(0.6)
                print(f"✅ Navigated to page {page_number} (fallback)")
                return True
            except Exception as wait_err:
                print(f"❌ Waiting for results failed: {wait_err}")
                return False

        except Exception as e:
            print(f"❌ Navigation failed: {e}")
            return False

    async def scrape_single_day(self, scrape_date: datetime):
        """Scrape records for a single day - FIXED VERSION."""
        date_str = scrape_date.strftime("%m/%d/%Y")
        print(f"\n{'='*60}")
        print(f"📅 SCRAPING DATE: {date_str}")
        print(f"{'='*60}")

        all_records = []
        
        # Set date range for single day
        await self.enter_filing_dates(date_str, date_str)
        search_success = await self.click_search_button()
        
        if not search_success:
            print("❌ Search failed, no records to scrape")
            return all_records
        
        # Wait for results
        await asyncio.sleep(10)

        max_pages = 10  # Reasonable limit for single day
        page_index = 1
        
        while page_index <= max_pages:
            print(f"\n📄 Processing page {page_index}")

            # Wait for results list with retries
            for retry in range(5):
                try:
                    await self._res_list_loc().locator("a.link#inst0, a.link[onclick*='loadRecord']").first.wait_for(timeout=15000)
                    break
                except Exception as e:
                    if retry == 4:
                        print(f"❌ No results found on page {page_index}")
                        return all_records
                    print(f"🔄 Retry {retry + 1}/5 for page loading...")
                    await asyncio.sleep(2)

            processed_this_page = 0
            consecutive_misses = 0
            page_records = []

            # Process records on current page
            for row_idx in range(40):
                print(f"   📝 Processing record {row_idx + 1}")

                # Click record link with retries
                success = False
                for retry in range(3):
                    success = await self.click_result_link_by_index(row_idx)
                    if success:
                        break
                    await asyncio.sleep(1)
                
                if not success:
                    print(f"   ❌ Could not click record {row_idx + 1}")
                    consecutive_misses += 1
                    if consecutive_misses >= 3:
                        print("   ⏹️ End of page reached")
                        break
                    continue

                # Wait for document details with retries
                doc_loaded = False
                for retry in range(3):
                    try:
                        await self._doc_loc().locator("body").first.wait_for(state="attached", timeout=20000)
                        await asyncio.sleep(1.0)
                        doc_loaded = True
                        break
                    except Exception as e:
                        if retry == 2:
                            print(f"   ❌ Failed to load details")
                        await asyncio.sleep(1)

                if not doc_loaded:
                    consecutive_misses += 1
                    continue

                # Extract decedent info
                dec_info = {}
                try:
                    dec_info = await self.extract_decedent_info_atomic()
                    print(f"  ✅ Decedent info extracted")
                except Exception as e:
                    print(f"   ❌ Decedent extraction failed: {e}")

                # Extract representatives
                reps = []
                try:
                    clicked = await self.safe_click_tab("Representatives", retries=2)
                    if clicked:
                        await asyncio.sleep(1.0)
                        reps = await self.extract_representatives_atomic()
                        print(f"  ✅ {len(reps)} representatives extracted")
                except Exception as e:
                    print(f"   ❌ Representatives extraction failed: {e}")

                # Get case metadata from URL
                case_meta = {}
                try:
                    for f in self.page.frames:
                        if f.url and "DocumentInfoView.jsp" in f.url and "caseFileId=" in f.url:
                            qs = parse_qs(urlparse(f.url).query)
                            case_meta = {
                                "caseFileId": (qs.get("caseFileId") or [""])[0],
                                "caseFileNum": (qs.get("caseFileNum") or [""])[0],
                            }
                            break
                except Exception as e:
                    print(f"   ❌ Case metadata error: {e}")

                # Combine record data
                base_record = {
                    "case_file_no": dec_info.get("case_file_no", ""),
                    "filing_date": dec_info.get("filing_date", ""),
                    "decedent_address": dec_info.get("decedent_address", ""),
                    **case_meta,
                }

                if reps:
                    record_data = [{**base_record, **rep} for rep in reps]
                else:
                    record_data = [{**base_record, "representative_name": "", "representative_address": ""}]

                # Filter to only keep records with representative info
                valid_records = [
                    r for r in record_data 
                    if r.get("representative_name") and r.get("representative_address")
                ]
                
                page_records.extend(valid_records)
                processed_this_page += len(valid_records)
                consecutive_misses = 0

                print(f"  ✅ Record {row_idx + 1}: {len(valid_records)} valid entries")

                # Return to results with retries
                back_success = False
                for retry in range(3):
                    back_success = await self.click_back_to_results()
                    if back_success:
                        break
                    await asyncio.sleep(1)
                
                if not back_success:
                    print("❌ Failed to return to results")
                    break

                await asyncio.sleep(0.5)

            # Add page records to total
            all_records.extend(page_records)
            print(f"✅ Page {page_index}: {len(page_records)} records")
            print(f"📊 Total so far: {len(all_records)}")

            # Navigate to next page with retries
            if page_index < max_pages:
                print(f"🔄 Navigating to page {page_index + 1}")
                next_success = False
                for retry in range(3):
                    next_success = await self.goto_results_page(page_index + 1)
                    if next_success:
                        break
                    print(f"🔄 Retry {retry + 1}/3 for next page...")
                    await asyncio.sleep(2)
                
                if not next_success:
                    print("❌ Failed to navigate to next page")
                    break
                
                page_index += 1
                await asyncio.sleep(1.0)
            else:
                break

        print(f"✅ Date {date_str} complete: {len(all_records)} records")
        return all_records

async def main():
    """Main function for daily incremental scraping."""
    print("🚀 Starting Delaware Probate Daily Scraper")
    
    # Check environment
    if not SPREADSHEET_ID:
        print("❌ SPREADSHEET_ID environment variable is required")
        return
    
    # Initialize Google Sheets
    try:
        svc = sheets_client()
        print("✅ Google Sheets client initialized")
    except Exception as e:
        print(f"❌ Failed to initialize Google Sheets: {e}")
        return

    # Get last scraped date
    last_date = get_last_scraped_date(svc, SPREADSHEET_ID)
    
    # Determine scrape date
    if last_date:
        scrape_date = last_date + timedelta(days=1)
        print(f"📅 Last scraped date: {last_date}")
        print(f"📅 Scraping from date: {scrape_date}")
    else:
        # If no previous data, start from a reasonable recent date
        scrape_date = datetime.now().date() - timedelta(days=30)  # Start from 30 days ago
        print(f"📅 No previous data found, scraping from: {scrape_date}")
    
    # Don't scrape future dates
    today = datetime.now().date()
    if scrape_date > today:
        print(f"📅 Scrape date {scrape_date} is in the future, skipping")
        return
    
    print(f"\n🎯 Target scrape date: {scrape_date}")

    # Setup browser and scraper
    try:
        async with async_playwright() as pw:
            print("🌐 Launching browser...")
            browser = await pw.chromium.launch(
                headless=True,  # True for GitHub Actions
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--single-process"
                ]
            )
            
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            
            page = await context.new_page()
            scraper = DelawareScraper(page, browser=browser, context=context)
            
            try:
                # Perform scraping workflow
                print("\n🔐 Starting scraping workflow...")
                await scraper.goto_login()
                await scraper.accept_terms()
                await scraper.click_search_public_records()
                
                # Scrape single day
                new_records = await scraper.scrape_single_day(scrape_date)
                
                if new_records:
                    print(f"\n📊 Scraping complete: {len(new_records)} new records found")
                    
                    # Upload to Google Sheets
                    appended_count = append_to_google_sheets(svc, SPREADSHEET_ID, new_records)
                    
                    if appended_count:
                        print(f"🎉 Successfully uploaded {appended_count} records to Google Sheets")
                    else:
                        print("❌ Failed to upload records to Google Sheets")
                else:
                    print("📭 No new records found for the target date")
                    
            except Exception as e:
                print(f"❌ Scraping failed: {e}")
                traceback.print_exc()
                
            finally:
                await browser.close()
                print("🔚 Browser closed")
                
    except Exception as e:
        print(f"❌ Browser setup failed: {e}")
        traceback.print_exc()

    print("\n✅ Daily scraping process completed")

if __name__ == "__main__":
    asyncio.run(main())
