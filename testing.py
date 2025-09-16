# scrape_delaware.py
import asyncio
import json
import os
import traceback
from datetime import datetime
from pathlib import Path
import time
from collections import defaultdict
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

OUT_DIR = Path("out")
OUT_DIR.mkdir(exist_ok=True)


class DelawareScraper:
    def __init__(self, page, browser=None, context=None,
                 base_url: str = "https://delcorowonlineservices.co.delaware.pa.us/countyweb/loginDisplay.action?countyname=DelawarePA"):
        self.page = page
        self.browser = browser
        self.context = context
        self.base_url = base_url

    async def _dump_debug(self, name_prefix: str):
        """Save HTML for debugging (removed screenshot to avoid PNG files)."""
        ts = datetime.now().strftime("%Y%m%dT%H%M%SZ")
        html = OUT_DIR / f"{name_prefix}-{ts}.html"
        try:
            html_content = await self.page.content()
            html.write_text(html_content, encoding="utf-8")
            print(f"Debug HTML dumped: {html}")
        except Exception as e:
            print("Failed to save HTML:", e)

    async def wait_for_frame_by_url_fragment(self, url_fragment: str, timeout: int = 60):
        """
        Poll the page.frames until a frame whose URL contains url_fragment appears.
        Returns the Frame object or raises TimeoutError.
        """
        print(f"Waiting for frame with url containing '{url_fragment}' (timeout {timeout}s)...")
        for i in range(timeout):
            for f in self.page.frames:
                if f.url and url_fragment in f.url:
                    print(f"Found frame with url {f.url}")
                    return f
            await asyncio.sleep(1)
        raise PlaywrightTimeoutError(f"Frame with url fragment '{url_fragment}' not found within {timeout}s")


    async def wait_for_frame_by_name(self, name: str, timeout: float = 30000):
        """Wait for a frame with a specific name to be available."""
        start_time = time.time()
        while (time.time() - start_time) * 1000 < timeout:
            for frame in self.page.frames:
                if frame.name == name:
                    return frame
            await asyncio.sleep(0.1)
        raise PlaywrightTimeoutError(f"Frame with name '{name}' not found within {timeout}ms")

    # -----------------------
    # Login / navigation
    # -----------------------
    async def goto_login(self, retries: int = 3):
        """Go to login page and click 'Login as Guest'."""
        for attempt in range(1, retries + 1):
            try:
                await self.page.goto(self.base_url, timeout=60000)
                # small wait for initial rendering
                await self.page.wait_for_timeout(1500)
                # Try robust selector variations
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
                    raise PlaywrightTimeoutError("Could not find 'Login as Guest' button with known selectors")
                # wait for the expected post-login url pattern
                await self.page.wait_for_url("**/main.jsp?countyname=DelawarePA", timeout=60000)
                print("✅ Logged in as Guest")
                return
            except Exception as e:
                print(f"⚠ goto_login attempt {attempt} failed: {e}")
                if attempt == retries:
                    await self._dump_debug("goto_login_failed")
                    raise
                await asyncio.sleep(3)

    # -----------------------
    # Accept terms (iframe)
    # -----------------------
    async def accept_terms(self, retries: int = 3):
        """Handle iframe and click Accept button using frame_locator."""
        for attempt in range(1, retries + 1):
            try:
                # Wait for iframe wrapper presence (name might differ)
                await asyncio.sleep(1)
                # Use frame_locator to target the inner iframe's accept button.
                # Try several iframe selectors to be robust.
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
                    # As a fallback: iterate frames and try to find an '#accept' element inside any frame
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
                    raise PlaywrightTimeoutError("Could not locate an Accept button in known frame locations")

                print(f"✅ Accepted terms (attempt {attempt})")
                return

            except Exception as e:
                print(f"⚠ accept_terms attempt {attempt} failed: {e}")
                if attempt == retries:
                    print("Available frames (debug):")
                    for f in self.page.frames:
                        print(" -", f.url)
                    await self._dump_debug("accept_terms_failed")
                    raise
                await asyncio.sleep(2)

    # -----------------------
    # Click Search Public Records row
    # -----------------------
    async def click_search_public_records(self, retries: int = 3):
        """
        Click the row that triggers Search Public Records.
        Uses iframe frame_locator and robust waiting.
        """
        for attempt in range(1, retries + 1):
            try:
                await self.page.wait_for_timeout(1000)
                # The UI might place the table inside the same 'bodyframe' iframe
                frame_locator = self.page.frame_locator("iframe[name='bodyframe']")
                selector = "#datagrid-row-r1-2-0"
                await frame_locator.locator(selector).wait_for(state="visible", timeout=20000)
                await frame_locator.locator(selector).click()
                print("✅ Clicked 'Search Public Records'")
                return
            except Exception as e:
                print(f"⚠ click_search_public_records attempt {attempt} failed: {e}")
                if attempt == retries:
                    await self._dump_debug("click_search_public_records_failed")
                    raise
                # log frame URLs for debugging
                print("Frames at failure:")
                for f in self.page.frames:
                    print(" →", f.url)
                await asyncio.sleep(2)

    # -----------------------
    # Enter filing dates inside dynamic criteria frame
    # -----------------------
    async def enter_filing_dates(self, from_date: str = "01/01/2025", to_date: str = None, retries: int = 3):
        """Fill Filing Date From/To in the dynamically-loaded criteriaframe."""
        if to_date is None:
            to_date = datetime.today().strftime("%m/%d/%Y")

        print("⏳ Waiting a few seconds for dynamic frames to load...")
        await asyncio.sleep(3)

        for attempt in range(1, retries + 1):
            try:
                # Look for a frame whose URL contains 'dynCriteria.do'
                criteria_frame = None
                try:
                    criteria_frame = await self.wait_for_frame_by_url_fragment("dynCriteria.do", timeout=30)
                except PlaywrightTimeoutError:
                    # fallback: try blank.jsp or other known fragments
                    try:
                        criteria_frame = await self.wait_for_frame_by_url_fragment("blank.jsp", timeout=10)
                    except PlaywrightTimeoutError:
                        criteria_frame = None

                if not criteria_frame:
                    raise PlaywrightTimeoutError("Could not find criteria frame (dynCriteria.do or blank.jsp)")

                # ensure frame is loaded
                await criteria_frame.wait_for_load_state("domcontentloaded", timeout=15000)
                await asyncio.sleep(1)

                # Wait for date container
                el = await criteria_frame.wait_for_selector("#elemDateRange", timeout=15000)
                if not el:
                    raise PlaywrightTimeoutError("Date range container '#elemDateRange' not found")

                # locate FROM/TO inputs (IDs observed from your code)
                from_input = await criteria_frame.wait_for_selector("#_easyui_textbox_input7", timeout=10000)
                to_input = await criteria_frame.wait_for_selector("#_easyui_textbox_input8", timeout=10000)

                # Clear & type (use .fill where possible)
                await from_input.fill("")
                await from_input.type(from_date)
                print(f"✅ Entered FROM date: {from_date}")

                await to_input.fill("")
                await to_input.type(to_date)
                print(f"✅ Entered TO date: {to_date}")

                return

            except Exception as e:
                print(f"⚠ enter_filing_dates attempt {attempt} failed: {e}")
                if attempt == retries:
                    print("Frames (debug):")
                    for f in self.page.frames:
                        print(" ->", f.url)
                    await self._dump_debug("enter_filing_dates_failed")
                    raise
                await asyncio.sleep(2)

    # -----------------------
    # Click Search button inside dynamic search frame
    # -----------------------
    async def click_search_button(self, retries: int = 3):
        """Click the 'Search Public Records' button by navigating through the iframe hierarchy."""
        for attempt in range(1, retries + 1):
            try:
                # First, wait for the main body frame
                body_frame = await self.wait_for_frame_by_name("bodyframe", timeout=30000)
                
                # Wait for the dynamic search frame inside the body frame
                dyn_search_frame = await self.wait_for_frame_by_name("dynSearchFrame", timeout=30000)
                
                # Wait for the frame to load the search criteria content
                await dyn_search_frame.wait_for_load_state("domcontentloaded", timeout=15000)
                
                # Wait for the search button to be available
                search_selector = "a[onclick*='executeSearchCommand'][onclick*='search']"
                await dyn_search_frame.wait_for_selector(search_selector, timeout=15000)
                
                # Click using JavaScript to ensure it works
                await dyn_search_frame.eval_on_selector(search_selector, "el => el.click()")
                
                print("✅ Clicked 'Search Public Records' button")
                return True
                
            except Exception as e:
                print(f"⚠ click_search_button attempt {attempt} failed: {e}")
                if attempt == retries:
                    print("❌ All attempts to click search button failed")
                    await self._dump_debug("search_button_failed")
                    return False
                await asyncio.sleep(2)

    # -----------------------
    # Result processing methods
    # -----------------------
    async def collect_result_row_links(self):
        """
        From the results page frame stack:
        bodyframe -> resultFrame -> resultListFrame
        Find the clickable links for each record (the anchor after the checkbox).
        Returns a list of element handles or click actions (we'll click via eval to avoid navigation race).
        """
        bodyframe = await self.wait_for_frame_by_name("bodyframe", 30000)
        resultFrame = None
        # resultFrame may be nested in resultsContent; name is 'resultFrame'
        for f in bodyframe.child_frames:
            if f.name == "resultFrame":
                resultFrame = f
                break
        if not resultFrame:
            # Fallback by URL fragment
            resultFrame = await self.wait_for_frame_by_url_fragment("SearchResultsView.jsp", 30)

        # Inside resultFrame there is resultListFrame
        resultListFrame = None
        for f in resultFrame.child_frames:
            if f.name == "resultListFrame":
                resultListFrame = f
                break
        if not resultListFrame:
            resultListFrame = await self.wait_for_frame_by_url_fragment("casefile_SearchResultList.jsp", 30)

        # Wait for the table
        # We'll look for anchors with id like inst0, inst1, ... or with onclick loadRecord(...)
        await resultListFrame.wait_for_load_state("domcontentloaded", timeout=15000)

        anchors = await resultListFrame.query_selector_all("a.link[id^='inst'], a.link[onclick*='loadRecord']")
        return resultListFrame, anchors

    async def click_result_link_by_index(self, resultListFrame, index):
        # Click via JS to avoid overlay issues
        selector = f"a.link#inst{index}"
        handle = await resultListFrame.query_selector(selector)
        if handle:
            await resultListFrame.eval_on_selector(selector, "el => el.click()")
            return True

        # Fallback by Nth link
        anchors = await resultListFrame.query_selector_all("a.link[onclick*='loadRecord']")
        if index < len(anchors):
            await anchors[index].evaluate("el => el.click()")
            return True
        return False

    async def wait_details_loaded(self, timeout_s=30):
        """
        After clicking a record link, details page loads within:
        bodyframe -> documentFrame -> docInfoFrame
        We wait until docInfoFrame has loaded expected content.
        """
        bodyframe = await self.wait_for_frame_by_name("bodyframe", 30000)
        # Find documentFrame
        documentFrame = None
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            for f in bodyframe.child_frames:
                if f.name == "documentFrame":
                    documentFrame = f
                    break
            if documentFrame:
                break
            await asyncio.sleep(0.2)
        if not documentFrame:
            documentFrame = await self.wait_for_frame_by_url_fragment("DocumentInfoView.jsp", timeout_s)

        # Find docInfoFrame
        docInfoFrame = None
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            for f in documentFrame.child_frames:
                if f.name == "docInfoFrame":
                    docInfoFrame = f
                    break
            if docInfoFrame:
                break
            await asyncio.sleep(0.2)
        if not docInfoFrame:
            docInfoFrame = await self.wait_for_frame_by_url_fragment("transAddDocCaseFile.do", timeout_s)

        await docInfoFrame.wait_for_load_state("domcontentloaded", timeout=15000)
        return bodyframe, documentFrame, docInfoFrame

    async def safe_click_tab(self, frame, tab_text, retries=3):
        """Safely click a tab by text, handling frame detachment issues."""
        for attempt in range(retries):
            try:
                # Wait for the tab structure to be loaded
                await frame.wait_for_selector("ul.tabs", timeout=10000)
                await asyncio.sleep(0.5)
                
                # Try multiple selectors based on the HTML structure you provided
                selectors_to_try = [
                    f"span.tabs-title:has-text('{tab_text}')",
                    f"li:has-text('{tab_text}') span.tabs-inner",
                    f"li:has-text('{tab_text}')",
                    f"span:has-text('{tab_text}')"
                ]
                
                clicked = False
                for selector in selectors_to_try:
                    try:
                        await frame.wait_for_selector(selector, timeout=5000)
                        await frame.click(selector, timeout=5000)
                        clicked = True
                        break
                    except:
                        continue
                
                # If regular clicking failed, try JavaScript
                if not clicked:
                    try:
                        clicked = await frame.evaluate(f"""
                            () => {{
                                const spans = Array.from(document.querySelectorAll('span.tabs-title'));
                                const targetSpan = spans.find(span => span.textContent.trim().includes('{tab_text}'));
                                if (targetSpan) {{
                                    targetSpan.click();
                                    return true;
                                }}
                                
                                // Try clicking the parent li element
                                const lis = Array.from(document.querySelectorAll('li'));
                                const targetLi = lis.find(li => li.textContent.includes('{tab_text}'));
                                if (targetLi) {{
                                    targetLi.click();
                                    return true;
                                }}
                                return false;
                            }}
                        """)
                    except Exception as js_err:
                        print(f"JavaScript click failed: {js_err}")
                
                if clicked:
                    return True
                    
            except Exception as e:
                print(f"Failed to click tab '{tab_text}' attempt {attempt + 1}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(1)
                continue
        return False


    async def select_tab_representatives(self, bodyframe, timeout_s=30):
        """
        Robustly click the 'Representatives' tab with improved retry logic.
        """
        attempt_deadline = time.time() + timeout_s
        last_err = None
        
        while time.time() < attempt_deadline:
            try:
                # Fresh frame acquisition each attempt
                bodyframe = await self.wait_for_frame_by_name("bodyframe", timeout=10000)
                
                # Find documentFrame
                documentFrame = None
                for attempt in range(10):  # 10 attempts with 0.5s each = 5s max wait
                    for f in bodyframe.child_frames:
                        if f.name == "documentFrame":
                            documentFrame = f
                            break
                    if documentFrame:
                        break
                    await asyncio.sleep(0.5)
                
                if not documentFrame:
                    documentFrame = await self.wait_for_frame_by_url_fragment("DocumentInfoView.jsp", 10)

                # Find docInfoFrame
                docInfoFrame = None
                for attempt in range(10):
                    for f in documentFrame.child_frames:
                        if f.name == "docInfoFrame":
                            docInfoFrame = f
                            break
                    if docInfoFrame:
                        break
                    await asyncio.sleep(0.5)
                
                if not docInfoFrame:
                    docInfoFrame = await self.wait_for_frame_by_url_fragment("transAddDocCaseFile.do", 10)

                await docInfoFrame.wait_for_load_state("domcontentloaded", timeout=15000)
                
                # Find tabs frame
                tabs_frame = None
                for attempt in range(10):
                    for f in docInfoFrame.child_frames:
                        if f.name == "tabs":
                            tabs_frame = f
                            break
                    if tabs_frame:
                        break
                    await asyncio.sleep(0.5)
                
                if not tabs_frame:
                    tabs_frame = await self.wait_for_frame_by_url_fragment("tabbar.do", 10)

                await tabs_frame.wait_for_load_state("domcontentloaded", timeout=10000)
                await asyncio.sleep(1)  # Extra stability wait

                # Click the Representatives tab
                if await self.safe_click_tab(tabs_frame, "Representatives"):
                    # Wait for representative section to load with multiple possible selectors
                    try:
                        await asyncio.sleep(3)  # Wait for content to load
                        # Check if docInfoFrame is still valid and has content
                        await docInfoFrame.wait_for_load_state("domcontentloaded", timeout=10000)
                        
                        # Try multiple selectors for representative content
                        selectors_to_try = [
                            "#PERSONAL_REPRESENTATIVEheader",
                            "span.subsectionheader",
                            "table.base",
                            "tr.evenrow, tr.oddrow"
                        ]
                        
                        content_found = False
                        for selector in selectors_to_try:
                            try:
                                await docInfoFrame.wait_for_selector(selector, timeout=5000)
                                content_found = True
                                break
                            except:
                                continue
                        
                        if content_found:
                            await asyncio.sleep(1)  # Final stability wait
                            return tabs_frame, docInfoFrame
                        
                    except Exception as wait_err:
                        print(f"Content wait failed: {wait_err}")
                        continue

            except Exception as e:
                last_err = e
                print(f"Tab selection attempt failed: {e}")
                await asyncio.sleep(1)

        raise last_err if last_err else PlaywrightTimeoutError("Failed to select Representatives tab within timeout")


    async def extract_representatives(self, docInfoFrame):
        """
        Parse representative name and address under the 'Personal Representative(s):' subsection.
        Updated to match the exact HTML structure provided.
        """
        try:
            html = await docInfoFrame.content()
            soup = BeautifulSoup(html, "html.parser")
            reps = []

            # Look for the Personal Representative header
            header = soup.select_one("#PERSONAL_REPRESENTATIVEheader")
            if header:
                # Find the parent table and then look for the next table that contains the data
                header_table = header.find_parent("table")
                if header_table:
                    # Look for the next table after the header table
                    next_table = header_table.find_next_sibling("table")
                    if next_table:
                        # Look for evenrow/oddrow within nested tables
                        rows = next_table.select("tr.evenrow, tr.oddrow")
                        
                        # Based on your HTML structure, name and address are in separate rows
                        name = ""
                        address = ""
                        
                        for i, row in enumerate(rows):
                            cells = row.select("td")
                            if len(cells) >= 2:
                                text_content = cells[1].get_text(" ", strip=True)
                                
                                # First row with substantial text is usually the name
                                if i == 0 and text_content and not text_content.isspace():
                                    name = " ".join(text_content.split())
                                # Second row with substantial text is usually the address
                                elif i == 1 and text_content and not text_content.isspace():
                                    # Clean up address by removing extra whitespace and line breaks
                                    address = " ".join(text_content.replace("\n", " ").split())
                        
                        if name:
                            reps.append({
                                "representative_name": name,
                                "representative_address": address
                            })

            # Fallback method: Look for any evenrow/oddrow pattern
            if not reps:
                all_rows = soup.select("tr.evenrow, tr.oddrow")
                current_name = ""
                current_address = ""
                
                for row in all_rows:
                    cells = row.select("td")
                    if len(cells) >= 2:
                        text_content = cells[1].get_text(" ", strip=True)
                        
                        # Skip empty content
                        if not text_content or text_content.isspace():
                            continue
                            
                        # If it looks like a name (no numbers, relatively short)
                        if not any(char.isdigit() for char in text_content) and len(text_content) < 100:
                            if current_name:  # Save previous representative if exists
                                reps.append({
                                    "representative_name": current_name,
                                    "representative_address": current_address
                                })
                            current_name = " ".join(text_content.split())
                            current_address = ""
                        # If it looks like an address (contains numbers or common address words)
                        elif (any(char.isdigit() for char in text_content) or 
                              any(word in text_content.upper() for word in ["AVE", "ST", "STREET", "AVENUE", "ROAD", "RD", "LANE", "LN", "DR", "DRIVE", "APT", "SUITE"])):
                            current_address = " ".join(text_content.replace("\n", " ").split())
                
                # Don't forget the last representative
                if current_name:
                    reps.append({
                        "representative_name": current_name,
                        "representative_address": current_address
                    })

            return reps

        except Exception as e:
            print(f"Error extracting representatives: {e}")
            return []

    async def select_tab_decedent(self, bodyframe, timeout_s=30):
        """
        Robustly click the 'Decedent & Estate Info' tab with improved retry logic.
        """
        attempt_deadline = time.time() + timeout_s
        last_err = None
        
        while time.time() < attempt_deadline:
            try:
                # Fresh frame acquisition each attempt
                bodyframe = await self.wait_for_frame_by_name("bodyframe", timeout=10000)
                
                # Find documentFrame
                documentFrame = None
                for attempt in range(10):
                    for f in bodyframe.child_frames:
                        if f.name == "documentFrame":
                            documentFrame = f
                            break
                    if documentFrame:
                        break
                    await asyncio.sleep(0.5)
                
                if not documentFrame:
                    documentFrame = await self.wait_for_frame_by_url_fragment("DocumentInfoView.jsp", 10)

                # Find docInfoFrame
                docInfoFrame = None
                for attempt in range(10):
                    for f in documentFrame.child_frames:
                        if f.name == "docInfoFrame":
                            docInfoFrame = f
                            break
                    if docInfoFrame:
                        break
                    await asyncio.sleep(0.5)
                
                if not docInfoFrame:
                    docInfoFrame = await self.wait_for_frame_by_url_fragment("transAddDocCaseFile.do", 10)

                await docInfoFrame.wait_for_load_state("domcontentloaded", timeout=15000)
                
                # Find tabs frame
                tabs_frame = None
                for attempt in range(10):
                    for f in docInfoFrame.child_frames:
                        if f.name == "tabs":
                            tabs_frame = f
                            break
                    if tabs_frame:
                        break
                    await asyncio.sleep(0.5)
                
                if not tabs_frame:
                    tabs_frame = await self.wait_for_frame_by_url_fragment("tabbar.do", 10)

                await tabs_frame.wait_for_load_state("domcontentloaded", timeout=10000)
                await asyncio.sleep(1)  # Extra stability wait

                # Click Decedent & Estate Info tab
                if await self.safe_click_tab(tabs_frame, "Decedent & Estate Info"):
                    try:
                        await asyncio.sleep(3)  # Wait for content to load
                        await docInfoFrame.wait_for_load_state("domcontentloaded", timeout=10000)
                        
                        # Try multiple selectors for decedent content
                        selectors_to_try = [
                            "#fcaddrCORESPONDENT_ADDRESSspan",
                            "#fieldFILING_DATEspan", 
                            "table.base",
                            "span.base"
                        ]
                        
                        content_found = False
                        for selector in selectors_to_try:
                            try:
                                await docInfoFrame.wait_for_selector(selector, timeout=5000)
                                content_found = True
                                break
                            except:
                                continue
                        
                        if content_found:
                            await asyncio.sleep(1)  # Final stability wait
                            return tabs_frame, docInfoFrame
                        
                    except Exception as wait_err:
                        print(f"Decedent content wait failed: {wait_err}")
                        continue

            except Exception as e:
                last_err = e
                print(f"Decedent tab selection attempt failed: {e}")
                await asyncio.sleep(1)

        raise last_err if last_err else PlaywrightTimeoutError("Failed to select Decedent & Estate Info tab within timeout")

    async def extract_decedent_info(self, docInfoFrame):
        """
        Extract Filing Date and decedent address fields from Decedent & Estate Info.
        Improved to handle the exact HTML structure you provided.
        """
        try:
            html = await docInfoFrame.content()
            soup = BeautifulSoup(html, "html.parser")

            filing_date = ""
            decedent_address = ""

            # Look for Filing Date - try multiple possible field IDs
            filing_date_selectors = [
                "#fieldFILING_DATEspan",
                "#fieldFILING_DATE", 
                "span[id*='FILING_DATE']"
            ]
            
            for selector in filing_date_selectors:
                element = soup.select_one(selector)
                if element:
                    # Get the parent row and find the value cell
                    row = element.find_parent("tr")
                    if row:
                        cells = row.find_all("td")
                        if len(cells) >= 3:
                            filing_date = cells[2].get_text(" ", strip=True)
                            break

            # Extract address components using the exact IDs from your HTML
            addr_components = {}
            
            # Address
            addr_span = soup.select_one("#fcaddrCORESPONDENT_ADDRESSspan")
            if addr_span:
                row = addr_span.find_parent("tr")
                if row:
                    cells = row.find_all("td")
                    if len(cells) >= 3:
                        addr_components['address'] = cells[2].get_text(" ", strip=True)

            # City
            city_span = soup.select_one("#fccityCORESPONDENT_ADDRESSspan")
            if city_span:
                row = city_span.find_parent("tr")
                if row:
                    cells = row.find_all("td")
                    if len(cells) >= 3:
                        addr_components['city'] = cells[2].get_text(" ", strip=True)

            # State and Zip (they're in the same row based on your HTML)
            state_span = soup.select_one("#fcstateCORESPONDENT_ADDRESSspan")
            if state_span:
                row = state_span.find_parent("tr")
                if row:
                    # State is in a nested table structure
                    nested_table = row.select_one("table.base")
                    if nested_table:
                        nested_row = nested_table.select_one("tr")
                        if nested_row:
                            nested_cells = nested_row.find_all("td")
                            if len(nested_cells) >= 1:
                                addr_components['state'] = nested_cells[0].get_text(" ", strip=True)
                            if len(nested_cells) >= 3:
                                addr_components['zip'] = nested_cells[2].get_text(" ", strip=True)

            # Combine address components
            address_parts = []
            for key in ['address', 'city', 'state', 'zip']:
                if key in addr_components and addr_components[key]:
                    address_parts.append(addr_components[key])
            
            decedent_address = ", ".join(address_parts) if address_parts else ""

            return {
                "filing_date": filing_date,
                "decedent_address": decedent_address
            }

        except Exception as e:
            print(f"Error extracting decedent info: {e}")
            return {
                "filing_date": "",
                "decedent_address": ""
            }
        
        
    async def click_back_to_results(self, retries=5):
        """
        Enhanced back to results with better retry logic and waits.
        """
        for attempt in range(retries):
            try:
                # Wait a bit for any ongoing navigation to complete
                await asyncio.sleep(1)
                
                bodyframe = await self.wait_for_frame_by_name("bodyframe", 10000)
                
                # Find resnavframe
                resnavframe = None
                for retry in range(10):  # 5 second max wait
                    for f in bodyframe.child_frames:
                        if f.name == "resnavframe":
                            resnavframe = f
                            break
                    if resnavframe:
                        break
                    await asyncio.sleep(0.5)
                
                if not resnavframe:
                    resnavframe = await self.wait_for_frame_by_url_fragment("navbar.do?page=search.details", 10)

                await resnavframe.wait_for_load_state("domcontentloaded", timeout=10000)
                await asyncio.sleep(1)  # Extra wait for stability
                
                # Try multiple methods to click back
                clicked = False
                
                # Method 1: Text-based selector
                try:
                    await resnavframe.click("text='Back to Results'", timeout=5000)
                    clicked = True
                except:
                    pass
                
                # Method 2: onclick attribute
                if not clicked:
                    try:
                        await resnavframe.click("a[onclick*='executeSearchNav'][onclick*='results']", timeout=5000)
                        clicked = True
                    except:
                        pass
                
                # Method 3: Image alt attribute
                if not clicked:
                    try:
                        await resnavframe.click("img[alt='Back to Results']", timeout=5000)
                        clicked = True
                    except:
                        pass
                
                # Method 4: JavaScript evaluation
                if not clicked:
                    try:
                        await resnavframe.evaluate("""
                            () => {
                                const elements = Array.from(document.querySelectorAll('a, img'));
                                const backElement = elements.find(el => 
                                    el.textContent.includes('Back to Results') || 
                                    el.alt === 'Back to Results' ||
                                    (el.onclick && el.onclick.toString().includes('results')) ||
                                    (el.href && el.href.includes('#'))
                                );
                                if (backElement) {
                                    if (backElement.onclick) {
                                        backElement.onclick();
                                    } else if (backElement.parentElement && backElement.parentElement.onclick) {
                                        backElement.parentElement.onclick();
                                    } else {
                                        backElement.click();
                                    }
                                    return true;
                                }
                                return false;
                            }
                        """)
                        clicked = True
                    except:
                        pass
                
                if clicked:
                    # Wait for results to load back
                    await asyncio.sleep(2)
                    # Verify we're back at results by checking for result frame
                    try:
                        await self.wait_for_frame_by_name("bodyframe", 5000)
                        result_frame = await self.wait_for_frame_by_url_fragment("SearchResultsView.jsp", 5)
                        print("Successfully returned to results page")
                        return True
                    except:
                        # If verification fails, continue to retry
                        print(f"Back button clicked but results verification failed, attempt {attempt + 1}")
                        continue
                else:
                    print(f"Could not find back button, attempt {attempt + 1}")
                
            except Exception as e:
                print(f"Back to results attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(2)
                continue
        
        print("Failed to return to results after all attempts")
        return False


    async def click_next_results_page(self):
        """
        Next results page link is inside:
        bodyframe -> resultFrame -> subnav (navbar.do?page=search.resultNav.next&subnav=1...)
        """
        bodyframe = await self.wait_for_frame_by_name("bodyframe", 30000)
        
        # Find resultFrame first
        resultFrame = None
        for f in bodyframe.child_frames:
            if f.name == "resultFrame":
                resultFrame = f
                break
        
        if not resultFrame:
            resultFrame = await self.wait_for_frame_by_url_fragment("SearchResultsView.jsp", 20)

        # subnav frame is a child frame that loads navbar.do with subnav=1
        subnav_frame = None
        for f in resultFrame.child_frames:
            if f.url and "navbar.do" in f.url and "subnav=1" in f.url:
                subnav_frame = f
                break
        
        if not subnav_frame:
            subnav_frame = await self.wait_for_frame_by_url_fragment("navbar.do?page=search.resultNav", 20)

        await subnav_frame.wait_for_load_state("domcontentloaded", timeout=15000)
        
        # Try to click next using multiple methods
        had_next = False
        
        # Method 1: Look for next button by text
        try:
            await subnav_frame.click("text='Next'", timeout=5000)
            had_next = True
        except:
            # Method 2: Look for next button by onclick handler
            try:
                await subnav_frame.click("a[onclick*='navigateResults'][onclick*='next']", timeout=5000)
                had_next = True
            except:
                # Method 3: Use JavaScript to find and click the next button
                try:
                    had_next = await subnav_frame.evaluate("""
                        () => {
                            const links = Array.from(document.querySelectorAll('a'));
                            const nextLink = links.find(el => 
                                el.textContent.includes('Next') || 
                                (el.onclick && el.onclick.toString().includes('next'))
                            );
                            if (nextLink) {
                                nextLink.click();
                                return true;
                            }
                            return false;
                        }
                    """)
                except:
                    print("Could not find Next button")
        
        await asyncio.sleep(1.0)
        return bool(had_next)

    async def deep_scrape_all_results(self):
        """
        Iterate all result pages, open each record, extract reps and decedent info,
        and build a list of dicts.
        """
        all_records = []
        page_index = 1
        
        while True:
            # Ensure results page visible
            try:
                resultListFrame, anchors = await self.collect_result_row_links()
            except Exception as e:
                print("No results frame found or results not visible:", e)
                break

            # Recompute anchors each loop in case of dynamic rendering
            anchors = await resultListFrame.query_selector_all("a.link[id^='inst'], a.link[onclick*='loadRecord']")
            num_rows = len(anchors)
            print(f"Results page {page_index}: found {num_rows} records")

            for row_idx in range(num_rows):
                print(f"Processing record {row_idx + 1} of {num_rows} on page {page_index}")
                
                # Click the row link
                ok = await self.click_result_link_by_index(resultListFrame, row_idx)
                if not ok:
                    print(f"Skipping row {row_idx}: link not found")
                    continue

                # Wait details
                try:
                    bodyframe, documentFrame, docInfoFrame = await self.wait_details_loaded(30)
                except Exception as e:
                    print(f"Failed to load details for record {row_idx}: {e}")
                    # Try to go back to results
                    try:
                        await self.click_back_to_results()
                    except:
                        pass
                    continue

                # Representatives
                reps = []
                try:
                    await self.select_tab_representatives(bodyframe, 20)
                    reps = await self.extract_representatives(docInfoFrame)
                except Exception as e:
                    print(f"Failed to extract representatives for record {row_idx}: {e}")

                # Decedent & Estate Info
                dec_info = {"filing_date": "", "decedent_address": ""}
                try:
                    await self.select_tab_decedent(bodyframe, 20)
                    dec_info = await self.extract_decedent_info(docInfoFrame)
                except Exception as e:
                    print(f"Failed to extract decedent info for record {row_idx}: {e}")

                # Also try to capture case identifiers from the DocumentInfoView URL
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
                except Exception:
                    pass

                base_record = {
                    "filing_date": dec_info.get("filing_date"),
                    "decedent_address": dec_info.get("decedent_address"),
                    **case_meta
                }

                if reps:
                    for r in reps:
                        rec = {**base_record, **r}
                        all_records.append(rec)
                else:
                    all_records.append({**base_record, "representative_name": "", "representative_address": ""})

                # Back to results
                try:
                    await self.click_back_to_results()
                except Exception as e:
                    print(f"Failed to go back to results: {e}")
                    # If we can't go back, we might need to restart the search
                    break

                # Wait for results list again before next iteration
                try:
                    resultListFrame, _ = await self.collect_result_row_links()
                except Exception:
                    # Try a brief wait and retry
                    await asyncio.sleep(1.0)
                    try:
                        resultListFrame, _ = await self.collect_result_row_links()
                    except:
                        print("Could not return to results list after processing record")
                        break

            # Try next page
            has_next = await self.click_next_results_page()
            if not has_next:
                print("No next page button or reached last page.")
                break

            # Wait for next results list to load
            await asyncio.sleep(1.5)
            page_index += 1

        return all_records

    def write_monthwise_xlsx(self, records, out_path):
        """
        records: list of dicts with keys:
        - filing_date (MM/DD/YYYY or similar)
        - decedent_address
        - representative_name
        - representative_address
        - caseFileId
        - caseFileNum
        Creates an XLSX with one sheet per YYYY-MM.
        """
        wb = Workbook()
        # Remove default sheet; we will create sheets on demand
        if wb.active:
            wb.remove(wb.active)

        by_month = defaultdict(list)
        for r in records:
            fd = (r.get("filing_date") or "").strip()
            month_key = "Unknown"
            try:
                # Accept MM/DD/YYYY or M/D/YYYY
                dt = datetime.strptime(fd, "%m/%d/%Y")
                month_key = dt.strftime("%Y-%m")
            except Exception:
                # Try alternative known formats if any
                try:
                    dt = datetime.strptime(fd, "%m/%d/%y")
                    month_key = dt.strftime("%Y-%m")
                except Exception:
                    pass
            by_month[month_key].append(r)

        headers = ["filing_date", "caseFileNum", "caseFileId", "decedent_address", "representative_name", "representative_address"]

        for month in sorted(by_month.keys()):
            ws = wb.create_sheet(title=month[:31])  # Excel sheet name limit
            ws.append(headers)
            for r in by_month[month]:
                ws.append([r.get(h, "") for h in headers])

            # Optional: auto-width
            for col_idx, h in enumerate(headers, start=1):
                max_len = max([len(str(h))] + [len(str(ws.cell(row=i, column=col_idx).value or "")) for i in range(2, ws.max_row + 1)])
                ws.column_dimensions[get_column_letter(col_idx)].width = min(60, max_len + 2)

        wb.save(out_path)
        print(f"✅ XLSX written: {out_path}")


async def run_full_scrape_and_export(scraper):
    """
    Call this after your existing click_search_button() and result load wait.
    """
    all_records = await scraper.deep_scrape_all_results()
    # Write JSON mirror
    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / "all_records.json").write_text(json.dumps(all_records, ensure_ascii=False, indent=2), encoding="utf-8")
    # Write month-wise XLSX
    xlsx_path = OUT_DIR / "delaware_records_monthwise.xlsx"
    scraper.write_monthwise_xlsx(all_records, xlsx_path)


async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)  # set False while debugging
        context = await browser.new_context()
        page = await context.new_page()

        scraper = DelawareScraper(page, browser=browser, context=context)
        try:
            await scraper.goto_login()
            await scraper.accept_terms()
            await scraper.click_search_public_records()
            await scraper.enter_filing_dates()
            await scraper.click_search_button()
            # sleep for 10 seconds to allow results to load
            await asyncio.sleep(10)
            await run_full_scrape_and_export(scraper)
        except Exception:
            print("❌ Scraper failed with exception:")
            traceback.print_exc()
            # dump page / screenshot for debugging
            try:
                await scraper._dump_debug("fatal_error")
            except Exception as e:
                print("debug dump also failed:", e)
            raise
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())