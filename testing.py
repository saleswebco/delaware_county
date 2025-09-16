# scrape_delaware.py
import asyncio
import json
import os
import traceback
from datetime import datetime
from pathlib import Path
import time

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

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
        """Save screenshot and page HTML for debugging."""
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        shot = OUT_DIR / f"{name_prefix}-{ts}.png"
        html = OUT_DIR / f"{name_prefix}-{ts}.html"
        try:
            await self.page.screenshot(path=str(shot), full_page=True)
        except Exception as e:
            print("Failed to take screenshot:", e)
        try:
            html_content = await self.page.content()
            html.write_text(html_content, encoding="utf-8")
        except Exception as e:
            print("Failed to save HTML:", e)
        print(f"🧾 Debug dumped: {shot} , {html}")

    async def wait_for_frame_by_url_fragment(self, url_fragment: str, timeout: int = 60):
        """
        Poll the page.frames until a frame whose URL contains url_fragment appears.
        Returns the Frame object or raises TimeoutError.
        """
        print(f"⏳ Waiting for frame with url containing '{url_fragment}' (timeout {timeout}s)...")
        for i in range(timeout):
            for f in self.page.frames:
                if f.url and url_fragment in f.url:
                    print(f"✅ Found frame with url {f.url}")
                    return f
            await asyncio.sleep(1)
        raise PlaywrightTimeoutError(f"Frame with url fragment '{url_fragment}' not found within {timeout}s")

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




    # # -----------------------
    # # Click Search button inside dynamic search frame
    # # -----------------------
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

    async def wait_for_frame_by_name(self, name: str, timeout: float = 30000):
        """Wait for a frame with a specific name to be available."""
        start_time = time.time()
        while (time.time() - start_time) * 1000 < timeout:
            for frame in self.page.frames:
                if frame.name == name:
                    return frame
            await asyncio.sleep(0.1)
        raise PlaywrightTimeoutError(f"Frame with name '{name}' not found within {timeout}ms")






    
async def get_frame_by_name(page, name, timeout_ms=30000):
    start = time.time()
    while (time.time() - start) * 1000 < timeout_ms:
        for f in page.frames:
            # Playwright v1.46+ uses frame.name; fallback to f.name
            try:
                if f.name == name:
                    return f
            except Exception:
                pass
        await asyncio.sleep(0.1)
    raise PlaywrightTimeoutError(f"Frame with name '{name}' not found within {timeout_ms}ms")

async def get_frame_by_url_contains(page, fragment, timeout_s=30):
    start = time.time()
    while (time.time() - start) < timeout_s:
        for f in page.frames:
            try:
                if f.url and fragment in f.url:
                    return f
            except Exception:
                pass
        await asyncio.sleep(0.2)
    raise PlaywrightTimeoutError(f"Frame containing '{fragment}' not found in {timeout_s}s")

async def wait_visible_in_frame(frame, selector, timeout_ms=15000):
    el = await frame.wait_for_selector(selector, timeout=timeout_ms, state="visible")
    return el

async def click_in_frame(frame, selector, timeout_ms=15000):
    await wait_visible_in_frame(frame, selector, timeout_ms)
    await frame.click(selector)

async def collect_result_row_links(page):
    """
    From the results page frame stack:
    bodyframe -> resultFrame -> resultListFrame
    Find the clickable links for each record (the anchor after the checkbox).
    Returns a list of element handles or click actions (we'll click via eval to avoid navigation race).
    """
    bodyframe = await get_frame_by_name(page, "bodyframe", 30000)
    resultFrame = None
    # resultFrame may be nested in resultsContent; name is 'resultFrame'
    for f in bodyframe.child_frames:
        if f.name == "resultFrame":
            resultFrame = f
            break
    if not resultFrame:
        # Fallback by URL fragment
        resultFrame = await get_frame_by_url_contains(page, "SearchResultsView.jsp", 30)

    # Inside resultFrame there is resultListFrame
    resultListFrame = None
    for f in resultFrame.child_frames:
        if f.name == "resultListFrame":
            resultListFrame = f
            break
    if not resultListFrame:
        resultListFrame = await get_frame_by_url_contains(page, "casefile_SearchResultList.jsp", 30)

    # Wait for the table
    # We'll look for anchors with id like inst0, inst1, ... or with onclick loadRecord(...)
    await resultListFrame.wait_for_load_state("domcontentloaded", timeout=15000)

    anchors = await resultListFrame.query_selector_all("a.link[id^='inst'], a.link[onclick*='loadRecord']")
    return resultListFrame, anchors

async def click_result_link_by_index(resultListFrame, index):
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

async def wait_details_loaded(page, timeout_s=30):
    """
    After clicking a record link, details page loads within:
    bodyframe -> documentFrame -> docInfoFrame
    We wait until docInfoFrame has loaded expected content.
    """
    bodyframe = await get_frame_by_name(page, "bodyframe", 30000)
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
        documentFrame = await get_frame_by_url_contains(page, "DocumentInfoView.jsp", timeout_s)

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
        docInfoFrame = await get_frame_by_url_contains(page, "transAddDocCaseFile.do", timeout_s)

    await docInfoFrame.wait_for_load_state("domcontentloaded", timeout=15000)
    return bodyframe, documentFrame, docInfoFrame

async def select_tab_representatives(bodyframe, timeout_s=20):
    """
    Robustly click the 'Representatives' tab in the tabs iframe.
    Retries if the frame reloads (detaches).
    """
    attempt_deadline = time.time() + timeout_s
    last_err = None
    while time.time() < attempt_deadline:
        try:
            # Reacquire frames fresh each iteration
            _, documentFrame, docInfoFrame = await wait_details_loaded(bodyframe.page, timeout_s)
            tabs_frame = None
            # Find tabs frame
            t_deadline = time.time() + 5
            while time.time() < t_deadline and not tabs_frame:
                for f in docInfoFrame.child_frames:
                    if f.name == "tabs":
                        tabs_frame = f
                        break
                if not tabs_frame:
                    await asyncio.sleep(0.1)
            if not tabs_frame:
                tabs_frame = await get_frame_by_url_contains(bodyframe.page, "tabbar.do", 10)

            await tabs_frame.wait_for_load_state("domcontentloaded", timeout=10000)

            # Click the Representatives tab by label text via JS
            await tabs_frame.evaluate("""
                () => {
                    const titles = Array.from(document.querySelectorAll('.tabs-inner .tabs-title'));
                    const node = titles.find(n => n.textContent.trim().toLowerCase() === 'representatives');
                    if (node) node.closest('.tabs-inner').click();
                }
            """)

            # Wait for representative section marker in docInfoFrame
            # We wait on an element id that exists in your sample: PERSONAL_REPRESENTATIVEheader
            await docInfoFrame.wait_for_selector("#PERSONAL_REPRESENTATIVEheader, span.subsectionheader", timeout=10000)
            # Small stability wait
            await asyncio.sleep(0.2)
            return tabs_frame, docInfoFrame
        except Exception as e:
            last_err = e
            # If frame got detached, retry by reacquiring frames
            await asyncio.sleep(0.3)

    raise last_err if last_err else PlaywrightTimeoutError("Failed to select Representatives tab within timeout")

async def extract_representatives(docInfoFrame):
    """
    Parse representative name and address under the 'Personal Representative(s):' subsection.
    Avoid deprecated :contains; use id or heuristics.
    """
    html = await docInfoFrame.content()
    soup = BeautifulSoup(html, "html.parser")

    # Prefer section by known header id; otherwise fall back to scanning even rows
    reps = []

    # Scope to content area by finding the header and taking following rows
    header = soup.select_one("#PERSONAL_REPRESENTATIVEheader")
    if header:
        # Limit search to nearest table following the header
        containing_td = header.find_parent("td")
        if containing_td:
            # Find the next table that contains the evenrow entries
            next_tbl = containing_td.find_parent("table")
            # Traverse forward to find the detailed table with evenrow rows
            target = None
            if next_tbl:
                # Walk forward through siblings searching for a table with evenrow rows
                sib = next_tbl.find_next_sibling()
                while sib:
                    if getattr(sib, "name", "").lower() == "table" and sib.select("tr.evenrow"):
                        target = sib
                        break
                    sib = sib.find_next_sibling()
            block = target if target else soup
        else:
            block = soup
    else:
        block = soup

    even_rows = block.select("tr.evenrow")
    i = 0
    while i < len(even_rows):
        name = even_rows[i].get_text(" ", strip=True)
        addr = ""
        if i + 1 < len(even_rows):
            addr = even_rows[i + 1].get_text(" ", strip=True)
        name = " ".join(name.split()).lstrip("  ").strip()
        addr = " ".join(addr.split()).lstrip("  ").strip()
        if name:
            reps.append({"representative_name": name, "representative_address": addr})
        i += 2

    return reps

async def select_tab_decedent(bodyframe, timeout_s=20):
    """
    Robustly click the 'Decedent & Estate Info' tab.
    Retries if tabs frame detaches due to reload.
    """
    attempt_deadline = time.time() + timeout_s
    last_err = None
    while time.time() < attempt_deadline:
        try:
            _, documentFrame, docInfoFrame = await wait_details_loaded(bodyframe.page, timeout_s)
            tabs_frame = None
            t_deadline = time.time() + 5
            while time.time() < t_deadline and not tabs_frame:
                for f in docInfoFrame.child_frames:
                    if f.name == "tabs":
                        tabs_frame = f
                        break
                if not tabs_frame:
                    await asyncio.sleep(0.1)
            if not tabs_frame:
                tabs_frame = await get_frame_by_url_contains(bodyframe.page, "tabbar.do", 10)

            await tabs_frame.wait_for_load_state("domcontentloaded", timeout=10000)

            # Click Decedent & Estate Info by title text
            await tabs_frame.evaluate("""
                () => {
                    const titles = Array.from(document.querySelectorAll('.tabs-inner .tabs-title'));
                    const node = titles.find(n => n.textContent.trim().toLowerCase().includes('decedent'));
                    if (node) node.closest('.tabs-inner').click();
                }
            """)

            # Wait for a Decedent info marker: filing date label span or correspondent address span
            await docInfoFrame.wait_for_selector("#fieldFILING_DATEspan, #fcaddrCORESPONDENT_ADDRESSspan", timeout=10000)
            await asyncio.sleep(0.2)
            return tabs_frame, docInfoFrame
        except Exception as e:
            last_err = e
            await asyncio.sleep(0.3)

    raise last_err if last_err else PlaywrightTimeoutError("Failed to select Decedent & Estate Info tab within timeout")


async def extract_decedent_info(docInfoFrame):
    """
    Extract Filing Date and decedent address fields from Decedent & Estate Info.
    We'll locate spans by label ids and read sibling cells.
    """
    html = await docInfoFrame.content()
    soup = BeautifulSoup(html, "html.parser")

    def get_value_by_label_id(label_id):
        lab = soup.select_one(f"span#{label_id}")
        if not lab:
            return None
        # label span is inside a td; the next td contains the value
        td = lab.find_parent("td")
        if not td:
            return None
        row = td.find_parent("tr")
        if not row:
            return None
        tds = row.find_all("td")
        # Typically: [pad], [label], [value], [...]
        if len(tds) >= 3:
            return tds[2].get_text(" ", strip=True)
        return None

    filing_date = get_value_by_label_id("fieldFILING_DATEspan") or get_value_by_label_id("fieldFILING_DATEspan".lower())
    # Address lines
    addr = get_value_by_label_id("fcaddrCORESPONDENT_ADDRESSspan") or ""
    city = get_value_by_label_id("fccityCORESPONDENT_ADDRESSspan") or ""
    # State and Zip are within a nested table in the 'State' row
    state_zip_text = ""
    state_row_label = soup.select_one("span#fcstateCORESPONDENT_ADDRESSspan")
    if state_row_label:
        td = state_row_label.find_parent("td")
        row = td.find_parent("tr") if td else None
        if row:
            tds = row.find_all("td")
            # The 'value' td contains nested table with State, 'Zip:' label, and zip value
            if len(tds) >= 3:
                nested = tds[2]
                parts = [p.get_text(" ", strip=True) for p in nested.find_all("td")]
                # Expect parts like [PA, Zip:, 19081]
                state_zip_text = " ".join([p for p in parts if p and p.lower() != "zip:"]).strip()

    decedent_address = ", ".join([x for x in [addr, city, state_zip_text] if x]).strip(", ").strip()
    return {
        "filing_date": filing_date,
        "decedent_address": decedent_address
    }

async def click_back_to_results(page):
    """
    Back to results button lives under:
    bodyframe -> resnavframe
    """
    bodyframe = await get_frame_by_name(page, "bodyframe", 30000)
    resnavframe = None
    # Find by name or URL fragment
    for f in bodyframe.child_frames:
        if f.name == "resnavframe":
            resnavframe = f
            break
    if not resnavframe:
        resnavframe = await get_frame_by_url_contains(page, "navbar.do?page=search.details", 20)

    await resnavframe.wait_for_load_state("domcontentloaded", timeout=15000)
    # Click via JS on the link with onclick parent.executeSearchNav('results')
    await resnavframe.evaluate("""
        () => {
            const a = Array.from(document.querySelectorAll('a.base')).find(el => el.getAttribute('onclick')?.includes("executeSearchNav ('results'"));
            if (a) a.click();
        }
    """)
    await asyncio.sleep(1.0)  # brief wait for results to reappear

async def click_next_results_page(page):
    """
    Next results page link is inside:
    bodyframe -> resultFrame -> subnav (navbar.do?page=search.resultNav.next&subnav=1...)
    """
    bodyframe = await get_frame_by_name(page, "bodyframe", 30000)
    # Find resultFrame first
    resultFrame = None
    for f in bodyframe.child_frames:
        if f.name == "resultFrame":
            resultFrame = f
            break
    if not resultFrame:
        resultFrame = await get_frame_by_url_contains(page, "SearchResultsView.jsp", 20)

    # subnav frame is a child frame that loads navbar.do with subnav=1
    subnav_frame = None
    for f in resultFrame.child_frames:
        if f.url and "navbar.do" in f.url and "subnav=1" in f.url:
            subnav_frame = f
            break
    if not subnav_frame:
        subnav_frame = await get_frame_by_url_contains(page, "navbar.do?page=search.resultNav", 20)

    await subnav_frame.wait_for_load_state("domcontentloaded", timeout=15000)
    # Try to click next
    had_next = await subnav_frame.evaluate("""
        () => {
            const a = Array.from(document.querySelectorAll('a.base')).find(el => el.getAttribute('onclick')?.includes("navigateResults('next'"));
            if (a) { a.click(); return true; }
            return false;
        }
    """)
    await asyncio.sleep(1.0)
    return bool(had_next)

async def deep_scrape_all_results(page):
    """
    Iterate all result pages, open each record, extract reps and decedent info,
    and build a list of dicts.
    """
    all_records = []
    page_index = 1
    while True:
        # Ensure results page visible
        try:
            resultListFrame, anchors = await collect_result_row_links(page)
        except Exception as e:
            print("No results frame found or results not visible:", e)
            break

        # Recompute anchors each loop in case of dynamic rendering
        anchors = await resultListFrame.query_selector_all("a.link[id^='inst'], a.link[onclick*='loadRecord']")
        num_rows = len(anchors)
        print(f"Results page {page_index}: found {num_rows} records")

        for row_idx in range(num_rows):
            # Click the row link
            ok = await click_result_link_by_index(resultListFrame, row_idx)
            if not ok:
                print(f"Skipping row {row_idx}: link not found")
                continue

            # Wait details
            bodyframe, documentFrame, docInfoFrame = await wait_details_loaded(page, 30)

            # Representatives
            await select_tab_representatives(bodyframe, 20)
            reps = await extract_representatives(docInfoFrame)
            # If no reps extracted, still proceed
            # Decedent & Estate Info
            await select_tab_decedent(bodyframe, 20)
            dec = await extract_decedent_info(docInfoFrame)

            # Also try to capture case identifiers from the DocumentInfoView URL
            case_meta = {}
            try:
                for f in page.frames:
                    if f.url and "DocumentInfoView.jsp" in f.url and "caseFileId=" in f.url:
                        from urllib.parse import urlparse, parse_qs
                        qs = parse_qs(urlparse(f.url).query)
                        case_meta = {
                            "caseFileId": (qs.get("caseFileId") or [""])[0],
                            "caseFileNum": (qs.get("caseFileNum") or [""])[0],
                        }
                        break
            except Exception:
                pass

            base_record = {
                "filing_date": dec.get("filing_date"),
                "decedent_address": dec.get("decedent_address"),
                **case_meta
            }

            if reps:
                for r in reps:
                    rec = {**base_record, **r}
                    all_records.append(rec)
            else:
                all_records.append({**base_record, "representative_name": "", "representative_address": ""})

            # Back to results
            await click_back_to_results(page)
            # Wait for results list again before next iteration
            try:
                resultListFrame, _ = await collect_result_row_links(page)
            except Exception:
                # Try a brief wait and retry
                await asyncio.sleep(1.0)
                resultListFrame, _ = await collect_result_row_links(page)

        # Try next page
        has_next = await click_next_results_page(page)
        if not has_next:
            print("No next page button or reached last page.")
            break

        # Wait for next results list to load
        await asyncio.sleep(1.5)
        page_index += 1

    return all_records

def write_monthwise_xlsx(records, out_path):
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

# ---------------
# Entry hook to run the deep scrape after your existing flow
# ---------------
async def run_full_scrape_and_export(scraper):
    """
    Call this after your existing click_search_button() and result load wait.
    """
    page = scraper.page
    all_records = await deep_scrape_all_results(page)
    # Write JSON mirror
    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / "all_records.json").write_text(json.dumps(all_records, ensure_ascii=False, indent=2), encoding="utf-8")
    # Write month-wise XLSX
    xlsx_path = OUT_DIR / "delaware_records_monthwise.xlsx"
    write_monthwise_xlsx(all_records, xlsx_path)


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
            # results = await scraper.parse_results_table()
            # # Write results to file (example)
            # out_file = OUT_DIR / "results.json"
            # out_file.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
            # print("✅ Scrape finished, results written to", out_file)
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













# import asyncio
# from datetime import datetime, timedelta
# import json
# import os
# import pandas as pd
# from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# CHECKPOINT_FILE = "checkpoint.json"
# OUTPUT_FILE = "delaware_county_records.xlsx"

# class DelawareScraper:
#     def _init_(self, base_url: str = "https://delcorowonlineservices.co.delaware.pa.us/countyweb/loginDisplay.action?countyname=DelawarePA"):
#         self.base_url = base_url
#         self.page = None
#         self.context = None
#         self.browser = None

#     # -------------------------
#     # INITIAL STEPS
#     # -------------------------

#     async def goto_login(self, retries: int = 3):
#         for attempt in range(retries):
#             try:
#                 await self.page.goto(self.base_url, timeout=60000)
#                 await self.page.wait_for_selector("input[value=' Login as Guest ']", timeout=30000)
#                 await self.page.click("input[value=' Login as Guest ']")
#                 await self.page.wait_for_url("/main.jsp?countyname=DelawarePA", timeout=60000)
#                 print("✅ Logged in as Guest")
#                 return
#             except PlaywrightTimeoutError as e:
#                 print(f"⚠ Login attempt {attempt+1} failed: {e}")
#                 if attempt == retries - 1:
#                     raise
#                 await asyncio.sleep(3)

#     async def accept_terms(self, retries: int = 3):
#         for attempt in range(retries):
#             try:
#                 frame = await self.page.wait_for_selector("iframe[name='bodyframe']", timeout=60000)
#                 bodyframe = await frame.content_frame()
#                 await bodyframe.wait_for_selector("#accept", timeout=60000)
#                 await bodyframe.click("#accept")
#                 print("✅ Accepted terms")
#                 return
#             except PlaywrightTimeoutError as e:
#                 print(f"⚠ Accept attempt {attempt+1} failed: {e}")
#                 if attempt == retries - 1:
#                     raise
#                 await asyncio.sleep(3)

#     async def click_search_public_records(self, retries: int = 3):
#         for attempt in range(retries):
#             try:
#                 frame = await self.page.wait_for_selector("iframe[name='bodyframe']", timeout=60000)
#                 bodyframe = await frame.content_frame()
#                 await bodyframe.wait_for_selector("#datagrid-row-r1-2-0", timeout=60000)
#                 await bodyframe.click("#datagrid-row-r1-2-0")
#                 print("✅ Clicked 'Search Public Records'")
#                 return
#             except PlaywrightTimeoutError as e:
#                 print(f"⚠ Search click attempt {attempt+1} failed: {e}")
#                 if attempt == retries - 1:
#                     raise
#                 await asyncio.sleep(3)

#     # -------------------------
#     # FRAME HANDLING
#     # -------------------------

#     async def find_criteria_frame(self):
#         max_wait = 60
#         for _ in range(max_wait):
#             for f in self.page.frames:
#                 if "dynCriteria.do" in f.url:
#                     return f
#             await asyncio.sleep(1)
#         raise Exception("⛔ criteriaframe with dynCriteria.do not found")

#     # -------------------------
#     # DATE INPUT
#     # -------------------------

#     async def enter_filing_dates(self, from_date: str, to_date: str, retries: int = 3):
#         print("⏳ Waiting 5 seconds for initial frames to load...")
#         await asyncio.sleep(5)

#         for attempt in range(retries):
#             try:
#                 frame = await self.find_criteria_frame()
#                 await frame.wait_for_load_state("domcontentloaded")
#                 await asyncio.sleep(2)
#                 await frame.wait_for_selector("#elemDateRange", timeout=30000)

#                 # Fill FROM date
#                 from_input = await frame.wait_for_selector("#_easyui_textbox_input7", timeout=30000)
#                 await from_input.fill("")
#                 await from_input.type(from_date)

#                 # Fill TO date
#                 to_input = await frame.wait_for_selector("#_easyui_textbox_input8", timeout=30000)
#                 await to_input.fill("")
#                 await to_input.type(to_date)

#                 print(f"✅ Entered Filing Dates: {from_date} → {to_date}")

#                 # 🔴 FIX: click the <img id="imgSearch"> button instead of the <a>
#                 search_img = await frame.wait_for_selector("#imgSearch", timeout=30000)
#                 await search_img.click()
#                 print("✅ Clicked Search button (imgSearch)")

#                 # Wait for results table
#                 await asyncio.sleep(5)
#                 await frame.wait_for_selector("table.datagrid-btable", timeout=60000)
#                 print("✅ Search results loaded")

#                 return frame  # return frame for scraping

#             except PlaywrightTimeoutError as e:
#                 print(f"⚠ Date entry attempt {attempt+1} failed: {e}")
#                 if attempt == retries - 1:
#                     raise
#                 await asyncio.sleep(3)
#     # -------------------------
#     # SEARCH + RESULTS
#     # -------------------------

#     async def click_search_and_wait(self):
#         frame = await self.find_criteria_frame()
#         search_btn = await frame.wait_for_selector("//*[@id='mainHeader']/span[2]/a[2]", timeout=60000)
#         await search_btn.click()
#         print("✅ Clicked Search")
#         await asyncio.sleep(5)
#         await frame.wait_for_selector("table.datagrid-btable", timeout=60000)
#         return frame

#     async def scrape_results_page(self, frame):
#         file_links = await frame.query_selector_all("a.link")
#         results = []

#         for link in file_links:
#             case_no = await link.inner_text()
#             print(f"➡ Opening case {case_no}")
#             await link.click()
#             await asyncio.sleep(3)
#             details = await self.scrape_case_details(frame)
#             if details:
#                 results.append(details)
#             back_btn = await frame.wait_for_selector("a[onclick*='executeSearchNav']", timeout=60000)
#             await back_btn.click()
#             await asyncio.sleep(3)
#             await frame.wait_for_selector("table.datagrid-btable", timeout=60000)

#         return results

#     async def scrape_case_details(self, frame):
#         try:
#             reps_tab = await frame.query_selector("span.tabs-title:text('Representatives')")
#             if not reps_tab:
#                 return None
#             await reps_tab.click()
#             await asyncio.sleep(2)

#             reps_table = await frame.query_selector("table.base")
#             if not reps_table:
#                 return None
#             tds = await reps_table.query_selector_all("td")
#             rep_name = (await tds[1].inner_text()).strip() if len(tds) > 1 else ""
#             rep_addr = (await tds[3].inner_text()).strip() if len(tds) > 3 else ""

#             dec_tab = await frame.query_selector("span.tabs-title:text('Decedent & Estate Info')")
#             if dec_tab:
#                 await dec_tab.click()
#                 await asyncio.sleep(2)

#             case_no = await self.extract_text_after_label(frame, "Case File No.:")
#             filing_date = await self.extract_text_after_label(frame, "Filing Date:")
#             death_date = await self.extract_text_after_label(frame, "Date of Death:")
#             dec_addr = await self.extract_text_after_label(frame, "Address:")

#             return {
#                 "CaseFileNo": case_no,
#                 "FilingDate": filing_date,
#                 "DateOfDeath": death_date,
#                 "RepresentativeName": rep_name,
#                 "RepresentativeAddress": rep_addr,
#                 "DecedentAddress": dec_addr,
#             }
#         except Exception as e:
#             print(f"⚠ Error scraping details: {e}")
#             return None

#     async def extract_text_after_label(self, frame, label):
#         cells = await frame.query_selector_all("td")
#         for i, el in enumerate(cells):
#             text = (await el.inner_text()).strip()
#             if text.startswith(label):
#                 return (await cells[i+1].inner_text()).strip()
#         return ""

#     # -------------------------
#     # FULL SCRAPE LOOP
#     # -------------------------

#     async def scrape_month(self, from_date: str, to_date: str):
#         await self.enter_filing_dates(from_date, to_date)
#         frame = await self.click_search_and_wait()

#         month_data = []
#         while True:
#             results = await self.scrape_results_page(frame)
#             month_data.extend(results)
#             next_btn = await frame.query_selector("a[onclick*='navigateResults']")
#             if not next_btn:
#                 break
#             await next_btn.click()
#             await asyncio.sleep(3)
#             await frame.wait_for_selector("table.datagrid-btable", timeout=60000)

#         return month_data

# # -------------------------
# # MAIN RUNNER
# # -------------------------

# async def main():
#     today = datetime.today()
#     start_month = datetime(2025, 1, 1)

#     if os.path.exists(CHECKPOINT_FILE):
#         with open(CHECKPOINT_FILE, "r") as f:
#             ckpt = json.load(f)
#         start_month = datetime.strptime(ckpt["last_month"], "%Y-%m")
#     else:
#         ckpt = {}

#     async with async_playwright() as pw:
#         browser = await pw.chromium.launch(headless=False)
#         context = await browser.new_context()
#         page = await context.new_page()

#         scraper = DelawareScraper()
#         scraper.page = page
#         scraper.context = context
#         scraper.browser = browser

#         await scraper.goto_login()
#         await scraper.accept_terms()
#         await scraper.click_search_public_records()

#         month = start_month
#         while month <= today:
#             from_date = month.strftime("%m/01/%Y")
#             to_date = (month + timedelta(days=32)).replace(day=1) - timedelta(days=1)
#             to_date_str = to_date.strftime("%m/%d/%Y")

#             print(f"📅 Scraping {month.strftime('%B %Y')}")
#             data = await scraper.scrape_month(from_date, to_date_str)

#             df = pd.DataFrame(data)
#             mode = "a" if os.path.exists(OUTPUT_FILE) else "w"
#             with pd.ExcelWriter(OUTPUT_FILE, mode=mode, engine="openpyxl", if_sheet_exists="replace") as writer:
#                 sheet_name = month.strftime("%Y_%m")
#                 df.to_excel(writer, sheet_name=sheet_name, index=False)

#             ckpt["last_month"] = month.strftime("%Y-%m")
#             with open(CHECKPOINT_FILE, "w") as f:
#                 json.dump(ckpt, f)

#             month = (month + timedelta(days=32)).replace(day=1)

#         await browser.close()


# if _name_ == "_main_":
#     asyncio.run(main())