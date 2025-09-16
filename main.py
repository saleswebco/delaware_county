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





    # -----------------------
    # Example parse results hook (fill in selectors for results)
    # -----------------------
    async def parse_results_table(self):
        """
        After searches are triggered, wait for results table (in some frame) and parse with BeautifulSoup.
        Adjust selectors to the actual results markup.
        """
        await asyncio.sleep(2)
        # try to find likely results frame by URL or by content
        results_frame = None
        for f in self.page.frames:
            if "results" in (f.name or "") or "search" in (f.url or ""):
                results_frame = f
                break
        if not results_frame:
            # fallback to bodyframe
            for f in self.page.frames:
                if "bodyframe" in (f.name or "") or "bodyframe" in (f.url or ""):
                    results_frame = f
                    break

        if not results_frame:
            print("⚠ Could not find results frame for parsing. Listing frames:")
            for f in self.page.frames:
                print(" ->", f.url)
            return []

        html = await results_frame.content()
        soup = BeautifulSoup(html, "html.parser")
        # EXAMPLE: adjust to real selectors
        rows = []
        for tr in soup.select("table.results tr"):
            cols = [td.get_text(strip=True) for td in tr.select("td")]
            if cols:
                rows.append(cols)
        print(f"ℹ Parsed {len(rows)} rows (example).")
        return rows


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
            results = await scraper.parse_results_table()
            # Write results to file (example)
            out_file = OUT_DIR / "results.json"
            out_file.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
            print("✅ Scrape finished, results written to", out_file)
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