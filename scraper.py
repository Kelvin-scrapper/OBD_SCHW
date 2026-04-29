"""
Schwab Financial Reports Downloader
Downloads the latest quarterly press release from Schwab's financial reports page
using undetected-chromedriver and Selenium with configurable headless mode.
"""

import os
import sys
import time
import requests
import subprocess
from pathlib import Path
from datetime import datetime
import re
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


class SchwabReportDownloader:
    """Download Schwab financial reports using undetected Chrome driver."""
    
    def __init__(self, download_dir=None, headless=False):
        """
        Initialize the downloader.

        Args:
            download_dir (str): Directory to save downloads. Defaults to current directory.
            headless (bool): Run browser in headless mode. Default is False.
        """
        self.download_dir = download_dir or os.getcwd()
        self.headless = headless
        self.base_url = "https://www.aboutschwab.com/financial-reports#panel-25-75--5376"
        self.investor_relations_url = "https://www.aboutschwab.com/investor-relations"
        self.driver = None

        # Create download directory if it doesn't exist
        Path(self.download_dir).mkdir(parents=True, exist_ok=True)

    def get_chrome_version(self):
        """Detect installed Chrome version."""
        try:
            if sys.platform == 'win32':
                # Windows - check file system for version folder
                chrome_paths = [
                    r"C:\Program Files\Google\Chrome\Application",
                    r"C:\Program Files (x86)\Google\Chrome\Application",
                    os.path.join(os.environ.get('LOCALAPPDATA', ''), r'Google\Chrome\Application'),
                ]

                for chrome_path in chrome_paths:
                    if os.path.exists(chrome_path):
                        # Look for version folder (e.g., 142.0.7444.177)
                        for item in os.listdir(chrome_path):
                            if re.match(r'^\d+\.\d+\.\d+\.\d+$', item):
                                major_version = item.split('.')[0]
                                print(f"[*] Detected Chrome version: {item} (major: {major_version})")
                                return int(major_version)
            else:
                # Linux/Mac - use command line
                result = subprocess.run(['google-chrome', '--version'],
                                      capture_output=True, text=True)
                version = re.search(r'(\d+)\.', result.stdout)
                if version:
                    major_version = version.group(1)
                    print(f"[*] Detected Chrome version: {major_version}")
                    return int(major_version)
        except Exception as e:
            print(f"[!] Could not detect Chrome version: {e}")

        return None
        
    def setup_driver(self):
        """Set up undetected Chrome driver with automatic version matching."""
        # Get absolute download path
        download_path = os.path.abspath(self.download_dir)

        # Detect Chrome version
        chrome_version = self.get_chrome_version()

        def create_chrome_options():
            """Create a fresh ChromeOptions object with our settings."""
            options = uc.ChromeOptions()

            # Configure headless mode
            if self.headless:
                options.add_argument('--headless=new')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')

            # General options for better performance and compatibility
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--start-maximized')

            # Download preferences
            prefs = {
                'download.default_directory': download_path,
                'download.prompt_for_download': False,
                'download.directory_upgrade': True,
                'safebrowsing.enabled': True,
                'plugins.always_open_pdf_externally': True,
            }
            options.add_experimental_option('prefs', prefs)

            return options

        print("[*] Initializing Chrome driver with automatic version matching...")

        # Try multiple initialization strategies with detected version
        strategies = [
            # Strategy 1: Use detected version with options
            lambda: uc.Chrome(options=create_chrome_options(), version_main=chrome_version, use_subprocess=True),
            # Strategy 2: Use detected version without subprocess
            lambda: uc.Chrome(options=create_chrome_options(), version_main=chrome_version),
            # Strategy 3: Minimal with detected version
            lambda: uc.Chrome(headless=self.headless, version_main=chrome_version, use_subprocess=True),
            # Strategy 4: Let UC auto-detect
            lambda: uc.Chrome(options=create_chrome_options(), use_subprocess=True),
            # Strategy 5: Bare minimum
            lambda: uc.Chrome(headless=self.headless),
        ]

        last_error = None
        for i, strategy in enumerate(strategies, 1):
            try:
                print(f"[*] Trying initialization strategy {i}...")
                self.driver = strategy()
                print(f"[+] Chrome driver initialized successfully using strategy {i} (Headless: {self.headless})")

                # Try to set user agent override (optional)
                try:
                    self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                        "userAgent": self.driver.execute_script("return navigator.userAgent").replace("Headless", "")
                    })
                except:
                    pass  # Ignore if this fails

                return  # Success!

            except Exception as e:
                last_error = e
                error_msg = str(e)
                print(f"[!] Strategy {i} failed: {error_msg[:100]}...")

                if i < len(strategies):
                    print(f"[*] Trying next approach...")
                continue

        # If all strategies failed, raise the last error
        print(f"[-] All initialization strategies failed")
        if last_error:
            raise last_error
        else:
            raise RuntimeError("Failed to initialize Chrome driver")
        
    def navigate_to_page(self):
        """Navigate to the Schwab financial reports page."""
        try:
            print(f"Navigating to: {self.base_url}")
            self.driver.get(self.base_url)
            
            # Wait for page to load
            time.sleep(3)  # Initial wait for dynamic content
            
            # Wait for the page to be fully loaded
            WebDriverWait(self.driver, 20).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            
            print("[+] Successfully navigated to the financial reports page")
            return True
            
        except TimeoutException:
            print("[-] Timeout while loading the page")
            return False
        except Exception as e:
            print(f"[-] Error navigating to page: {e}")
            return False
    
    def parse_release_date(self, date_str):
        """Parse release date string to datetime object for comparison."""
        try:
            # Expected format: "RELEASED MM/DD/YY"
            date_match = re.search(r'(\d{2})/(\d{2})/(\d{2})', date_str)
            if date_match:
                month, day, year = date_match.groups()
                # Convert 2-digit year to 4-digit (assuming 20xx)
                full_year = f"20{year}"
                return datetime.strptime(f"{month}/{day}/{full_year}", "%m/%d/%Y")
        except:
            pass
        return None

    def find_latest_quarterly_report(self):
        """Find and return information about the latest quarterly report based on release date."""
        try:
            # Wait for the quarterly reports section to load
            wait = WebDriverWait(self.driver, 20)

            print("[*] Scanning all quarters to find the latest...")

            # Find all links with "earnings_release.pdf" in href
            all_earnings_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, 'earnings_release.pdf')]")

            print(f"  Found {len(all_earnings_links)} earnings release links")

            quarterly_reports = []

            for link in all_earnings_links:
                try:
                    href = link.get_attribute('href')
                    if not href:
                        continue

                    # Extract quarter and year from URL
                    # Pattern: q{1-4}_{year}_earnings_release.pdf
                    url_match = re.search(r'q([1-4])_(\d{4})_earnings_release', href, re.IGNORECASE)
                    if not url_match:
                        continue

                    quarter_num = int(url_match.group(1))
                    year = int(url_match.group(2))
                    quarter_name = f"Q{quarter_num} QUARTER {year}"
                    release_date_str = f"Q{quarter_num} {year}"
                    # Use end-of-quarter month as approximate release date for sorting
                    release_date = datetime(year, quarter_num * 3, 1)

                    quarterly_reports.append({
                        'quarter': quarter_name,
                        'release_date': release_date,
                        'release_date_str': release_date_str,
                        'url': href,
                        'element': link
                    })
                    print(f"  Found: {quarter_name}")
                except Exception as e:
                    # Skip links that don't have the required structure
                    continue

            if quarterly_reports:
                # Sort by release date (most recent first)
                quarterly_reports.sort(key=lambda x: x['release_date'], reverse=True)

                latest_report = quarterly_reports[0]
                print(f"\n[+] Latest quarterly report: {latest_report['quarter']}")
                print(f"  Released: {latest_report['release_date_str']}")
                print(f"  URL: {latest_report['url']}")
                return latest_report
            else:
                print("[-] No quarterly reports found")
                return None

        except Exception as e:
            print(f"[-] Error finding quarterly report: {e}")
            import traceback
            traceback.print_exc()
            return None

    def find_quarterly_press_release(self):
        """
        Find the 'Press release' PDF link that sits alongside the earnings release
        in the quarterly earnings reports section.

        Strategy: locate the latest earnings_release.pdf anchor, walk up the DOM to
        find a common section/list container, then look for a sibling link whose
        visible text is 'Press release' or whose href contains 'press_release'.
        """
        try:
            print("\n[*] Searching for quarterly press release link...")

            # Reuse the same earnings-release anchors already on the page
            all_earnings_links = self.driver.find_elements(
                By.XPATH, "//a[contains(@href, 'earnings_release')]"
            )

            if not all_earnings_links:
                print("[-] No earnings release links found; cannot locate press release")
                return None

            # Pick the most-recent earnings release link as the anchor
            candidates = []
            for link in all_earnings_links:
                href = link.get_attribute('href') or ''
                m = re.search(r'q([1-4])_(\d{4})_earnings_release', href, re.IGNORECASE)
                if m:
                    q, y = int(m.group(1)), int(m.group(2))
                    candidates.append((datetime(y, q * 3, 1), link))

            if not candidates:
                print("[-] No dated earnings release links found")
                return None

            candidates.sort(key=lambda x: x[0], reverse=True)
            anchor_link = candidates[0][1]

            # Walk up the DOM looking for a container that also holds a 'Press release' link
            for level in range(1, 12):
                try:
                    parent = anchor_link.find_element(By.XPATH, f"./ancestor::*[{level}]")
                    press_links = parent.find_elements(
                        By.XPATH,
                        ".//a["
                        "contains(@href, 'press_release') or "
                        "contains(translate(normalize-space(text()), "
                        "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', "
                        "'abcdefghijklmnopqrstuvwxyz'), 'press release')"
                        "]"
                    )
                    if press_links:
                        href = press_links[0].get_attribute('href') or ''
                        # Skip if it points to the same earnings_release file
                        if 'earnings_release' in href and 'press_release' not in href:
                            continue
                        if not href:
                            continue
                        print(f"[+] Found press release link at ancestor level {level}: {href}")
                        filename = href.split('/')[-1].split('?')[0]  # retain original filename from URL
                        return {'url': href, 'filename': filename, 'element': press_links[0]}
                except Exception:
                    continue

            print("[-] Could not find 'Press release' link in the quarterly section")
            return None

        except Exception as e:
            print(f"[-] Error finding quarterly press release: {e}")
            import traceback
            traceback.print_exc()
            return None

    def find_trading_activity_report(self):
        """Find and return the 13-week trading activity report."""
        # Search both pages for the trading activity PDF
        pages_to_search = [
            ('financial-reports', self.base_url),
            ('investor-relations', self.investor_relations_url),
        ]

        for page_label, page_url in pages_to_search:
            try:
                if page_label not in self.driver.current_url:
                    print(f"\n[*] Navigating to {page_label} page...")
                    self.driver.get(page_url)
                    time.sleep(3)
                    WebDriverWait(self.driver, 20).until(
                        lambda driver: driver.execute_script("return document.readyState") == "complete"
                    )
                else:
                    print(f"\n[*] Already on {page_label} page, searching in place...")

                print("[*] Searching for 13-week trading activity report...")

                # Strategy A: URL contains "13" and ("week" or "trading")
                for xpath in [
                    "//a[contains(@href, '13-week') and contains(@href, '.pdf')]",
                    "//a[contains(@href, '13week') and contains(@href, '.pdf')]",
                    "//a[contains(@href, 'trading') and contains(@href, '13') and contains(@href, '.pdf')]",
                    # Strategy B: link text mentions "13-week" or "13 week"
                    "//a[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '13-week') and contains(@href, '.pdf')]",
                    "//a[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '13 week') and contains(@href, '.pdf')]",
                    # Strategy C: link text mentions "trading activity" and links to a PDF
                    "//a[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'trading activity') and contains(@href, '.pdf')]",
                ]:
                    links = self.driver.find_elements(By.XPATH, xpath)
                    if links:
                        link = links[0]
                        href = link.get_attribute('href')
                        text = link.text or link.get_attribute('title') or '13-week trading activity report'
                        print(f"[+] Found 13-week trading activity report on {page_label} page")
                        print(f"  URL: {href}")
                        return {'text': text, 'url': href, 'element': link}

            except Exception as e:
                print(f"  [!] Error searching {page_label}: {e}")
                continue

        print("[-] No 13-week trading activity report found on any page")
        return None

    def find_monthly_client_metrics(self):
        """Find and return the SMART supplement (monthly client metrics) link."""
        try:
            print("\n[*] Searching for SMART supplement (monthly client metrics)...")

            # Ensure we are on the financial reports page
            if 'financial-reports' not in self.driver.current_url:
                print("[*] Navigating to financial reports page...")
                self.driver.get(self.base_url)
                time.sleep(3)
                WebDriverWait(self.driver, 20).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )

            # Strategy 1: Find SMART PDF links directly (case-insensitive href check)
            for xpath in [
                "//a[contains(@href, 'SMART') and contains(@href, '.pdf')]",
                "//a[contains(@href, 'smart') and contains(@href, '.pdf')]",
                # Also try link text containing "SMART"
                "//a[contains(normalize-space(text()), 'SMART') and contains(@href, '.pdf')]",
            ]:
                try:
                    smart_links = self.driver.find_elements(By.XPATH, xpath)
                    if smart_links:
                        link = smart_links[0]
                        href = link.get_attribute('href')
                        text = link.text or link.get_attribute('title') or 'SMART Supplement'
                        print(f"[+] Found SMART supplement link")
                        print(f"  Link text: {text}")
                        print(f"  URL: {href}")
                        return {'text': text, 'url': href, 'element': link}
                except Exception as e:
                    print(f"  [!] SMART search attempt failed: {e}")

            # Strategy 2: Find the monthly activity report announcement and look for SMART/table link
            # The announcement contains both a press release link and a SMART supplement link;
            # prefer the one whose href or text mentions SMART, "table", or "supplement"
            try:
                announcement_elements = self.driver.find_elements(
                    By.XPATH,
                    "//*[contains(text(), 'monthly activity report for')]"
                )

                if announcement_elements:
                    print(f"[*] Found {len(announcement_elements)} monthly activity announcement(s)")
                    for elem in announcement_elements:
                        try:
                            for ancestor_level in range(1, 8):
                                try:
                                    parent = elem.find_element(By.XPATH, f"./ancestor::div[{ancestor_level}]")
                                    all_pdf_links = parent.find_elements(By.XPATH, ".//a[contains(@href, '.pdf')]")
                                    if all_pdf_links:
                                        # Prefer SMART / table links over press release
                                        preferred = [
                                            l for l in all_pdf_links
                                            if any(kw in (l.get_attribute('href') or '').upper()
                                                   for kw in ['SMART', 'TABLE', 'SUPPLEMENT'])
                                            or any(kw in (l.text or '').upper()
                                                   for kw in ['SMART', 'TABLE', 'SUPPLEMENT'])
                                        ]
                                        link = preferred[0] if preferred else all_pdf_links[0]
                                        href = link.get_attribute('href')
                                        text = link.text or link.get_attribute('title') or 'Monthly Activity Report'
                                        announcement_text = elem.text
                                        month_year_match = re.search(r'for\s+(\w+\s+\d{4})', announcement_text)
                                        month_year = month_year_match.group(1) if month_year_match else "latest"
                                        print(f"[+] Found monthly client metrics for {month_year}")
                                        print(f"  Link text: {text}")
                                        print(f"  URL: {href}")
                                        return {'text': text, 'url': href, 'element': link, 'month_year': month_year}
                                except Exception:
                                    continue
                        except Exception as e:
                            print(f"  [!] Error processing announcement element: {e}")
                            continue
            except Exception as e:
                print(f"  [!] Strategy 2 failed: {e}")

            print("[-] Could not find SMART supplement or monthly activity report")
            return None

        except Exception as e:
            print(f"[-] Error finding monthly client metrics: {e}")
            import traceback
            traceback.print_exc()
            return None

    def download_file(self, file_url, filename=None):
        """
        Download a file from the given URL.
        
        Args:
            file_url (str): URL of the file to download
            filename (str): Optional custom filename. If not provided, uses URL filename.
        """
        try:
            if not filename:
                # Extract filename from URL
                filename = file_url.split('/')[-1]
                if not filename.endswith('.pdf'):
                    filename = f"schwab_report_{datetime.now().strftime('%Y%m%d')}.pdf"
            
            filepath = os.path.join(self.download_dir, filename)
            
            print(f"Downloading: {filename}")
            
            # Method 1: Try using Selenium to click the link (works with download preferences)
            if hasattr(self, 'driver') and self.driver:
                # Execute JavaScript to download the file
                script = f"""
                var link = document.createElement('a');
                link.href = '{file_url}';
                link.download = '{filename}';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
                """
                self.driver.execute_script(script)
                
                # Wait for download to complete
                time.sleep(5)
                
                # Check if file was downloaded
                if os.path.exists(filepath):
                    print(f"[+] Downloaded via Selenium: {filepath}")
                    return filepath
            
            # Method 2: Direct download using requests (fallback)
            print("  Attempting direct download with requests...")
            response = requests.get(file_url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }, timeout=30)
            
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                print(f"[+] Downloaded via requests: {filepath}")
                return filepath
            else:
                print(f"[-] Failed to download. Status code: {response.status_code}")
                return None

        except Exception as e:
            print(f"[-] Error downloading file: {e}")
            return None
    
    def run(self):
        """Main execution method to find and download the latest quarterly report and trading activity."""
        try:
            print("\n=== Schwab Financial Report Downloader ===\n")
            print(f"Download directory: {self.download_dir}")
            print(f"Headless mode: {self.headless}\n")

            # Set up the driver
            self.setup_driver()

            # Navigate to the page
            if not self.navigate_to_page():
                return False

            # Find the latest quarterly report
            report_info = self.find_latest_quarterly_report()

            downloaded_files = []

            if report_info:
                # Download the quarterly report
                downloaded_file = self.download_file(report_info['url'])

                if downloaded_file:
                    print(f"\n[+] Successfully downloaded the latest quarterly report!")
                    print(f"  File saved to: {downloaded_file}")
                    downloaded_files.append(downloaded_file)
                else:
                    print("\n[-] Failed to download the quarterly report")
            else:
                print("\n[-] Could not find the latest quarterly report on the page")

            # Find and download quarterly press release (same section as earnings release)
            press_release_info = self.find_quarterly_press_release()

            if press_release_info:
                downloaded_file = self.download_file(
                    press_release_info['url'],
                    filename=press_release_info.get('filename')
                )
                if downloaded_file:
                    print(f"\n[+] Successfully downloaded the quarterly press release!")
                    print(f"  File saved to: {downloaded_file}")
                    downloaded_files.append(downloaded_file)
                else:
                    print("\n[-] Failed to download the quarterly press release")
            else:
                print("\n[-] Could not find the quarterly press release")

            # Find and download 13-week trading activity report
            trading_report_info = self.find_trading_activity_report()

            if trading_report_info:
                # Download the trading activity report
                downloaded_file = self.download_file(trading_report_info['url'])

                if downloaded_file:
                    print(f"\n[+] Successfully downloaded the 13-week trading activity report!")
                    print(f"  File saved to: {downloaded_file}")
                    downloaded_files.append(downloaded_file)
                else:
                    print("\n[-] Failed to download the trading activity report")
            else:
                print("\n[-] Could not find the 13-week trading activity report")

            # Find and download Monthly Client Metrics (from announcement section)
            monthly_metrics_info = self.find_monthly_client_metrics()

            if monthly_metrics_info:
                # Download the Monthly Client Metrics
                downloaded_file = self.download_file(monthly_metrics_info['url'])

                if downloaded_file:
                    print(f"\n[+] Successfully downloaded the Monthly Client Metrics!")
                    print(f"  File saved to: {downloaded_file}")
                    downloaded_files.append(downloaded_file)
                else:
                    print("\n[-] Failed to download the Monthly Client Metrics")
            else:
                print("\n[-] Could not find the Monthly Client Metrics")

            # Return True if at least one file was downloaded
            if downloaded_files:
                print(f"\n[+] Total files downloaded: {len(downloaded_files)}")
                return True
            else:
                print("\n[-] No files were downloaded")
                return False

        except Exception as e:
            print(f"\n[-] Unexpected error: {e}")
            return False
            
        finally:
            # Clean up - handle Windows cleanup issues gracefully
            if self.driver:
                print("\nClosing browser...")
                try:
                    self.driver.quit()
                except OSError as e:
                    # Ignore Windows handle errors during cleanup
                    if "WinError 6" in str(e) or "handle is invalid" in str(e):
                        print("[!] Browser closed (ignoring cleanup warning)")
                    else:
                        print(f"[!] Warning during browser cleanup: {e}")
                except Exception as e:
                    print(f"[!] Warning during browser cleanup: {e}")


def main():
    """Main function to run the downloader."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Download Schwab financial reports')
    parser.add_argument('--download-dir', '-d', type=str, default='./downloads',
                      help='Directory to save downloaded files (default: ./downloads)')
    parser.add_argument('--headless', '-hl', action='store_true',
                      help='Run browser in headless mode')
    parser.add_argument('--no-headless', dest='headless', action='store_false',
                      help='Run browser in normal mode (default)')
    parser.set_defaults(headless=True)
    
    args = parser.parse_args()
    
    # Create downloader instance
    downloader = SchwabReportDownloader(
        download_dir=args.download_dir,
        headless=args.headless
    )
    
    # Run the downloader
    success = downloader.run()
    
    # Return appropriate exit code
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())