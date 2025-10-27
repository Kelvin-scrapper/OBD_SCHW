"""
Schwab Financial Reports Downloader
Downloads the latest quarterly press release from Schwab's financial reports page
using undetected-chromedriver and Selenium with configurable headless mode.
"""

import os
import time
import requests
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
        
    def setup_driver(self):
        """Set up undetected Chrome driver with appropriate options."""
        options = uc.ChromeOptions()
        
        # Configure headless mode
        if self.headless:
            options.add_argument('--headless=new')  # Use new headless mode for better compatibility
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            
        # General options for better performance and compatibility
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Download preferences
        prefs = {
            'download.default_directory': self.download_dir,
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': True,
            'plugins.always_open_pdf_externally': True,  # Download PDFs instead of opening them
        }
        options.add_experimental_option('prefs', prefs)
        
        # Additional options to avoid detection
        # options.add_experimental_option("excludeSwitches", ["enable-automation"])
        # options.add_experimental_option('useAutomationExtension', False)
        
        # Initialize undetected Chrome driver
        self.driver = uc.Chrome(options=options, version_main=None)
        
        # Execute CDP commands to avoid detection
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": self.driver.execute_script("return navigator.userAgent").replace("Headless", "")
        })
        
        print(f"[+] Chrome driver initialized (Headless: {self.headless})")
        
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
                    text = link.text or link.get_attribute('aria-label') or ''

                    # Only process "PRESS RELEASE AND TABLES" links
                    if 'PRESS RELEASE AND TABLES' not in text:
                        continue

                    # Find parent container to get quarter name and release date
                    # Try multiple parent levels
                    parent = None
                    for i in range(1, 6):
                        try:
                            test_parent = link.find_element(By.XPATH, f"./ancestor::div[{i}]")
                            # Check if this parent contains QUARTER heading and RELEASED text
                            parent_html = test_parent.get_attribute('innerHTML') or ''
                            if 'QUARTER' in parent_html and 'RELEASED' in parent_html:
                                parent = test_parent
                                break
                        except:
                            continue

                    if not parent:
                        continue

                    # Extract quarter name
                    quarter_elem = parent.find_element(By.XPATH, ".//*[contains(text(), 'QUARTER 20')]")
                    quarter_name = quarter_elem.text

                    # Extract release date
                    release_date_elem = parent.find_element(By.XPATH, ".//*[contains(text(), 'RELEASED')]")
                    release_date_str = release_date_elem.text
                    release_date = self.parse_release_date(release_date_str)

                    if release_date:
                        quarterly_reports.append({
                            'quarter': quarter_name,
                            'release_date': release_date,
                            'release_date_str': release_date_str,
                            'text': text,
                            'url': href,
                            'element': link
                        })
                        print(f"  Found: {quarter_name} - {release_date_str}")
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

    def find_trading_activity_report(self):
        """Find and return the 13-week trading activity report from investor relations page."""
        try:
            print("\n[*] Navigating to investor relations page...")
            self.driver.get(self.investor_relations_url)

            # Wait for page to load
            time.sleep(3)
            WebDriverWait(self.driver, 20).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )

            print("[*] Searching for 13-week trading activity report...")

            # Find link containing "13-week" and ".pdf"
            trading_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, '13-week') and contains(@href, '.pdf')]")

            if trading_links:
                link = trading_links[0]  # Get the first (likely most recent) one
                href = link.get_attribute('href')
                text = link.text or link.get_attribute('title') or '13-week trading activity report'

                print(f"[+] Found 13-week trading activity report")
                print(f"  URL: {href}")
                return {
                    'text': text,
                    'url': href,
                    'element': link
                }
            else:
                print("[-] No 13-week trading activity report found")
                return None

        except Exception as e:
            print(f"[-] Error finding trading activity report: {e}")
            import traceback
            traceback.print_exc()
            return None

    def find_smart_supplement(self):
        """Find and return the latest SMART supplement PDF from financial reports page."""
        try:
            print("\n[*] Searching for latest SMART supplement...")

            # We should already be on the financial reports page from previous navigation
            # If not, navigate there
            if 'financial-reports' not in self.driver.current_url:
                print("[*] Navigating to financial reports page...")
                self.driver.get(self.base_url)
                time.sleep(3)
                WebDriverWait(self.driver, 20).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )

            # Find all links containing "SMART" (case insensitive) and ".pdf"
            smart_links = self.driver.find_elements(By.XPATH, "//a[contains(translate(@href, 'SMART', 'smart'), 'smart') and contains(@href, '.pdf')]")

            if not smart_links:
                print("[-] No SMART supplement links found")
                return None

            print(f"[*] Found {len(smart_links)} SMART supplement link(s)")

            # Parse dates from filenames and find the most recent
            supplements = []
            for link in smart_links:
                try:
                    href = link.get_attribute('href')
                    text = link.text or link.get_attribute('title') or ''

                    # Extract date from filename (e.g., "Sept2025", "September2025", "Sep2025")
                    # Pattern: Month name + Year (4 digits)
                    date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*(\d{4})', href, re.IGNORECASE)

                    if date_match:
                        month_str = date_match.group(1)
                        year = int(date_match.group(2))

                        # Convert month string to number
                        month_map = {
                            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
                            'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
                            'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dec': 12
                        }
                        month = month_map.get(month_str.lower()[:3], 1)

                        # Create a comparable date
                        date_value = datetime(year, month, 1)

                        supplements.append({
                            'link': link,
                            'href': href,
                            'text': text,
                            'date': date_value,
                            'date_str': f"{month_str} {year}"
                        })
                        print(f"  Found: {month_str} {year} - {href}")
                    else:
                        # If no date found, still keep it but with lowest priority
                        supplements.append({
                            'link': link,
                            'href': href,
                            'text': text,
                            'date': datetime(1900, 1, 1),
                            'date_str': 'Unknown date'
                        })

                except Exception as e:
                    print(f"  [-] Error parsing link: {e}")
                    continue

            if not supplements:
                print("[-] No SMART supplement with valid date found")
                return None

            # Sort by date (most recent first)
            supplements.sort(key=lambda x: x['date'], reverse=True)

            latest = supplements[0]
            print(f"\n[+] Latest SMART supplement: {latest['date_str']}")
            print(f"  URL: {latest['href']}")

            return {
                'text': latest['text'] or f"SMART supplement {latest['date_str']}",
                'url': latest['href'],
                'element': latest['link']
            }

        except Exception as e:
            print(f"[-] Error finding SMART supplement: {e}")
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

            # Find and download SMART supplement
            smart_supplement_info = self.find_smart_supplement()

            if smart_supplement_info:
                # Download the SMART supplement
                downloaded_file = self.download_file(smart_supplement_info['url'])

                if downloaded_file:
                    print(f"\n[+] Successfully downloaded the SMART supplement!")
                    print(f"  File saved to: {downloaded_file}")
                    downloaded_files.append(downloaded_file)
                else:
                    print("\n[-] Failed to download the SMART supplement")
            else:
                print("\n[-] Could not find the SMART supplement")

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
            # Clean up
            if self.driver:
                print("\nClosing browser...")
                self.driver.quit()


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
    parser.set_defaults(headless=False)
    
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
    exit(main())