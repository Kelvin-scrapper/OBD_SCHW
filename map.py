"""
Schwab Data Mapper - Universal Edition
Extracts data from downloaded PDFs and maps them to the standardized output format.
Enhanced with intelligent table detection and fixed output column order.
"""

import os
import re
from pathlib import Path
import pandas as pd
import PyPDF2
from datetime import datetime


class SchwabDataMapper:
    """Extract and map data from Schwab PDFs according to the runbook."""

    # Fixed output column order - NEVER CHANGES
    OUTPUT_COLUMNS = [
        'Unnamed: 0',
        'USA.OBD.SCHWAB.ASSETS.TOTAL.M',
        'USA.OBD.SCHWAB.TOTALCLIENTASSETS.M',
        'USA.OBD.SCHWAB.ASSETS.NETNEW.M',
        'USA.OBD.SCHWAB.NETMARKETGAINS.M',
        'USA.OBD.SCHWAB.ASSETSCORENETNEW.M',
        'USA.OBD.SCHWAB.CLIENTCASH.M',
        'USA.OBD.SCHWAB.CASHPERCENT.M',
        'USA.OBD.SCHWAB.CASHPERCENTQ.M',
        'USA.OBD.SCHWAB.NETBUY.MUTUALFUNDS.M',
        'USA.OBD.SCHWAB.NETBUY.EXCHANGETRADEDFUNDS.M',
        'USA.OBD.SCHWAB.NETBUY.MONEYMARKETFUNDS.M',
        'USA.OBD.SCHWAB.CLIENTACCOUNTS.ACTIVEACCOUNTS.M',
        'USA.OBD.SCHWAB.CLIENTACCOUNTS.BANKINGACCOUNTS.M',
        'USA.OBD.SCHWAB.CLIENTACCOUNTS.WORKPLANPARTAC.M',
        'USA.OBD.SCHWAB.CLIENTACTIVITY.NEWACCOUNTS.M',
        'USA.OBD.SCHWAB.CLIENTACTIVITY.INBOUNDCALLS.M',
        'USA.OBD.SCHWAB.CLIENTACTIVITY.WEBLOGINS.M',
        'USA.OBD.SCHWAB.CLIENTACTIVITY.ACCOUNTATTRITION.M',
        'USA.OBD.SCHWAB.CLIENTACTIVITY.AVERAGETREDS.M',
        'USA.OBD.SCHWAB.LARGECAPITALIZATIONSTOCK.M',
        'USA.OBD.SCHWAB.MUTUALFUND.SMALL.M',
        'USA.OBD.SCHWAB.MUTUALFUND.INTERNATIONAL.M',
        'USA.OBD.SCHWAB.MUTUALFUND.SPECIALIZED.M',
        'USA.OBD.SCHWAB.MUTUALFUND.TOTALEQUITIES.M',
        'USA.OBD.SCHWAB.MUTUALFUND.HYBRID.M',
        'USA.OBD.SCHWAB.MUTUALFUND.TAXABLEBOND.M',
        'USA.OBD.SCHWAB.MUTUALFUND.TAXFREEBOND.M',
        'USA.OBD.SCHWAB.MUTUALFUND.TOTALBONDS.M',
        'USA.OBD.SCHWAB.MUTUALFUND.DOMESTICGROWTH.M',
        'USA.OBD.SCHWAB.MARGINLOANBALANCES.M'
    ]

    # Hardcoded field mappings based on runbook
    FIELD_MAPPINGS = {
        # Column headers from the existing data file
        'USA.OBD.SCHWAB.ASSETS.TOTAL.M': {
            'description': 'Charles Schwab:\n  Total Client Assets:\n  $ Bln.',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Beginning Client Assets',
            'unit': 'billions'
        },
        'USA.OBD.SCHWAB.TOTALCLIENTASSETS.M': {
            'description': 'Charles Schwab:\n  Total Client Assets:\n  $ Bln.\n  (Quarterly)',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Total Client Assets (at month end)',
            'unit': 'billions',
            'note': 'Use only last month of reported quarter'
        },
        'USA.OBD.SCHWAB.ASSETS.NETNEW.M': {
            'description': 'Charles Schwab:\n  Net New Assets:\n  $ Bln.',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Net New Assets',
            'unit': 'billions'
        },
        'USA.OBD.SCHWAB.NETMARKETGAINS.M': {
            'description': 'Charles Schwab:\n  Net Market Gains/Losses:\n  $ Bln.',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Net Market Gains (Losses)',
            'unit': 'billions'
        },
        'USA.OBD.SCHWAB.ASSETSCORENETNEW.M': {
            'description': 'Charles Schwab:\n  Core Net New Assets:\n  $ Bln.',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Core Net New Assets',
            'unit': 'billions'
        },
        'USA.OBD.SCHWAB.CLIENTCASH.M': {
            'description': 'Charles Schwab:\n  Client Cash:\n  $ Bln.\n  Calc',
            'source': 'calculated',
            'formula': 'Total Client Assets * Cash as % of Client Assets / 100',
            'dependencies': ['USA.OBD.SCHWAB.ASSETS.TOTAL.M', 'USA.OBD.SCHWAB.CASHPERCENT.M'],
            'unit': 'billions'
        },
        'USA.OBD.SCHWAB.CASHPERCENT.M': {
            'description': 'Charles Schwab:\n  Cash as % of Client Assets:\n  %',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Client Cash as a Percentage of Client Assets',
            'unit': 'percentage'
        },
        'USA.OBD.SCHWAB.CASHPERCENTQ.M': {
            'description': 'Charles Schwab:\n  Cash as % of Client Assets:\n  %\n  (Quarterly)',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Client Cash as a Percentage of Client Assets',
            'unit': 'percentage',
            'note': 'Use only last month of reported quarter'
        },
        'USA.OBD.SCHWAB.NETBUY.MUTUALFUNDS.M': {
            'description': 'Charles Schwab:\n  Net Buy (Sell) Activity:\n  Mutual Funds (5):\n  $ Mln',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Mutual Funds',
            'unit': 'millions',
            'section': 'Net Buy (Sell) Activity'
        },
        'USA.OBD.SCHWAB.NETBUY.EXCHANGETRADEDFUNDS.M': {
            'description': 'Charles Schwab:\n  Net Buy (Sell) Activity:\n  Exchange-Traded Funds (6):\n  $ Mln',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Exchange-Traded Funds',
            'unit': 'millions',
            'section': 'Net Buy (Sell) Activity'
        },
        'USA.OBD.SCHWAB.NETBUY.MONEYMARKETFUNDS.M': {
            'description': 'Charles Schwab:\n  Net Buy (Sell) Activity:\n  Money Market Funds:\n  $ Mln',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Money Market Funds',
            'unit': 'millions',
            'section': 'Net Buy (Sell) Activity'
        },
        'USA.OBD.SCHWAB.CLIENTACCOUNTS.ACTIVEACCOUNTS.M': {
            'description': 'Charles Schwab:\n  Client Accounts:\n  Active Brokerage Accounts:\n  Thousands',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Active Brokerage Accounts',
            'unit': 'thousands',
            'section': 'Client Accounts'
        },
        'USA.OBD.SCHWAB.CLIENTACCOUNTS.BANKINGACCOUNTS.M': {
            'description': 'Charles Schwab:\n  Client Accounts:\n  Banking Accounts:\n  Thousands',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Banking Accounts',
            'unit': 'thousands',
            'section': 'Client Accounts'
        },
        'USA.OBD.SCHWAB.CLIENTACCOUNTS.WORKPLANPARTAC.M': {
            'description': 'Charles Schwab:\n  Client Accounts:\n  Workplace Plan Participant Accounts:\n  Thousands',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Workplace Plan Participant Accounts',
            'unit': 'thousands',
            'section': 'Client Accounts'
        },
        'USA.OBD.SCHWAB.CLIENTACTIVITY.NEWACCOUNTS.M': {
            'description': 'Charles Schwab:\n  Client Activity:\n  New Brokerage Accounts:\n  Thousands',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'New Brokerage Accounts',
            'unit': 'thousands',
            'section': 'Client Activity'
        },
        'USA.OBD.SCHWAB.CLIENTACTIVITY.INBOUNDCALLS.M': {
            'description': 'Charles Schwab:\n  Client Activity:\n  Inbound Calls:\n  Thousands',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Inbound Calls',
            'unit': 'thousands',
            'section': 'Client Activity',
            'note': 'Field not found in current reports'
        },
        'USA.OBD.SCHWAB.CLIENTACTIVITY.WEBLOGINS.M': {
            'description': 'Charles Schwab:\n  Client Activity:\n  Web Logins:\n  Thousands',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Web Logins',
            'unit': 'thousands',
            'section': 'Client Activity',
            'note': 'Field not found in current reports'
        },
        'USA.OBD.SCHWAB.CLIENTACTIVITY.ACCOUNTATTRITION.M': {
            'description': 'Charles Schwab:\n  Client Activity:\n  Client Account Attrition:\n  Thousands\n  calc',
            'source': 'calculated',
            'formula': 'Active Brokerage Accounts (current) - Active Brokerage Accounts (prev) - New Brokerage Accounts (current)',
            'dependencies': ['USA.OBD.SCHWAB.CLIENTACCOUNTS.ACTIVEACCOUNTS.M', 'USA.OBD.SCHWAB.CLIENTACTIVITY.NEWACCOUNTS.M'],
            'unit': 'thousands'
        },
        'USA.OBD.SCHWAB.CLIENTACTIVITY.AVERAGETREDS.M': {
            'description': 'Charles Schwab:\n  Client Activity:\n  Daily Average Trades:\n  Thousands\n  calc',
            'source': 'calculated',
            'formula': 'Average of weekly data from 13-week trading report',
            'dependencies': ['13_week_trading_report'],
            'unit': 'thousands'
        },
        'USA.OBD.SCHWAB.LARGECAPITALIZATIONSTOCK.M': {
            'description': 'Charles Schwab:\n  Large Capitalization Stock:\n  $ Mln',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Equities',
            'unit': 'millions',
            'section': 'Mutual Funds and Exchange-Traded Funds',
            'note': 'Part of Net Buys (Sells)'
        },
        'USA.OBD.SCHWAB.MUTUALFUND.SMALL.M': {
            'description': 'Charles Schwab:\n  Mutual Fund and Exchange-Traded Fund Net Buys:\n  Small / Mid Capitalization Stock:\n  $ Mln',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Small/Mid Cap Stock',
            'unit': 'millions',
            'note': 'Breakdown not available in current format'
        },
        'USA.OBD.SCHWAB.MUTUALFUND.INTERNATIONAL.M': {
            'description': 'Charles Schwab:\n  Mutual Fund and Exchange-Traded Fund Net Buys:\n  International:\n  $ Mln',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'International',
            'unit': 'millions',
            'note': 'Breakdown not available in current format'
        },
        'USA.OBD.SCHWAB.MUTUALFUND.SPECIALIZED.M': {
            'description': 'Charles Schwab:\n  Mutual Fund and Exchange-Traded Fund Net Buys:\n  Specialized:\n  $ Mln',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Specialized',
            'unit': 'millions',
            'note': 'Breakdown not available in current format'
        },
        'USA.OBD.SCHWAB.MUTUALFUND.TOTALEQUITIES.M': {
            'description': 'Charles Schwab:\n  Mutual Fund and Exchange-Traded Fund Net Buys:\n  total Equities:\n  $ Mln',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Equities',
            'unit': 'millions',
            'section': 'Mutual Funds and Exchange-Traded Funds Net Buys (Sells)'
        },
        'USA.OBD.SCHWAB.MUTUALFUND.HYBRID.M': {
            'description': 'Charles Schwab:\n  Mutual Fund and Exchange-Traded Fund Net Buys:\n  Hybrid:\n  $ Mln',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Hybrid',
            'unit': 'millions',
            'section': 'Mutual Funds and Exchange-Traded Funds Net Buys (Sells)'
        },
        'USA.OBD.SCHWAB.MUTUALFUND.TAXABLEBOND.M': {
            'description': 'Charles Schwab:\n  Mutual Fund and Exchange-Traded Fund Net Buys:\n  Taxable Bond:\n  $ Mln',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Bonds',
            'unit': 'millions',
            'section': 'Mutual Funds and Exchange-Traded Funds Net Buys (Sells)',
            'note': 'Total bonds, taxable breakdown not available'
        },
        'USA.OBD.SCHWAB.MUTUALFUND.TAXFREEBOND.M': {
            'description': 'Charles Schwab:\n  Mutual Fund and Exchange-Traded Fund Net Buys:\n  Tax-Free Bond:\n  $ Mln',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Tax-Free Bond',
            'unit': 'millions',
            'note': 'Breakdown not available in current format'
        },
        'USA.OBD.SCHWAB.MUTUALFUND.TOTALBONDS.M': {
            'description': 'Charles Schwab:\n  Mutual Fund and Exchange-Traded Fund Net Buys:\n  total Bonds:\n  $ Mln',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Bonds',
            'unit': 'millions',
            'section': 'Mutual Funds and Exchange-Traded Funds Net Buys (Sells)'
        },
        'USA.OBD.SCHWAB.MUTUALFUND.DOMESTICGROWTH.M': {
            'description': 'Charles Schwab:\n  Mutual Fund and Exchange-Traded Fund Net Buys:\n  Domestic Growth:',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Domestic Growth',
            'unit': 'millions',
            'note': 'Field not found in current reports'
        },
        'USA.OBD.SCHWAB.MARGINLOANBALANCES.M': {
            'description': 'Charles Schwab:\n  Margin Loan Balances:\n  $ Bln.',
            'source': 'quarterly_report',
            'location': 'page_9',
            'field': 'Receivables from brokerage clients',
            'unit': 'billions',
            'note': 'Also called Receivables from brokerage clients - net'
        }
    }

    def __init__(self, download_dir='./downloads'):
        """
        Initialize the mapper.

        Args:
            download_dir (str): Directory containing downloaded PDFs
        """
        self.download_dir = Path(download_dir)
        self.data = {}
        self.monthly_activity_page = None  # Will be auto-detected

    def find_latest_files(self):
        """Find the most recent quarterly report, trading activity report, and SMART supplement."""
        # Find quarterly reports (earnings release PDFs)
        quarterly_reports = list(self.download_dir.glob('*earnings_release*.pdf'))
        quarterly_reports.extend(list(self.download_dir.glob('*press_release*.pdf')))

        # Find trading activity reports
        trading_reports = list(self.download_dir.glob('*13-week*.pdf'))

        # Find SMART supplement
        smart_supplements = list(self.download_dir.glob('*SMART*.pdf'))

        if not quarterly_reports:
            raise FileNotFoundError(f"No quarterly reports found in {self.download_dir}")

        # Get most recent files by modification time
        latest_quarterly = max(quarterly_reports, key=lambda p: p.stat().st_mtime)
        latest_trading = max(trading_reports, key=lambda p: p.stat().st_mtime) if trading_reports else None
        latest_smart = max(smart_supplements, key=lambda p: p.stat().st_mtime) if smart_supplements else None

        return latest_quarterly, latest_trading, latest_smart

    def extract_pdf_text(self, pdf_path, page_num=None):
        """
        Extract text from a PDF file.

        Args:
            pdf_path (Path): Path to PDF file
            page_num (int): Specific page number (0-indexed), or None for all pages

        Returns:
            str: Extracted text
        """
        with open(pdf_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)

            if page_num is not None:
                return reader.pages[page_num].extract_text()
            else:
                return '\n'.join(page.extract_text() for page in reader.pages)

    def find_monthly_activity_page(self, pdf_path):
        """
        Intelligently find the page containing the Monthly Activity Report.

        Args:
            pdf_path (Path): Path to quarterly report PDF

        Returns:
            int: Page index (0-based) containing monthly activity data, or None if not found
        """
        with open(pdf_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)

            # Patterns that indicate the Monthly Activity Report page
            indicators = [
                r'Beginning Client Assets',
                r'Total Client Assets \(at month end\)',
                r'Net New Assets',
                r'Active Brokerage Accounts',
                r'Sep\s+Oct\s+Nov\s+Dec\s+Jan\s+Feb\s+Mar',  # Month headers
            ]

            # Search through all pages
            for page_idx in range(len(reader.pages)):
                text = reader.pages[page_idx].extract_text()

                # Check if page contains multiple indicators
                match_count = sum(1 for pattern in indicators if re.search(pattern, text))

                if match_count >= 3:  # Page must match at least 3 indicators
                    print(f"[+] Monthly Activity Report detected on page {page_idx + 1}")
                    return page_idx

            print("[-] Could not auto-detect Monthly Activity Report page, defaulting to page 9")
            return 8  # Default to page 9 (index 8)

    def find_financial_highlights_page(self, pdf_path):
        """
        Find the page containing Financial and Operating Highlights table.

        Args:
            pdf_path (Path): Path to quarterly report PDF

        Returns:
            int: Page index (0-based) containing financial highlights, or None if not found
        """
        with open(pdf_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)

            # Patterns that indicate the Financial Highlights page
            indicators = [
                r'Financial and Operating Highlights',
                r'Receivables from brokerage clients',
                r'Financial Condition \(at quarter end',
                r'Net Revenues',
            ]

            # Search through all pages (typically page 1-3)
            for page_idx in range(min(5, len(reader.pages))):
                text = reader.pages[page_idx].extract_text()

                # Check if page contains multiple indicators
                match_count = sum(1 for pattern in indicators if re.search(pattern, text))

                if match_count >= 2:  # Page must match at least 2 indicators
                    print(f"[+] Financial Highlights detected on page {page_idx + 1}")
                    return page_idx

            print("[-] Could not auto-detect Financial Highlights page, defaulting to page 1")
            return 0  # Default to page 1 (index 0)

    def extract_quarterly_margin_balances(self, pdf_path):
        """
        Extract quarterly Receivables from brokerage clients from Financial Highlights.

        Args:
            pdf_path (Path): Path to quarterly report PDF

        Returns:
            dict: Quarterly margin balances {quarter_key: value_in_billions}
        """
        page_idx = self.find_financial_highlights_page(pdf_path)
        text = self.extract_pdf_text(pdf_path, page_num=page_idx)

        quarterly_margins = {}
        lines = text.split('\n')

        # Find the line with "Receivables from brokerage clients"
        for line in lines:
            if 'Receivables from brokerage clients' in line and 'net' in line:
                line_clean = re.sub(r'\s+', ' ', line.strip())

                # Extract numeric values (format: XX.X for billions)
                # Pattern matches numbers like 93.8, 82.8, 84.4, 85.4, 74.0
                values = re.findall(r'\b\d{1,3}\.\d\b', line_clean)

                if len(values) >= 5:  # Expect 5 quarters of data
                    # Values are in order: Q3-25, Q2-25, Q1-25, Q4-24, Q3-24
                    # Need to map to actual quarter keys
                    print(f"[+] Found {len(values)} quarterly margin values: {values}")

                    # We'll map these when we know the actual quarters from monthly data
                    # Store as raw values for now
                    return [float(v) for v in values]

        print("[-] Could not extract margin balances from Financial Highlights")
        return []

    def parse_monthly_activity_report(self, text):
        """
        Parse the Monthly Activity Report from page 9 of the quarterly report.

        Args:
            text (str): Extracted text from the PDF

        Returns:
            dict: Parsed monthly data with month keys (e.g., '2025-09')
        """
        monthly_data = {}
        lines = text.split('\n')

        # Find the month header line
        month_line_idx = None
        for idx, line in enumerate(lines):
            if 'Sep Oct Nov Dec Jan Feb Mar Apr May Jun Jul Aug Sep' in line or \
               re.search(r'(Sep|Oct|Nov|Dec|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug)\s+(Oct|Nov|Dec|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep)', line):
                month_line_idx = idx
                break

        if month_line_idx is None:
            print("[-] Could not locate month headers")
            return monthly_data

        # Parse month names
        month_line = lines[month_line_idx]
        months = re.findall(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b', month_line)

        print(f"[+] Found {len(months)} months: {months}")

        # Map month names to numbers
        month_to_num = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
        }

        # Build month keys with year detection
        month_keys = []
        # Determine starting year: first month is from 2024
        year = 2024
        prev_month_num = month_to_num[months[0]]  # Start from first month's number

        for month_name in months:
            month_num = month_to_num[month_name]
            # Detect year rollover (when month number decreases significantly)
            if month_num < prev_month_num and prev_month_num - month_num > 6:
                year += 1
            month_keys.append(f"{year}-{month_num:02d}")
            prev_month_num = month_num

        print(f"[+] Month keys: {month_keys}")

        # Extract data for each row - using line-by-line parsing
        data_rows = {
            'Beginning Client Assets': [],
            'Net New Assets': [],
            'Net Market Gains (Losses)': [],
            'Total Client Assets (at month end)': [],
            'Core Net New Assets': [],
            'Active Brokerage Accounts': [],
            'Banking Accounts': [],
            'Workplace Plan Participant Accounts': [],
            'New Brokerage Accounts': [],
            'Client Cash as a Percentage of Client Assets': [],
            'Mutual Funds': [],
            'Exchange-Traded Funds': [],
            'Money Market Funds': [],
            'Equities': [],
            'Hybrid': [],
            'Bonds': []
        }

        # Track if we're in the Net Buy (Sell) Activity section
        in_net_buy_sell_section = False
        in_net_buys_section = False

        for i, line in enumerate(lines):
            line_clean = re.sub(r'\s+', ' ', line.strip())

            # Section markers
            if 'Net Buy (Sell) Activity' in line:
                in_net_buy_sell_section = True
                in_net_buys_section = False
            elif 'Net Buys (Sells)' in line:
                in_net_buys_section = True
                in_net_buy_sell_section = False

            # Parse data lines
            if line_clean.startswith('Beginning Client Assets'):
                values = re.findall(r'[\d,]+\.[\d]+', line_clean)
                data_rows['Beginning Client Assets'] = [float(v.replace(',', '')) for v in values[:len(months)]]

            elif line_clean.startswith('Net New Assets') and 'Core' not in line_clean:
                values = re.findall(r'\([\d,]+\.[\d]+\)|[\d,]+\.[\d]+', line_clean)
                data_rows['Net New Assets'] = [self._parse_value(v) for v in values[:len(months)]]

            elif line_clean.startswith('Net Market Gains'):
                values = re.findall(r'\([\d,]+\.[\d]+\)|[\d,]+\.[\d]+', line_clean)
                data_rows['Net Market Gains (Losses)'] = [self._parse_value(v) for v in values[:len(months)]]

            elif line_clean.startswith('Total Client Assets (at month end)'):
                values = re.findall(r'[\d,]+\.[\d]+', line_clean)
                data_rows['Total Client Assets (at month end)'] = [float(v.replace(',', '')) for v in values[:len(months)]]

            elif line_clean.startswith('Core Net New Assets'):
                values = re.findall(r'[\d,]+\.[\d]+', line_clean)
                data_rows['Core Net New Assets'] = [float(v.replace(',', '')) for v in values[:len(months)]]

            elif line_clean.startswith('Active Brokerage Accounts'):
                # Extract all numbers, filter out small ones (percentages)
                values = re.findall(r'[\d,]+', line_clean)
                values = [v for v in values if len(v.replace(',', '')) == 5][:len(months)]
                data_rows['Active Brokerage Accounts'] = [float(v.replace(',', '')) for v in values]

            elif line_clean.startswith('Banking Accounts'):
                values = re.findall(r'[\d,]+', line_clean)
                values = [v for v in values if 1000 <= int(v.replace(',', '')) <= 5000][:len(months)]
                data_rows['Banking Accounts'] = [float(v.replace(',', '')) for v in values]

            elif line_clean.startswith('Workplace Plan Participant Accounts'):
                values = re.findall(r'[\d,]+', line_clean)
                values = [v for v in values if 5000 <= int(v.replace(',', '')) <= 6000][:len(months)]
                data_rows['Workplace Plan Participant Accounts'] = [float(v.replace(',', '')) for v in values]

            elif line_clean.startswith('New Brokerage Accounts'):
                values = re.findall(r'[\d,]+', line_clean)
                values = [v for v in values if 100 <= int(v.replace(',', '')) <= 1000][:len(months)]
                data_rows['New Brokerage Accounts'] = [float(v.replace(',', '')) for v in values]

            elif 'Client Cash as a Percentage' in line_clean:
                values = re.findall(r'[\d]+\.[\d]+%', line_clean)
                data_rows['Client Cash as a Percentage of Client Assets'] = [float(v.replace('%', '')) for v in values]

            elif in_net_buy_sell_section and line_clean.startswith('Mutual Funds'):
                # Skip the footnote reference like "(7)" by matching values with at least 3 digits
                values = re.findall(r'\([\d,]{3,}\)|[\d,]{3,}', line_clean)
                data_rows['Mutual Funds'] = [self._parse_value(v) for v in values[:len(months)]]

            elif in_net_buy_sell_section and 'Exchange-Traded Funds' in line_clean:
                # Skip the footnote reference like "(8)" by matching values with at least 3 digits
                values = re.findall(r'[\d,]{3,}', line_clean)
                data_rows['Exchange-Traded Funds'] = [float(v.replace(',', '')) for v in values[:len(months)]]

            elif in_net_buy_sell_section and 'Money Market Funds' in line_clean:
                # Match values with at least 3 digits to skip footnotes
                values = re.findall(r'\([\d,]{3,}\)|[\d,]{3,}', line_clean)
                data_rows['Money Market Funds'] = [self._parse_value(v) for v in values[:len(months)]]

            elif in_net_buys_section and line_clean.startswith('Equities'):
                values = re.findall(r'\([\d,]+\)|[\d,]+', line_clean)
                data_rows['Equities'] = [self._parse_value(v) for v in values[:len(months)]]

            elif in_net_buys_section and line_clean.startswith('Hybrid'):
                values = re.findall(r'\([\d,]+\)|[\d,]+', line_clean)
                data_rows['Hybrid'] = [self._parse_value(v) for v in values[:len(months)]]

            elif in_net_buys_section and line_clean.startswith('Bonds'):
                values = re.findall(r'[\d,]+', line_clean)
                data_rows['Bonds'] = [float(v.replace(',', '')) for v in values[:len(months)]]

            # Note: Margin balances now extracted from Financial Highlights table instead

        # Organize data by month
        for i, month_key in enumerate(month_keys):
            monthly_data[month_key] = {}
            for field_name, values in data_rows.items():
                if i < len(values):
                    monthly_data[month_key][field_name] = values[i]
                else:
                    monthly_data[month_key][field_name] = None

        return monthly_data

    def _parse_value(self, value_str):
        """Parse a value that might be in parentheses (negative) or positive."""
        value_str = value_str.strip()
        if value_str.startswith('(') and value_str.endswith(')'):
            # Negative value
            return -float(value_str[1:-1].replace(',', ''))
        else:
            return float(value_str.replace(',', ''))

    def parse_trading_activity_report(self, text):
        """
        Parse the 13-week trading activity report.

        Args:
            text (str): Extracted text from the PDF

        Returns:
            dict: Monthly average trades calculated from weekly data
        """
        monthly_averages = {}

        # Extract weekly date ranges and trade values
        date_pattern = r'(\d{1,2}/\d{1,2})-(\d{1,2}/\d{1,2})'
        dates = re.findall(date_pattern, text)

        # Extract trade values
        trades_line = [line for line in text.split('\n') if 'Daily Average Trades' in line]
        if not trades_line:
            print("[-] Could not find Daily Average Trades in trading report")
            return monthly_averages

        trades_values = re.findall(r'[\d,]+', trades_line[0])
        # Filter out the parenthetical note numbers and keep actual trade values
        trades_values = [float(v.replace(',', '')) for v in trades_values if len(v.replace(',', '')) >= 4]

        print(f"[+] Found {len(trades_values)} weekly trade values")

        # Group by month and calculate averages
        # Parse dates to determine which month they belong to
        week_data = []
        for i, (start_date, end_date) in enumerate(dates):
            if i < len(trades_values):
                # Parse the end date to determine the month
                month, day = end_date.split('/')
                # Assume year 2025 for recent data
                month_key = f"2025-{int(month):02d}"
                week_data.append({
                    'month': month_key,
                    'value': trades_values[i]
                })

        # Calculate monthly averages
        months = {}
        for week in week_data:
            month = week['month']
            if month not in months:
                months[month] = []
            months[month].append(week['value'])

        for month, values in months.items():
            monthly_averages[month] = sum(values) / len(values)

        return monthly_averages

    def parse_smart_supplement(self, text):
        """
        Parse the SMART supplement PDF to extract detailed mutual fund breakdowns.

        Args:
            text (str): Extracted text from the SMART supplement PDF

        Returns:
            dict: Monthly data for each fund category (Large Cap, Small/Mid Cap, etc.)
        """
        lines = text.split('\n')

        # Find the month headers line
        month_line = None
        for line in lines:
            if 'Sep Oct Nov Dec Jan Feb Mar Apr May Jun Jul Aug Sep' in line or \
               'Sep' in line and 'Oct' in line and 'Nov' in line and 'Dec' in line and 'Jan' in line:
                month_line = line
                break

        if not month_line:
            print("[-] Could not find month headers in SMART supplement")
            return {}

        # Extract months from the header (should be 13 months)
        # Format: "2024 2025\nSep Oct Nov Dec Jan Feb Mar Apr May Jun Jul Aug Sep"
        # We need to build proper YYYY-MM keys
        months_2024 = ['2024-09', '2024-10', '2024-11', '2024-12']
        months_2025 = ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09']
        all_months = months_2024 + months_2025

        # Initialize result structure
        fund_data = {month: {} for month in all_months}

        # Define the expected field order based on SMART supplement structure
        data_row_index = 0
        expected_field_order = [
            'USA.OBD.SCHWAB.LARGECAPITALIZATIONSTOCK.M',
            'USA.OBD.SCHWAB.MUTUALFUND.SMALL.M',
            'USA.OBD.SCHWAB.MUTUALFUND.INTERNATIONAL.M',
            'USA.OBD.SCHWAB.MUTUALFUND.SPECIALIZED.M',
            'USA.OBD.SCHWAB.MUTUALFUND.TOTALEQUITIES.M',
            'USA.OBD.SCHWAB.MUTUALFUND.HYBRID.M',
            'USA.OBD.SCHWAB.MUTUALFUND.TAXABLEBOND.M',
            'USA.OBD.SCHWAB.MUTUALFUND.TAXFREEBOND.M',
            'USA.OBD.SCHWAB.MUTUALFUND.TOTALBONDS.M'
        ]

        # Parse the data rows
        # First, collect ALL lines with 13 numeric values
        all_data_rows = []
        for line in lines:
            line_clean = line.strip()
            # Check if this is a data row with numeric values
            # Pattern: numbers with optional commas and parentheses for negatives
            if re.match(r'^[\d,\(\)\-\s\.]+$', line_clean) and len(line_clean) > 20:
                # Extract all numeric values from this line
                values = re.findall(r'\([\d,\.]+\)|[\d,\.]+', line_clean)
                if len(values) == 13:
                    all_data_rows.append((line_clean, values))

        # The Net Buys (Sells) data is the LAST 9 rows with 13 values
        # (the earlier rows are for other metrics like Client Assets, Trading Days, etc.)
        net_buys_rows = all_data_rows[-9:] if len(all_data_rows) >= 9 else []

        for line_clean, values in net_buys_rows:
            if data_row_index < len(expected_field_order):
                # Parse values
                parsed_values = []
                for v in values:
                    v_clean = v.replace(',', '').strip()
                    if v_clean.startswith('(') and v_clean.endswith(')'):
                        # Negative value
                        parsed_values.append(-float(v_clean[1:-1]))
                    else:
                        parsed_values.append(float(v_clean))

                # Map to months
                field_name = expected_field_order[data_row_index]
                for i, month in enumerate(all_months):
                    fund_data[month][field_name] = parsed_values[i]

                data_row_index += 1

        print(f"[+] Extracted {data_row_index} fund categories from SMART supplement")

        return fund_data

    def calculate_client_cash(self, total_assets, cash_percent):
        """
        Calculate Client Cash.
        Formula: Total Client Assets * Cash % / 100

        Args:
            total_assets (float): Total client assets in billions
            cash_percent (float): Cash percentage

        Returns:
            float: Client cash in billions
        """
        return total_assets * cash_percent / 100

    def calculate_account_attrition(self, current_active, prev_active, new_accounts):
        """
        Calculate Client Account Attrition.
        Formula: Current Active Accounts - Previous Active Accounts - New Accounts

        Args:
            current_active (float): Current month active accounts (thousands)
            prev_active (float): Previous month active accounts (thousands)
            new_accounts (float): New accounts this month (thousands)

        Returns:
            float: Account attrition (thousands)
        """
        return current_active - prev_active - new_accounts

    def extract_all_data(self):
        """
        Main extraction method to get all data from PDFs.

        Returns:
            pd.DataFrame: Extracted data in the standardized format
        """
        print("\n=== Schwab Data Mapper ===\n")

        # Find latest files
        quarterly_pdf, trading_pdf, smart_pdf = self.find_latest_files()
        print(f"[+] Quarterly report: {quarterly_pdf.name}")
        if trading_pdf:
            print(f"[+] Trading report: {trading_pdf.name}")
        if smart_pdf:
            print(f"[+] SMART supplement: {smart_pdf.name}")

        # Extract quarterly margin balances from Financial Highlights
        print("\n[*] Extracting quarterly margin balances from Financial Highlights...")
        quarterly_margin_values = self.extract_quarterly_margin_balances(quarterly_pdf)

        # Extract quarterly report data (auto-detect Monthly Activity Report page)
        print("\n[*] Locating Monthly Activity Report...")
        monthly_page_idx = self.find_monthly_activity_page(quarterly_pdf)

        print("\n[*] Extracting monthly report data...")
        quarterly_text = self.extract_pdf_text(quarterly_pdf, page_num=monthly_page_idx)
        monthly_data = self.parse_monthly_activity_report(quarterly_text)

        print(f"[+] Extracted data for {len(monthly_data)} months")

        # Extract trading activity data
        trading_averages = {}
        if trading_pdf:
            print("\n[*] Extracting trading activity data...")
            trading_text = self.extract_pdf_text(trading_pdf)
            trading_averages = self.parse_trading_activity_report(trading_text)
            print(f"[+] Extracted trading data for {len(trading_averages)} months")

        # Extract SMART supplement data
        smart_data = {}
        if smart_pdf:
            print("\n[*] Extracting SMART supplement data...")
            smart_text = self.extract_pdf_text(smart_pdf)
            smart_data = self.parse_smart_supplement(smart_text)
            print(f"[+] Extracted SMART data for {len(smart_data)} months")

        # Build the final dataset with FIXED column order
        print("\n[*] Building final dataset...")
        all_months = sorted(monthly_data.keys())

        output_data = []
        prev_active_accounts = None

        for month in all_months:
            # Initialize row with fixed column order - all columns start as None
            row = {col: None for col in self.OUTPUT_COLUMNS}
            row['Unnamed: 0'] = month
            data = monthly_data[month]

            # Map each field
            # "Total Client Assets" (Column B) = Use "Total Client Assets (at month end)" from PDF
            total_assets_at_end = data.get('Total Client Assets (at month end)')
            row['USA.OBD.SCHWAB.ASSETS.TOTAL.M'] = total_assets_at_end
            row['USA.OBD.SCHWAB.TOTALCLIENTASSETS.M'] = total_assets_at_end
            row['USA.OBD.SCHWAB.ASSETS.NETNEW.M'] = data.get('Net New Assets')
            row['USA.OBD.SCHWAB.NETMARKETGAINS.M'] = data.get('Net Market Gains (Losses)')
            row['USA.OBD.SCHWAB.ASSETSCORENETNEW.M'] = data.get('Core Net New Assets')

            # Calculate Client Cash
            # Client Cash = B * H / 100, where B is "Total Client Assets (at month end)"
            cash_percent = data.get('Client Cash as a Percentage of Client Assets')
            if total_assets_at_end and cash_percent:
                row['USA.OBD.SCHWAB.CLIENTCASH.M'] = self.calculate_client_cash(total_assets_at_end, cash_percent)
            else:
                row['USA.OBD.SCHWAB.CLIENTCASH.M'] = None

            row['USA.OBD.SCHWAB.CASHPERCENT.M'] = cash_percent
            row['USA.OBD.SCHWAB.CASHPERCENTQ.M'] = None  # Will be filled in quarterly pass

            row['USA.OBD.SCHWAB.NETBUY.MUTUALFUNDS.M'] = data.get('Mutual Funds')
            row['USA.OBD.SCHWAB.NETBUY.EXCHANGETRADEDFUNDS.M'] = data.get('Exchange-Traded Funds')
            row['USA.OBD.SCHWAB.NETBUY.MONEYMARKETFUNDS.M'] = data.get('Money Market Funds')

            row['USA.OBD.SCHWAB.CLIENTACCOUNTS.ACTIVEACCOUNTS.M'] = data.get('Active Brokerage Accounts')
            row['USA.OBD.SCHWAB.CLIENTACCOUNTS.BANKINGACCOUNTS.M'] = data.get('Banking Accounts')
            row['USA.OBD.SCHWAB.CLIENTACCOUNTS.WORKPLANPARTAC.M'] = data.get('Workplace Plan Participant Accounts')
            row['USA.OBD.SCHWAB.CLIENTACTIVITY.NEWACCOUNTS.M'] = data.get('New Brokerage Accounts')

            # Fields not available in current reports
            row['USA.OBD.SCHWAB.CLIENTACTIVITY.INBOUNDCALLS.M'] = None
            row['USA.OBD.SCHWAB.CLIENTACTIVITY.WEBLOGINS.M'] = None

            # Calculate Account Attrition
            current_active = data.get('Active Brokerage Accounts')
            new_accounts = data.get('New Brokerage Accounts')
            if prev_active_accounts and current_active and new_accounts:
                row['USA.OBD.SCHWAB.CLIENTACTIVITY.ACCOUNTATTRITION.M'] = self.calculate_account_attrition(
                    current_active, prev_active_accounts, new_accounts
                )
            else:
                row['USA.OBD.SCHWAB.CLIENTACTIVITY.ACCOUNTATTRITION.M'] = None
            prev_active_accounts = current_active

            # Daily Average Trades from trading report
            row['USA.OBD.SCHWAB.CLIENTACTIVITY.AVERAGETREDS.M'] = trading_averages.get(month)

            # Detailed mutual fund breakdowns from SMART supplement
            if month in smart_data and smart_data[month]:
                row['USA.OBD.SCHWAB.LARGECAPITALIZATIONSTOCK.M'] = smart_data[month].get('USA.OBD.SCHWAB.LARGECAPITALIZATIONSTOCK.M')
                row['USA.OBD.SCHWAB.MUTUALFUND.SMALL.M'] = smart_data[month].get('USA.OBD.SCHWAB.MUTUALFUND.SMALL.M')
                row['USA.OBD.SCHWAB.MUTUALFUND.INTERNATIONAL.M'] = smart_data[month].get('USA.OBD.SCHWAB.MUTUALFUND.INTERNATIONAL.M')
                row['USA.OBD.SCHWAB.MUTUALFUND.SPECIALIZED.M'] = smart_data[month].get('USA.OBD.SCHWAB.MUTUALFUND.SPECIALIZED.M')
                row['USA.OBD.SCHWAB.MUTUALFUND.TOTALEQUITIES.M'] = smart_data[month].get('USA.OBD.SCHWAB.MUTUALFUND.TOTALEQUITIES.M')
                row['USA.OBD.SCHWAB.MUTUALFUND.HYBRID.M'] = smart_data[month].get('USA.OBD.SCHWAB.MUTUALFUND.HYBRID.M')
                row['USA.OBD.SCHWAB.MUTUALFUND.TAXABLEBOND.M'] = smart_data[month].get('USA.OBD.SCHWAB.MUTUALFUND.TAXABLEBOND.M')
                row['USA.OBD.SCHWAB.MUTUALFUND.TAXFREEBOND.M'] = smart_data[month].get('USA.OBD.SCHWAB.MUTUALFUND.TAXFREEBOND.M')
                row['USA.OBD.SCHWAB.MUTUALFUND.TOTALBONDS.M'] = smart_data[month].get('USA.OBD.SCHWAB.MUTUALFUND.TOTALBONDS.M')
            else:
                # Fallback to quarterly report aggregate values
                row['USA.OBD.SCHWAB.LARGECAPITALIZATIONSTOCK.M'] = None
                row['USA.OBD.SCHWAB.MUTUALFUND.SMALL.M'] = None
                row['USA.OBD.SCHWAB.MUTUALFUND.INTERNATIONAL.M'] = None
                row['USA.OBD.SCHWAB.MUTUALFUND.SPECIALIZED.M'] = None
                row['USA.OBD.SCHWAB.MUTUALFUND.TOTALEQUITIES.M'] = data.get('Equities')
                row['USA.OBD.SCHWAB.MUTUALFUND.HYBRID.M'] = data.get('Hybrid')
                row['USA.OBD.SCHWAB.MUTUALFUND.TAXABLEBOND.M'] = None
                row['USA.OBD.SCHWAB.MUTUALFUND.TAXFREEBOND.M'] = None
                row['USA.OBD.SCHWAB.MUTUALFUND.TOTALBONDS.M'] = data.get('Bonds')

            row['USA.OBD.SCHWAB.MUTUALFUND.DOMESTICGROWTH.M'] = None

            row['USA.OBD.SCHWAB.MARGINLOANBALANCES.M'] = None  # Will be filled in quarterly pass

            output_data.append(row)

        # Second pass: Apply quarterly logic and calculate Margin Loan Balances
        # For "Total Client Assets (Quarterly)" and "Cash % (Quarterly)":
        # Use the LAST month's value of each quarter for ALL 3 months in that quarter
        #
        # For "Margin Loan Balances":
        # Formula: M - M(prev) - P (similar to Account Attrition)
        # Where M = current month margin, M(prev) = previous month margin, P = adjustment (set to 0)
        print("\n[*] Applying quarterly value logic and calculating Margin Loan Balances...")

        # Group rows by quarter
        quarters = {}
        for row in output_data:
            month_str = row['Unnamed: 0']
            year, month = month_str.split('-')
            year, month = int(year), int(month)

            # Determine quarter (Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec)
            quarter = (month - 1) // 3 + 1
            quarter_key = f"{year}-Q{quarter}"

            if quarter_key not in quarters:
                quarters[quarter_key] = []
            quarters[quarter_key].append(row)

        # Apply quarterly values for Total Client Assets and Cash %
        for quarter_key, quarter_rows in quarters.items():
            if len(quarter_rows) > 0:
                # Get the last month's row in this quarter
                last_month_row = quarter_rows[-1]

                # Get quarterly values from the last month
                quarterly_total_assets = last_month_row['USA.OBD.SCHWAB.TOTALCLIENTASSETS.M']
                quarterly_cash_percent = last_month_row['USA.OBD.SCHWAB.CASHPERCENT.M']

                # Apply to ALL months in this quarter
                for row in quarter_rows:
                    row['USA.OBD.SCHWAB.TOTALCLIENTASSETS.M'] = quarterly_total_assets
                    row['USA.OBD.SCHWAB.CASHPERCENTQ.M'] = quarterly_cash_percent

        # Third pass: Apply quarterly Margin Loan Balances from Financial Highlights
        # Map quarterly values from Financial Highlights to the quarters we have
        print("\n[*] Mapping quarterly margin balances from Financial Highlights...")

        # Sort quarters chronologically
        sorted_quarters = sorted(quarters.keys())

        # The quarterly_margin_values are in reverse chronological order (newest first)
        # We need to reverse them to match our sorted quarters
        if quarterly_margin_values and len(quarterly_margin_values) >= len(sorted_quarters):
            # Reverse to get oldest first
            quarterly_margin_values_ordered = list(reversed(quarterly_margin_values))

            # Take the last N values to match our quarters
            margin_values_to_use = quarterly_margin_values_ordered[-len(sorted_quarters):]

            for i, quarter_key in enumerate(sorted_quarters):
                quarter_rows = quarters[quarter_key]
                quarterly_margin = margin_values_to_use[i]

                # Apply to ALL months in this quarter
                for row in quarter_rows:
                    row['USA.OBD.SCHWAB.MARGINLOANBALANCES.M'] = quarterly_margin

                print(f"  {quarter_key}: ${quarterly_margin}B applied to {len(quarter_rows)} months")
        else:
            print("  [-] Could not map quarterly margin values, using None")
            for quarter_rows in quarters.values():
                for row in quarter_rows:
                    row['USA.OBD.SCHWAB.MARGINLOANBALANCES.M'] = None

        # Create DataFrame with fixed column order
        df = pd.DataFrame(output_data, columns=self.OUTPUT_COLUMNS)
        print(f"[+] Built dataset with {len(df)} rows and {len(df.columns)} columns")
        print(f"[+] Processed {len(quarters)} quarters")

        return df

    def save_to_excel(self, df, output_path='_OBD_SCHW_DATA_OUTPUT.xlsx'):
        """
        Save the extracted data to Excel file with fixed column order.

        Args:
            df (pd.DataFrame): Data to save
            output_path (str): Output file path
        """
        # Add the description row at the top using FIXED column order
        header_row = {col: '' for col in self.OUTPUT_COLUMNS}
        for col in self.OUTPUT_COLUMNS:
            if col in self.FIELD_MAPPINGS:
                header_row[col] = self.FIELD_MAPPINGS[col]['description']

        # Create a new DataFrame with header row using fixed column order
        header_df = pd.DataFrame([header_row], columns=self.OUTPUT_COLUMNS)

        # Ensure df has the correct column order before concatenating
        df = df[self.OUTPUT_COLUMNS]

        final_df = pd.concat([header_df, df], ignore_index=True)

        # Save to Excel
        final_df.to_excel(output_path, sheet_name='DATA', index=False)
        print(f"\n[+] Data saved to: {output_path}")
        print(f"[+] Column order is fixed and consistent")


def main():
    """Main function to run the mapper."""
    import argparse

    parser = argparse.ArgumentParser(description='Extract and map Schwab data from PDFs')
    parser.add_argument('--download-dir', '-d', type=str, default='./downloads',
                      help='Directory containing downloaded PDFs (default: ./downloads)')
    parser.add_argument('--output', '-o', type=str, default='_OBD_SCHW_DATA_OUTPUT.xlsx',
                      help='Output Excel file path (default: _OBD_SCHW_DATA_OUTPUT.xlsx)')

    args = parser.parse_args()

    # Create mapper instance
    mapper = SchwabDataMapper(download_dir=args.download_dir)

    # Extract all data
    df = mapper.extract_all_data()

    # Save to Excel
    mapper.save_to_excel(df, output_path=args.output)

    print("\n[+] Extraction complete!")
    return 0


if __name__ == "__main__":
    exit(main())
