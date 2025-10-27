# Schwab Data Extraction Tool

Automated tool to download and extract financial data from Charles Schwab quarterly reports.

## Overview

This tool consists of three main components:

1. **orchestrator.py** - Main entry point that coordinates the entire process
2. **main.py** - Downloads PDFs from Schwab's investor relations website
3. **map.py** - Extracts and maps data from PDFs to Excel output

## Features

- ✅ **Automatic PDF Downloads**: Downloads quarterly earnings releases, 13-week trading reports, and SMART supplements
- ✅ **Intelligent Table Detection**: Automatically finds data tables even if PDF layout changes
- ✅ **Fixed Output Format**: Column headers never change, ensuring consistency
- ✅ **Universal Design**: Can be adapted for other companies via configuration
- ✅ **Robust Extraction**: Uses multiple pattern-matching strategies for reliability
- ✅ **Quarterly Data Mapping**: Correctly applies quarterly values from Financial Highlights

## Installation

### Prerequisites

```bash
pip install pandas openpyxl PyPDF2 selenium undetected-chromedriver requests
```

### Python Version
- Python 3.8 or higher

## Usage

### Quick Start (Recommended)

Run the complete process with one command:

```bash
python orchestrator.py
```

This will:
1. Download latest PDFs from Schwab's website
2. Extract and map all data fields
3. Generate Excel output file

### Skip Download (Use Existing Files)

If you already have the PDFs downloaded:

```bash
python orchestrator.py --skip-download
```

### Headless Mode

Run browser in headless mode (no visible window):

```bash
python orchestrator.py --headless
```

### Custom Output Location

```bash
python orchestrator.py --output my_data.xlsx
```

### All Options

```bash
python orchestrator.py --help
```

## Output

### Excel File Structure

The output Excel file `_OBD_SCHW_DATA_OUTPUT.xlsx` contains:

- **Row 1**: Field descriptions
- **Row 2+**: Monthly data (13 months)

### Column Order (Fixed)

1. `Unnamed: 0` - Date (YYYY-MM format)
2. `USA.OBD.SCHWAB.ASSETS.TOTAL.M` - Total Client Assets
3. `USA.OBD.SCHWAB.TOTALCLIENTASSETS.M` - Total Client Assets (Quarterly)
4. `USA.OBD.SCHWAB.ASSETS.NETNEW.M` - Net New Assets
5. `USA.OBD.SCHWAB.NETMARKETGAINS.M` - Net Market Gains/Losses
6. `USA.OBD.SCHWAB.ASSETSCORENETNEW.M` - Core Net New Assets
7. `USA.OBD.SCHWAB.CLIENTCASH.M` - Client Cash (calculated)
8. `USA.OBD.SCHWAB.CASHPERCENT.M` - Cash as % of Client Assets
9. `USA.OBD.SCHWAB.CASHPERCENTQ.M` - Cash % (Quarterly)
10. `USA.OBD.SCHWAB.NETBUY.MUTUALFUNDS.M` - Net Buy/Sell Mutual Funds
11. `USA.OBD.SCHWAB.NETBUY.EXCHANGETRADEDFUNDS.M` - Net Buy/Sell ETFs
12. `USA.OBD.SCHWAB.NETBUY.MONEYMARKETFUNDS.M` - Net Buy/Sell Money Market Funds
13. `USA.OBD.SCHWAB.CLIENTACCOUNTS.ACTIVEACCOUNTS.M` - Active Brokerage Accounts
14. `USA.OBD.SCHWAB.CLIENTACCOUNTS.BANKINGACCOUNTS.M` - Banking Accounts
15. `USA.OBD.SCHWAB.CLIENTACCOUNTS.WORKPLANPARTAC.M` - Workplace Plan Accounts
16. `USA.OBD.SCHWAB.CLIENTACTIVITY.NEWACCOUNTS.M` - New Brokerage Accounts
17. `USA.OBD.SCHWAB.CLIENTACTIVITY.INBOUNDCALLS.M` - Inbound Calls (deprecated)
18. `USA.OBD.SCHWAB.CLIENTACTIVITY.WEBLOGINS.M` - Web Logins (deprecated)
19. `USA.OBD.SCHWAB.CLIENTACTIVITY.ACCOUNTATTRITION.M` - Account Attrition (calculated)
20. `USA.OBD.SCHWAB.CLIENTACTIVITY.AVERAGETREDS.M` - Daily Average Trades
21. `USA.OBD.SCHWAB.LARGECAPITALIZATIONSTOCK.M` - Large Cap Stock (from SMART)
22. `USA.OBD.SCHWAB.MUTUALFUND.SMALL.M` - Small/Mid Cap (from SMART)
23. `USA.OBD.SCHWAB.MUTUALFUND.INTERNATIONAL.M` - International (from SMART)
24. `USA.OBD.SCHWAB.MUTUALFUND.SPECIALIZED.M` - Specialized (from SMART)
25. `USA.OBD.SCHWAB.MUTUALFUND.TOTALEQUITIES.M` - Total Equities
26. `USA.OBD.SCHWAB.MUTUALFUND.HYBRID.M` - Hybrid
27. `USA.OBD.SCHWAB.MUTUALFUND.TAXABLEBOND.M` - Taxable Bonds (from SMART)
28. `USA.OBD.SCHWAB.MUTUALFUND.TAXFREEBOND.M` - Tax-Free Bonds (from SMART)
29. `USA.OBD.SCHWAB.MUTUALFUND.TOTALBONDS.M` - Total Bonds
30. `USA.OBD.SCHWAB.MUTUALFUND.DOMESTICGROWTH.M` - Domestic Growth (deprecated)
31. `USA.OBD.SCHWAB.MARGINLOANBALANCES.M` - **Margin Loan Balances** (from Financial Highlights)

**⚠️ IMPORTANT: Column order is fixed and will NEVER change, even in future updates.**

## Data Sources

### Primary Data Sources

1. **Quarterly Earnings Release PDF**
   - Page: Auto-detected (typically page 9)
   - Contains: Monthly Activity Report with 13 months of data

2. **Financial and Operating Highlights Table**
   - Page: Auto-detected (typically page 1-5)
   - Contains: **Quarterly "Receivables from brokerage clients - net"** (Margin Loan Balances)

3. **13-Week Trading Activity Report PDF**
   - Contains: Weekly average trades, aggregated to monthly

4. **SMART Supplement PDF**
   - Contains: Detailed mutual fund category breakdowns

### Data Extraction Logic

#### Margin Loan Balances (Special Handling)
- **Source**: Financial and Operating Highlights table
- **Field**: "Receivables from brokerage clients - net"
- **Values**: Quarterly snapshots (e.g., $93.8B for Q3 2025)
- **Application**: Each quarterly value is applied to ALL 3 months in that quarter

Example:
```
Q3 2025: $93.8B
  → 2025-07: $93.8B
  → 2025-08: $93.8B
  → 2025-09: $93.8B
```

#### Quarterly Fields
These fields use the **last month's value** for all months in the quarter:
- Total Client Assets (Quarterly)
- Cash % (Quarterly)

#### Calculated Fields
- **Client Cash**: `Total Client Assets × Cash % / 100`
- **Account Attrition**: `Current Active - Previous Active - New Accounts`

## Architecture

### File Structure

```
OBD_SCHW/
├── orchestrator.py          # Main coordinator
├── main.py                  # PDF downloader
├── map.py                   # Data extractor (universal)
├── universal_mapper.py      # Generic mapper framework
├── config_schwab.json       # Schwab-specific config
├── downloads/               # Downloaded PDFs
├── _OBD_SCHW_DATA_OUTPUT.xlsx  # Final output
└── orchestrator.log         # Execution log
```

### How It Works

```
orchestrator.py
    ├─> main.py (downloads PDFs)
    │     ├─> Quarterly earnings release
    │     ├─> 13-week trading report
    │     └─> SMART supplement
    │
    └─> map.py (extracts data)
          ├─> Auto-detects table pages
          ├─> Extracts monthly data
          ├─> Extracts quarterly margin balances
          ├─> Calculates derived fields
          └─> Outputs fixed-format Excel
```

## Advanced Usage

### Running Individual Components

#### Download Only
```bash
python main.py --download-dir ./downloads
```

#### Extract Only (from existing PDFs)
```bash
python map.py --download-dir ./downloads --output my_output.xlsx
```

### Logging

The orchestrator creates a log file `orchestrator.log` with detailed execution information.

## Troubleshooting

### "Could not auto-detect Monthly Activity Report page"
- The tool will default to page 9
- If data is still incorrect, check PDF structure manually

### "Missing files" warning
- Some fields may be incomplete
- Check `downloads/` directory for required PDFs

### Unicode errors on Windows
- This has been fixed in the latest version
- If issues persist, use Python 3.9+

### Permission denied when saving Excel
- Close the Excel file if it's open
- Use a different output filename with `--output`

## Maintenance

### Updating for New PDF Formats

If Schwab changes their PDF format:

1. The **intelligent table detection** should handle most changes automatically
2. Check `orchestrator.log` for detection results
3. If needed, update pattern matching in `map.py`:
   - `find_monthly_activity_page()` - For Monthly Activity Report
   - `find_financial_highlights_page()` - For Financial Highlights

### Column Order is SACRED

**Never modify `OUTPUT_COLUMNS` in map.py** - this ensures downstream systems don't break.

## Universal Mapper (Beta)

A generic version `universal_mapper.py` with `config_schwab.json` is included for adapting to other companies. This is experimental.

## License

Internal use only.

## Support

For issues or questions, check the `orchestrator.log` file first for detailed error messages.
