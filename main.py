"""
Schwab Data Extraction Orchestrator
Coordinates the download and mapping processes.
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import logging


class DataExtractionOrchestrator:
    """Orchestrates the download and data mapping processes."""

    def __init__(self, download_dir='./downloads', output_file=None,
                 headless=True, skip_download=False):
        """
        Initialize the orchestrator.

        Args:
            download_dir (str): Directory for downloaded PDFs
            output_file (str): Output Excel file path
            headless (bool): Run browser in headless mode for downloads
            skip_download (bool): Skip download step, use existing files
        """
        self.download_dir = Path(download_dir)
        datestamp = datetime.today().strftime("%Y%m%d")
        self.output_file = output_file or f'./output/OBD_SHW_DATA_{datestamp}.xlsx'
        self.headless = headless
        self.skip_download = skip_download

        # Setup logging
        self.setup_logging()

    def setup_logging(self):
        """Configure logging for the orchestrator."""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'

        # Setup file handler with UTF-8 encoding
        file_handler = logging.FileHandler('orchestrator.log', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(log_format))

        # Setup console handler with UTF-8 encoding
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(log_format))

        # Force UTF-8 for stdout on Windows
        if sys.platform == 'win32':
            import codecs
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')

        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[console_handler, file_handler]
        )
        self.logger = logging.getLogger(__name__)

    def run_download(self):
        """
        Run scraper.py to download PDFs from Schwab website.

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info("=" * 60)
        self.logger.info("STEP 1: DOWNLOADING PDF FILES")
        self.logger.info("=" * 60)

        try:
            # Build command
            cmd = [
                sys.executable,  # Use current Python interpreter
                'scraper.py',
                '--download-dir', str(self.download_dir)
            ]

            if self.headless:
                cmd.append('--headless')

            self.logger.info(f"Running command: {' '.join(cmd)}")

            # Run scraper.py
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )

            # Log output
            self.logger.info(result.stdout)

            if result.returncode == 0:
                self.logger.info("[SUCCESS] Download completed successfully")
                return True
            else:
                self.logger.error(f"[FAILED] Download failed with return code {result.returncode}")
                if result.stderr:
                    self.logger.error(f"Error: {result.stderr}")
                return False

        except subprocess.CalledProcessError as e:
            self.logger.error(f"[X] Download failed: {e}")
            if e.stdout:
                self.logger.error(f"Output: {e.stdout}")
            if e.stderr:
                self.logger.error(f"Error output: {e.stderr}")
            return False
        except Exception as e:
            self.logger.error(f"[X] Unexpected error during download: {e}")
            return False

    def run_mapper(self):
        """
        Run mapper.py to extract and map data from PDFs.

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info("=" * 60)
        self.logger.info("STEP 2: EXTRACTING AND MAPPING DATA")
        self.logger.info("=" * 60)

        try:
            # Build command
            cmd = [
                sys.executable,
                'mapper.py',
                '--download-dir', str(self.download_dir),
                '--output', self.output_file
            ]

            self.logger.info(f"Running command: {' '.join(cmd)}")

            # Run mapper.py
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )

            # Log output
            self.logger.info(result.stdout)

            if result.returncode == 0:
                self.logger.info(f"[SUCCESS] Data mapping completed successfully")
                self.logger.info(f"[SUCCESS] Output saved to: {self.output_file}")
                return True
            else:
                self.logger.error(f"[FAILED] Data mapping failed with return code {result.returncode}")
                if result.stderr:
                    self.logger.error(f"Error: {result.stderr}")
                return False

        except subprocess.CalledProcessError as e:
            self.logger.error(f"[X] Data mapping failed: {e}")
            if e.stderr:
                self.logger.error(f"Error output: {e.stderr}")
            return False
        except Exception as e:
            self.logger.error(f"[X] Unexpected error during mapping: {e}")
            return False

    def verify_downloads(self):
        """
        Verify that required PDF files exist in download directory.

        Returns:
            bool: True if files exist, False otherwise
        """
        self.logger.info("\n[*] Verifying downloaded files...")

        if not self.download_dir.exists():
            self.logger.error(f"[ERROR] Download directory does not exist: {self.download_dir}")
            return False

        # Check for required file patterns
        required_patterns = [
            '*earnings_release*.pdf',
            '*13-week*.pdf',
            '*SMART*.pdf'
        ]

        missing = []
        for pattern in required_patterns:
            files = list(self.download_dir.glob(pattern))
            if not files:
                missing.append(pattern)
            else:
                self.logger.info(f"  [OK] Found {pattern}: {files[0].name}")

        if missing:
            self.logger.warning(f"  [WARNING] Missing files: {', '.join(missing)}")
            self.logger.warning("  Some data fields may be incomplete")
        else:
            self.logger.info("  [OK] All required files present")

        return True

    def run(self):
        """
        Run the complete orchestration process.

        Returns:
            int: Exit code (0 for success, 1 for failure)
        """
        start_time = datetime.now()

        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("  SCHWAB DATA EXTRACTION ORCHESTRATOR".center(60))
        self.logger.info("=" * 60)
        self.logger.info("")
        self.logger.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Download directory: {self.download_dir}")
        self.logger.info(f"Output file: {self.output_file}")
        self.logger.info("")

        # Step 1: Download PDFs (unless skipped)
        if not self.skip_download:
            if not self.run_download():
                self.logger.error("\n[FAILED] ORCHESTRATION FAILED: Download step failed")
                return 1

            # Verify downloads
            if not self.verify_downloads():
                self.logger.error("\n[FAILED] ORCHESTRATION FAILED: File verification failed")
                return 1
        else:
            self.logger.info("=" * 60)
            self.logger.info("STEP 1: SKIPPED (using existing files)")
            self.logger.info("=" * 60)
            if not self.verify_downloads():
                self.logger.error("\n[FAILED] ORCHESTRATION FAILED: No existing files found")
                return 1

        # Step 2: Extract and map data
        if not self.run_mapper():
            self.logger.error("\n[FAILED] ORCHESTRATION FAILED: Mapping step failed")
            return 1

        # Success summary
        end_time = datetime.now()
        duration = end_time - start_time

        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("[SUCCESS] ORCHESTRATION COMPLETED SUCCESSFULLY")
        self.logger.info("=" * 60)
        self.logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Duration: {duration.total_seconds():.2f} seconds")
        self.logger.info(f"Output file: {self.output_file}")
        self.logger.info("")

        return 0


def main():
    """Main function to run the orchestrator."""
    parser = argparse.ArgumentParser(
        description='Orchestrate Schwab data download and extraction',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full run (download + extract)
  python main.py

  # Skip download, use existing files
  python main.py --skip-download

  # Run with headless browser
  python main.py --headless

  # Custom output location
  python main.py --output my_data.xlsx

  # Custom download directory
  python main.py --download-dir ./my_downloads
        """
    )

    parser.add_argument(
        '--download-dir', '-d',
        type=str,
        default='./downloads',
        help='Directory to save/read downloaded PDFs (default: ./downloads)'
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        default=None,
        help='Output Excel file path (default: ./output/OBD_SHW_DATA_YYYYMMDD.xlsx)'
    )

    parser.add_argument(
        '--headless',
        action='store_true',
        default=True,
        help='Run browser in headless mode for downloads (default: True)'
    )
    parser.add_argument(
        '--no-headless',
        dest='headless',
        action='store_false',
        help='Run browser in normal (visible) mode'
    )

    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip download step and use existing files in download directory'
    )

    args = parser.parse_args()

    # Create and run orchestrator
    orchestrator = DataExtractionOrchestrator(
        download_dir=args.download_dir,
        output_file=args.output,
        headless=args.headless,
        skip_download=args.skip_download
    )

    exit_code = orchestrator.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
