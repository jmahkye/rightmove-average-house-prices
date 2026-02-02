#!/usr/bin/env python3
"""
Rightmove Property Scraper CLI Tool

Scrapes property listings from Rightmove and saves to CSV.
Can be run manually or scheduled to run daily.
"""

import argparse
import csv
import logging
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
import schedule
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rightmove_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def parse_listing_age(date_listed: Optional[str]) -> Optional[float]:
    """
    Parse the date_listed field and return age in days

    Args:
        date_listed: Text like "Added today", "Added yesterday", "Reduced on 15/01/2026"

    Returns:
        Age in days (float), or None if unable to parse
    """
    if not date_listed:
        return None

    date_listed = date_listed.lower().strip()

    # "Added today" or "Reduced today"
    if 'today' in date_listed:
        return 0.0

    # "Added yesterday" or "Reduced yesterday"
    if 'yesterday' in date_listed:
        return 1.0

    # "Added on DD/MM/YYYY" or "Reduced on DD/MM/YYYY"
    date_match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_listed)
    if date_match:
        try:
            day, month, year = int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3))
            listing_date = datetime(year, month, day)
            age = (datetime.now() - listing_date).days
            return float(age)
        except ValueError:
            logger.warning(f"Could not parse date from: {date_listed}")
            return None

    # If we can't parse, return None (we'll be conservative and include it)
    logger.debug(f"Unknown date format: {date_listed}")
    return None


class RightmoveScraper:
    """Scraper for Rightmove property listings"""

    BASE_URL = "https://www.rightmove.co.uk"

    def __init__(self, delay: float = 2.0):
        """
        Initialise scraper

        Args:
            delay: Delay in seconds between requests (be respectful!)
        """
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-GB,en;q=0.9',
            'Referer': 'https://www.rightmove.co.uk/'
        })

    def search_properties(self, search_url: str, max_pages: int = 5) -> List[Dict]:
        """
        Search for properties and extract basic information

        Args:
            search_url: Full Rightmove search URL
            max_pages: Maximum number of pages to scrape

        Returns:
            List of property dictionaries
        """
        properties = []

        for page_num in range(max_pages):
            logger.info(f"Scraping page {page_num + 1}...")

            # Add pagination parameter
            if page_num > 0:
                separator = '&' if '?' in search_url else '?'
                page_url = f"{search_url}{separator}index={page_num * 24}"
            else:
                page_url = search_url

            try:
                response = self.session.get(page_url, timeout=30)
                response.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Failed to fetch page {page_num + 1}: {e}")
                break

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all property cards (updated selector for current Rightmove structure)
            property_cards = soup.find_all('div', class_='PropertyCard_propertyCardContainerWrapper__mcK1Z')

            if not property_cards:
                logger.warning(f"No properties found on page {page_num + 1}")
                break

            logger.info(f"Found {len(property_cards)} properties on page {page_num + 1}")

            for card in property_cards:
                property_data = self._extract_card_data(card)
                if property_data:
                    properties.append(property_data)

            # Be respectful - delay between pages
            if page_num < max_pages - 1:
                time.sleep(self.delay)

        logger.info(f"Total properties scraped: {len(properties)}")
        return properties

    def _extract_card_data(self, card) -> Optional[Dict]:
        """
        Extract data from a single property card

        Args:
            card: BeautifulSoup element for property card

        Returns:
            Dictionary of property data or None if extraction fails
        """
        try:
            data = {}

            # Property URL and ID
            link_elem = card.find('a', attrs={'data-test': 'property-details'})
            if link_elem and link_elem.get('href'):
                data['listing_url'] = self.BASE_URL + link_elem['href']
                # Extract property ID from URL
                id_match = re.search(r'/properties/(\d+)', link_elem['href'])
                data['property_id'] = id_match.group(1) if id_match else None
            else:
                return None

            # Address
            address_elem = card.find('address', class_='PropertyAddress_address__LYRPq')
            data['address'] = address_elem.text.strip() if address_elem else None

            # Price
            price_elem = card.find('div', class_='PropertyPrice_price__VL65t')
            if price_elem:
                price_text = price_elem.text.strip()
                price_match = re.search(r'£([\d,]+)', price_text)
                data['price'] = int(price_match.group(1).replace(',', '')) if price_match else None
            else:
                data['price'] = None

            # Bedrooms
            bed_elem = card.find('span', class_=lambda x: x and 'bedroomsCount' in str(x))
            data['bedrooms'] = int(bed_elem.text.strip()) if bed_elem else None

            # Bathrooms
            bath_container = card.find('div', class_=lambda x: x and 'bathContainer' in str(x))
            if bath_container:
                bath_span = bath_container.find('span', attrs={'aria-label': lambda x: x and 'in property' in str(x)})
                data['bathrooms'] = int(bath_span.text.strip()) if bath_span else None
            else:
                data['bathrooms'] = None

            # Property type (e.g. Flat, House)
            prop_type_elem = card.find('span', class_=lambda x: x and 'propertyType' in str(x))
            data['property_type'] = prop_type_elem.text.strip() if prop_type_elem else None

            # Estate agent
            agent_link = card.find('a', attrs={'data-testid': lambda x: x and 'branch-logo' in str(x)})
            data['agent'] = agent_link.get('title', '').strip() if agent_link else None

            # Agent contact (phone number)
            phone_link = card.find('a', class_=lambda x: x and 'phoneLinkDesktop' in str(x))
            if phone_link:
                phone_text = phone_link.text.strip()
                # Extract just the phone number, removing "Local call rate" etc
                phone_match = re.search(r'(\d[\d\s]+\d)', phone_text)
                data['agent_contact'] = phone_match.group(1).strip() if phone_match else phone_text.split('\n')[
                    0].strip()
            else:
                data['agent_contact'] = None

            # Date added/reduced
            date_elem = card.find('span', class_=lambda x: x and 'addedOrReduced' in str(x))
            data['date_listed'] = date_elem.text.strip() if date_elem else None

            # Property description
            desc_elem = card.find('p', attrs={'data-testid': 'property-description'})
            data['description'] = desc_elem.text.strip() if desc_elem else None

            # Placeholder for fields we might get from detail page
            data['area_sqft'] = None
            data['leasehold'] = None

            return data

        except Exception as e:
            logger.warning(f"Error extracting card data: {e}")
            return None

    def enrich_property_details(self, properties: List[Dict], fetch_details: bool = False) -> List[Dict]:
        """
        Optionally fetch additional details from individual property pages

        Args:
            properties: List of property dictionaries
            fetch_details: Whether to visit individual pages for more info

        Returns:
            Enriched list of properties
        """
        if not fetch_details:
            logger.info("Skipping detailed page scraping (use --fetch-details to enable)")
            return properties

        logger.info(f"Fetching detailed information for {len(properties)} properties...")
        logger.warning("This will take a while - being respectful to the server")

        for idx, prop in enumerate(properties, 1):
            if not prop.get('listing_url'):
                continue

            logger.info(f"Fetching details for property {idx}/{len(properties)}")

            try:
                response = self.session.get(prop['listing_url'], timeout=30)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, 'html.parser')

                # Extract area (square footage)
                # Look for span with data-testid="info-reel-SIZE-text"
                size_span = soup.find('span', attrs={'data-testid': 'info-reel-SIZE-text'})
                if size_span:
                    # Find the paragraph containing sq ft
                    size_p = size_span.find('p', class_='_1hV1kqpVceE9m-QrX_hWDN')
                    if size_p:
                        size_text = size_p.text.strip()
                        # Extract number from "726 sq ft" format
                        area_match = re.search(r'([\d,]+)\s*sq\.?\s*ft', size_text, re.IGNORECASE)
                        if area_match:
                            prop['area_sqft'] = int(area_match.group(1).replace(',', ''))

                # Determine if leasehold
                # Search for paragraphs containing "leasehold" or "freehold"
                tenure_p = soup.find('p', string=re.compile(r'(leasehold|freehold)', re.IGNORECASE))
                if tenure_p:
                    tenure_text = tenure_p.text.strip().lower()
                    prop['leasehold'] = 'leasehold' in tenure_text

                # Be very respectful with delays when scraping detail pages
                time.sleep(self.delay + 2)  # Extra delay for detail pages

            except requests.RequestException as e:
                logger.warning(f"Failed to fetch details for property {prop.get('property_id')}: {e}")
                continue
            except Exception as e:
                logger.warning(f"Error parsing property details: {e}")
                continue

        return properties


def filter_recent_listings(properties: List[Dict], max_age_days: Optional[float] = None) -> List[Dict]:
    """
    Filter properties to only include recent listings

    Args:
        properties: List of property dictionaries
        max_age_days: Maximum age in days (None = no filtering)

    Returns:
        Filtered list of properties
    """
    if max_age_days is None:
        logger.info("No recency filter applied")
        return properties

    logger.info(f"Filtering listings posted within last {max_age_days} day(s)...")

    filtered = []
    for prop in properties:
        age = parse_listing_age(prop.get('date_listed'))

        if age is None:
            # If we can't parse the date, be conservative and include it
            logger.debug(f"Unknown age for property {prop.get('property_id')}, including it")
            filtered.append(prop)
        elif age <= max_age_days:
            logger.debug(f"Including property {prop.get('property_id')}, age: {age} days")
            filtered.append(prop)
        else:
            logger.debug(f"Excluding property {prop.get('property_id')}, age: {age} days")

    logger.info(f"Filtered from {len(properties)} to {len(filtered)} properties")
    return filtered


def save_to_csv(properties: List[Dict], output_file: Path, append: bool = False) -> None:
    """
    Save properties to CSV file

    Args:
        properties: List of property dictionaries
        output_file: Path to output CSV file
        append: If True, append to existing file; if False, overwrite
    """
    if not properties:
        logger.warning("No properties to save")
        return

    fieldnames = [
        'property_id', 'address', 'description', 'bedrooms', 'bathrooms', 'property_type', 'area_sqft',
        'leasehold', 'price', 'agent', 'agent_contact', 'date_listed', 'listing_url'
    ]

    try:
        # Check if file exists and if we should write header
        file_exists = output_file.exists()
        mode = 'a' if append else 'w'

        with open(output_file, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')

            # Write header only if file is new or we're overwriting
            if not append or not file_exists:
                writer.writeheader()

            writer.writerows(properties)

        action = "appended" if append and file_exists else "saved"
        logger.info(f"Successfully {action} {len(properties)} properties to {output_file}")
    except IOError as e:
        logger.error(f"Failed to write CSV file: {e}")
        sys.exit(1)


def deduplicate_csv(csv_file: Path) -> None:
    """
    Remove duplicate properties from CSV file using pandas, keeping the most recent entry

    Args:
        csv_file: Path to CSV file to deduplicate
    """
    if not csv_file.exists():
        logger.warning(f"CSV file {csv_file} does not exist, skipping deduplication")
        return

    logger.info(f"Deduplicating {csv_file}...")

    try:
        # Read CSV with pandas
        df = pd.read_csv(csv_file)

        if df.empty:
            logger.info("No data to deduplicate")
            return

        original_count = len(df)

        # Check for rows without property_id
        missing_id = df['property_id'].isna()
        if missing_id.any():
            logger.warning(f"Found {missing_id.sum()} row(s) without property_id")

        # Drop duplicates based on property_id, keeping the last occurrence
        df_deduped = df.drop_duplicates(subset=['property_id'], keep='last')

        duplicates_removed = original_count - len(df_deduped)

        # Write back deduplicated data
        df_deduped.to_csv(csv_file, index=False)

        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate(s), {len(df_deduped)} unique properties remain")
        else:
            logger.info("No duplicates found")

    except Exception as e:
        logger.error(f"Error during deduplication: {e}")


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Scrape Rightmove property listings to CSV',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with a search URL
  %(prog)s "https://www.rightmove.co.uk/property-for-sale/find.html?searchLocation=E11&..."

  # Save to specific file
  %(prog)s "https://..." -o my_properties.csv

  # Scrape more pages and fetch detailed info
  %(prog)s "https://..." --max-pages 10 --fetch-details

  # Use custom delay (in seconds)
  %(prog)s "https://..." --delay 3.0

  # Daily scrape: only include listings posted within 1 day, append to file
  %(prog)s "https://..." -o daily_listings.csv --max-age 1.0 --append

  # Run in scheduled mode (daily at 9am) with recency filter
  %(prog)s "https://..." --schedule --run-time 09:00 --max-pages 10 --max-age 1.0 --append

  # Run scheduler and execute immediately
  %(prog)s "https://..." --schedule --run-time 14:30 --run-now --fetch-details --max-age 1.0 --append
        """
    )

    parser.add_argument(
        'url',
        help='Rightmove search URL'
    )

    parser.add_argument(
        '-o', '--output',
        type=Path,
        default=Path(f'properties_{datetime.now():%Y%m%d_%H%M%S}.csv'),
        help='Output CSV file (default: properties_TIMESTAMP.csv)'
    )

    parser.add_argument(
        '--max-pages',
        type=int,
        default=5,
        help='Maximum number of pages to scrape (default: 5)'
    )

    parser.add_argument(
        '--delay',
        type=float,
        default=2.0,
        help='Delay between requests in seconds (default: 2.0)'
    )

    parser.add_argument(
        '--fetch-details',
        action='store_true',
        help='Fetch detailed info from individual property pages (slow!)'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    parser.add_argument(
        '--schedule',
        action='store_true',
        help='Run in scheduled mode - scrape daily at specified time'
    )

    parser.add_argument(
        '--run-time',
        type=str,
        default='09:00',
        help='Time to run daily scrape in 24-hour format (default: 09:00). Only used with --schedule'
    )

    parser.add_argument(
        '--run-now',
        action='store_true',
        help='Run immediately when starting scheduler (only used with --schedule)'
    )

    parser.add_argument(
        '--max-age',
        type=float,
        default=None,
        help='Only include listings posted within this many days (e.g., 1.0 for daily scrapes). Default: no filtering'
    )

    parser.add_argument(
        '--append',
        action='store_true',
        help='Append to existing CSV file instead of overwriting. Useful for daily scrapes.'
    )

    return parser.parse_args()


def run_scrape(url: str, output_file: Path, max_pages: int, delay: float, fetch_details: bool,
               max_age_days: Optional[float] = None, append: bool = False) -> None:
    """
    Run a single scrape operation

    Args:
        url: Rightmove search URL
        output_file: Path to output CSV file
        max_pages: Maximum number of pages to scrape
        delay: Delay between requests in seconds
        fetch_details: Whether to fetch detailed info from individual pages
        max_age_days: Only include listings posted within this many days (None = no filter)
        append: If True, append to existing file; if False, overwrite
    """
    logger.info("=" * 70)
    logger.info("Starting scrape...")
    logger.info(f"Search URL: {url}")
    logger.info(f"Max pages: {max_pages}")
    logger.info(f"Output file: {output_file}")
    logger.info(f"Fetch details: {fetch_details}")
    logger.info(f"Max age filter: {max_age_days} days" if max_age_days else "Max age filter: None")
    logger.info(f"Append mode: {append}")
    logger.info("=" * 70)

    # Create scraper
    scraper = RightmoveScraper(delay=delay)

    # Search properties
    properties = scraper.search_properties(url, max_pages=max_pages)

    if not properties:
        logger.error("No properties found")
        return

    # Optionally enrich with detailed info
    properties = scraper.enrich_property_details(properties, fetch_details=fetch_details)

    # Filter by recency if requested
    properties = filter_recent_listings(properties, max_age_days=max_age_days)

    if not properties:
        logger.warning("No properties match the recency filter")
        return

    # Save to CSV
    save_to_csv(properties, output_file, append=append)

    # Deduplicate the CSV file (removes duplicates based on property_id)
    deduplicate_csv(output_file)

    logger.info("Scraping complete!")
    logger.info("=" * 70)


def run_scheduled(args: argparse.Namespace) -> None:
    """
    Run scraper in scheduled mode

    Args:
        args: Parsed command line arguments
    """
    logger.info("=" * 70)
    logger.info("RIGHTMOVE PROPERTY SCRAPER - SCHEDULED MODE")
    logger.info("=" * 70)
    logger.info(f"Configured to run daily at {args.run_time}")
    logger.info(f"Search URL: {args.url[:80]}...")
    logger.info(f"Max pages: {args.max_pages}")
    logger.info(f"Fetch details: {args.fetch_details}")
    logger.info(f"Delay: {args.delay}s")
    logger.info(f"Max age filter: {args.max_age} days" if args.max_age else "Max age filter: None")
    logger.info(f"Append mode: {args.append}")
    logger.info("=" * 70)

    # Create output directory if specified or use default
    if args.output.name.startswith('properties_'):
        # User didn't specify output, use daily_scrapes directory
        output_dir = Path('daily_scrapes')
        output_dir.mkdir(exist_ok=True)
        logger.info(f"Saving files to: {output_dir}/")
    else:
        # User specified a file, we'll use timestamped versions in same location
        output_dir = args.output.parent
        logger.info(f"Saving files to: {output_dir}/")

    def scheduled_job():
        """Job to run on schedule"""
        # Generate timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if args.output.name.startswith('properties_'):
            # Use default naming in daily_scrapes
            output_file = output_dir / f"properties_{timestamp}.csv"
        else:
            # Use user's naming pattern with timestamp
            base_name = args.output.stem
            output_file = output_dir / f"{base_name}_{timestamp}.csv"

        try:
            run_scrape(
                url=args.url,
                output_file=output_file,
                max_pages=args.max_pages,
                delay=args.delay,
                fetch_details=args.fetch_details,
                max_age_days=args.max_age,
                append=args.append
            )
        except Exception as e:
            logger.error(f"Scheduled scrape failed: {e}")

    # Schedule the job
    schedule.every().day.at(args.run_time).do(scheduled_job)

    # Run immediately if requested
    if args.run_now:
        logger.info("Running initial scrape now...")
        scheduled_job()

    logger.info(f"Scheduler is running. Next run at {args.run_time}. Press Ctrl+C to stop.")

    # Keep running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")


def main():
    """Main entry point"""
    args = parse_arguments()

    # Adjust logging level if verbose
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Run in scheduled mode or one-time mode
    if args.schedule:
        run_scheduled(args)
    else:
        # One-time scrape
        logger.info("Starting Rightmove scraper...")

        run_scrape(
            url=args.url,
            output_file=args.output,
            max_pages=args.max_pages,
            delay=args.delay,
            fetch_details=args.fetch_details,
            max_age_days=args.max_age,
            append=args.append
        )


if __name__ == "__main__":
    main()