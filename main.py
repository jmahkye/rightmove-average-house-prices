import requests
from bs4 import BeautifulSoup
import re
import matplotlib.pyplot as plt
from statistics import mean
import time
import os
import csv
from datetime import datetime, timezone

# Define region codes for each location
REGION_CODES = {
    'Greater Manchester': 'REGION^79192',
    'South London': 'REGION^92051',
    'Tyne and Wear': 'REGION^61207',
    'Leytonstone': 'REGION^87521',
    'Walthamstow': 'REGION^85310',
    'Wickford': 'REGION^1450',
    'Dagenham': 'REGION^399',
    'Lewisham': 'REGION^85388',
    'Penge': 'REGION^85321',
    'Islington': 'REGION^87515',
    'Plymouth': 'REGION^1073'
}


def extract_prices_new_structure(soup):
    """Extract prices using the new Rightmove HTML structure"""
    prices = []

    # Method 1: Direct price class (most specific)
    price_elements = soup.find_all('div', class_='PropertyPrice_price__VL65t')

    if price_elements:
        for element in price_elements:
            price_text = element.text.strip()

            # Extract numeric value
            price_match = re.search(r'£([\d,]+)', price_text)
            if price_match:
                price = int(price_match.group(1).replace(',', ''))
                prices.append(price)

    # Method 2: Using data-testid (more stable for future changes)
    if not prices:
        print("Trying data-testid approach...")
        price_links = soup.find_all(attrs={'data-testid': 'property-price'})

        for link in price_links:
            # Look for price div within this link
            price_div = link.find('div', class_=re.compile(r'PropertyPrice_price__'))
            if price_div:
                price_text = price_div.text.strip()
                price_match = re.search(r'£([\d,]+)', price_text)
                if price_match:
                    price = int(price_match.group(1).replace(',', ''))
                    prices.append(price)

    # Method 3: Broader search for any PropertyPrice_price class (in case suffix changes)
    if not prices:
        print("Trying broader PropertyPrice_price search...")
        price_elements = soup.find_all('div', class_=re.compile(r'PropertyPrice_price__'))

        for element in price_elements:
            price_text = element.text.strip()
            price_match = re.search(r'£([\d,]+)', price_text)
            if price_match:
                price = int(price_match.group(1).replace(',', ''))
                prices.append(price)

    return prices


def get_avg_price(bedroom_count, location_code, max_pages=5):
    """
    Scrape Rightmove for average property prices based on bedroom count and location

    Args:
        bedroom_count (int): Number of bedrooms
        location_code (str): Rightmove region identifier code
        max_pages (int): Maximum number of pages to scrape

    Returns:
        tuple: (Average price of properties, Number of properties found)
    """
    # Get location name from code
    location_name = next((name for name, code in REGION_CODES.items() if code == location_code), "Unknown")

    # Properly formatted Rightmove URL
    base_url = "https://www.rightmove.co.uk/property-for-sale/find.html"

    # Base parameters for the search
    params = {
        'locationIdentifier': location_code,
        'minBedrooms': bedroom_count,
        'maxBedrooms': bedroom_count,
        'propertyTypes': '',  # All property types
        'primaryDisplayPropertyType': '',  # Both houses and flats
        'mustHave': '',
        'dontShow': '',
        'furnishTypes': '',
        'sortType': '6',
        'keywords': ''
    }

    # Set headers to mimic a browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.rightmove.co.uk/'
    }

    # Make the request
    print(f"Scraping prices for {bedroom_count} bedroom properties in {location_name}...")

    all_prices = []

    # Scrape multiple pages
    for page in range(max_pages):
        # Add page index parameter for pagination
        if page > 0:
            params['index'] = page * 24  # Rightmove shows 24 properties per page

        response = requests.get(base_url, params=params, headers=headers)

        # Check if the request was successful
        if response.status_code != 200:
            print(f"Failed to retrieve data for page {page + 1}: {response.status_code}")
            break

        # Print URL for debugging (only for first page)
        if page == 0:
            print(f"Successfully retrieved: {response.url}")

            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')

            page_prices = extract_prices_new_structure(soup)

            # If no prices found and it's not the first page, we might have reached the end
            if not page_prices and page > 0:
                print(f"No more properties found after page {page}.")
                break
            elif not page_prices and page == 0:
                print(f"No prices found for {location_name} with {bedroom_count} bedrooms.")
                break

            # Add this page's prices to the total
            all_prices.extend(page_prices)
            print(f"Page {page + 1}: Found {len(page_prices)} properties. Total so far: {len(all_prices)}")

            # Add a delay between page requests
            if page < max_pages - 1:
                time.sleep(1)

    # Calculate average if prices were found
    if all_prices:
        avg_price = mean(all_prices)
        print(f"Total properties found: {len(all_prices)} with an average price of £{avg_price:,.2f}")
        return avg_price, len(all_prices)
    else:
        return None, 0


def create_plot(location, prices_data, sample_sizes, color='skyblue'):
    """
    Create and save a plot for a single location

    Args:
        location (str): Location name
        prices_data (dict): Dictionary with bedroom counts and prices
        sample_sizes (dict): Dictionary with sample size information
        color (str): Color for the bars

    Returns:
        str: Path to the saved PNG file
    """
    # Create figure
    fig, ax = plt.figure(figsize=(10, 6)), plt.gca()

    # Get data for plotting
    bedroom_counts = list(prices_data.keys())
    price_values = list(prices_data.values())

    # Create bars
    bars = ax.bar(bedroom_counts, price_values, color=color, width=0.6)

    # Customize plot
    ax.set_xlabel('Number of Bedrooms', fontsize=12)
    ax.set_ylabel('Average Price (£)', fontsize=12)
    ax.set_title(f'Average Property Prices in {location}', fontsize=14)
    ax.grid(axis='y', linestyle='--', alpha=0.7)

    # Add price labels on top of bars
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2., height + height * 0.02,
                f'£{height:,.0f}', ha='center', va='bottom', fontsize=10)

    # Format y-axis with comma separators
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'£{int(x / 1000)}k'))

    # Add sample size annotation
    if sample_sizes:
        # Create sample size text
        sample_text = []
        for bed_key in prices_data.keys():
            count = sample_sizes.get(bed_key, "N/A")
            sample_text.append(f"{bed_key}: {count} properties")

        # Add annotation with sample sizes
        ax.annotate("Sample sizes:\n" + "\n".join(sample_text),
                    xy=(0.05, 0.95), xycoords='axes fraction',
                    fontsize=9, verticalalignment='top',
                    bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.3))

    # Ensure output directory exists
    output_dir = 'property_prices'
    os.makedirs(output_dir, exist_ok=True)

    # Save plot to file
    filename = f"{output_dir}/{location.replace(' ', '_').lower()}_prices.png"
    plt.tight_layout()
    plt.savefig(filename, dpi=300)
    plt.close()

    print(f"Plot saved to {filename}")

    # Return filename for reference
    return filename


def main():
    # CSV setup
    csv_filename = 'uk_daily_house_prices.csv'
    csv_headers = ['Location', 'Rooms', 'Timestamp(unix)', 'Timestamp(UTC)', 'Average_Price', 'Sample_Size', 'Source']

    # Create CSV file if it doesn't exist
    if not os.path.exists(csv_filename):
        with open(csv_filename, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(csv_headers)
        print(f"Created CSV file: {csv_filename}")

    while True:
        # Get current timestamp
        timestamp = datetime.now(timezone.utc)
        unix_timestamp = int(timestamp.timestamp())
        utc_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')

        print(f"\nStarting data collection at {utc_timestamp}")

        # Process each location independently
        for location, code in REGION_CODES.items():
            print(f"\nProcessing {location}...")

            # Scrape for 1, 2, and 3 bedroom properties
            for bedrooms in [1, 2, 3]:
                avg_price, count = get_avg_price(bedrooms, code, max_pages=10)

                if avg_price:
                    # Write to CSV
                    with open(csv_filename, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(
                            [location, bedrooms, unix_timestamp, utc_timestamp, avg_price, count, 'Rightmove'])

                    print(f"Logged: {location}, {bedrooms} bedrooms, £{avg_price:,.2f} (n={count}) [Rightmove]")
                else:
                    print(f"No data found for {location} with {bedrooms} bedrooms")

                # Add a delay to avoid overwhelming the server
                time.sleep(3)

        print(f"\nData collection complete. Sleeping for 24 hours...")
        time.sleep(60 * 60)  # Wait 24 hours


if __name__ == "__main__":
    main()