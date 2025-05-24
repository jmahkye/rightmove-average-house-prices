import requests
from bs4 import BeautifulSoup
import re
import matplotlib.pyplot as plt
import numpy as np
from statistics import mean
import time
import os

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

        # Find all price elements - Rightmove uses this class for prices
        price_elements = soup.find_all('div', class_='propertyCard-priceValue')

        if not price_elements:
            # If we didn't find prices with the expected class, try alternative
            price_elements = soup.find_all('span', attrs={'data-test': 'property-price'})

        # If still no prices found and it's not the first page, we might have reached the end
        if not price_elements and page > 0:
            print(f"No more properties found after page {page}.")
            break

        # Extract prices
        page_prices = []
        for element in price_elements:
            price_text = element.text.strip()
            # Extract numeric value using regex
            price_match = re.search(r'£([\d,]+)', price_text)
            if price_match:
                # Convert to number
                price = int(price_match.group(1).replace(',', ''))
                page_prices.append(price)

        # If no prices found on this page
        if not page_prices:
            if page == 0:
                print(f"No prices found for {location_name} with {bedroom_count} bedrooms.")
            else:
                print(f"No more prices found after page {page}.")
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
    # Define location colors
    colors = {
        'Greater Manchester': 'skyblue',
        'South London': 'lightcoral',
        'Tyne and Wear': 'lightgreen'
    }

    # Process each location independently
    for location, code in REGION_CODES.items():
        # Initialize data structures for this location
        location_data = {}
        location_sample_sizes = {}

        # Scrape for 1, 2, and 3 bedroom properties
        for bedrooms in [1, 2, 3]:
            avg_price, count = get_avg_price(bedrooms, code, max_pages=10)
            if avg_price:
                bed_key = f"{bedrooms} Bed"
                location_data[bed_key] = avg_price
                location_sample_sizes[bed_key] = count
            # Add a delay to avoid overwhelming the server
            time.sleep(3)

        # Create plot for this location if data was found
        # if location_data:
        #     color = colors.get(location, 'skyblue')
        #     filename = create_plot(location, location_data, location_sample_sizes, color)
        #
        #     # Print data summary for this location
        #     print(f"\n{location} data summary:")
        #     for beds, price in location_data.items():
        #         sample_count = location_sample_sizes.get(beds, "N/A")
        #         print(f"  {beds}: £{price:,.2f} (Sample size: {sample_count})")
        #     print(f"Plot saved to: {filename}")
        # else:
        #     print(f"No data found for {location}")


if __name__ == "__main__":
    main()