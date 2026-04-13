'''
This script contains a function get_amazon_prices() described below, the input item list taken from the mapping list with a few changes, 
and code to generate an excel file from the resulting dataframe containing mean amazon prices and standard deviations.

The output of is a prices.xlsx which is saved in fire-emissions/local/prices.xlsx.

Future users should replace their user agent (line 41).
'''


import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np


def get_amazon_prices(items):
    '''
    Get average prices for each item from Amazon using the requests BeautifulSoup libraries.

    Takes an items list as input and returns a dataframe with mean price in pounds and standard deviation per item.
    The top 10 items in the search are averaged to get a mean price and standard deviation for each item.
    '''
    
    prices = {
        'item': [],
        'price': [],
        'stdev': []
    }

    # Get list of search urls for each item
    base_url = "https://www.amazon.co.uk/s?k="
    search_urls = []
    for item in items:
        search_url = base_url + item.replace(" ", "+")
        search_urls.append(search_url)

    # Get search results
    for item, url in zip(items, search_urls):
        headers = {
            # Replace with your user agent - this can be looked up at https://www.whatismybrowser.com/detect/what-is-my-user-agent/
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'
            }
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        
        print(f"Item: {item}")
        print(f"URL: {url}")
        print(f"Response status code: {response.status_code}")
        
        # Find the prices of the top 10 items
        price_elements = soup.find_all("span", class_="a-price-whole")
        top_10_prices = []
        for price_element in price_elements[:10]:
            price = price_element.text
            top_10_prices.append(float(price.replace(",", "")))
        print(f"Top 10 prices for {item}: {top_10_prices}")
        
        # Calculate the average and standard deviation
        average_price = sum(top_10_prices) / len(top_10_prices)
        standard_deviation = np.std(top_10_prices)
        print(f"Average price for {item}: {average_price}")
        print(f"Standard deviation for {item}: {standard_deviation}")

        # Create a dataframe
        prices['item'].append(item)
        prices['price'].append(average_price)
        prices['stdev'].append(standard_deviation) 
        prices_df = pd.DataFrame(prices)
        
    return prices_df


# List of items from mapping list
items = [
    "2-3 seated sofa",
    "4-5 seated sofa",
    "Artwork",
    "Baby/high chair",
    "Bath of shower",
    "Bathroom cabinet",
    "Bed",
    "Blanket or throw",
    "Books (large shelves)",
    "Books (medium shelves)",
    "Books (small shelves)",
    "Carpet",
    "CDs or DVDs (small shelves)",
    "Clothes in a wardrobe",
    "Clothes in drawers",
    "Clothes rack",
    "Coffee table",
    "Curtains",
    "Desk",
    "Desktop PC",
    "Dining room table",
    "Drawers: large",
    "Drawers: small",
    "Environmental control",
    "Extractor fan",
    "Floor lamp",
    "Food bin",
    "Gaming console",
    "General trash bin",
    "Guitar",
    "Hard chair",
    "Kitchen cupboard",
    "Kitchen table",
    "Laptop",
    "Large cushion",
    "Large fridge freezer",
    "Large kitchen appliance", # Changed large kitchen appliance to washing machine to get better results, ignoring the alphabetical order
    "Large plant",
    "Large table",
    "Laundry basket",
    "Loose clothing or fabric",
    "Media device",
    "Media unit (TV unit)",
    "Night side table",
    "Oven",
    "PC monitor",
    "Piano",
    "Printer",
    "Recycling bin",
    "Rug",
    "Separate freezer",
    "Shelf unit: large",
    "Shelf unit: medium",
    "Shelf unit: small",
    "Blender", # Changed small kitchen appliance to blender to get better results, ignoring the alphabetical order
    "Small refrigerator",
    "Small side table",
    "Soft armchair/lazy boy",
    "Soft floor seating",
    "Sound system",
    "Storage box",
    "Table lamp",
    "Tall stool",
    "Toy shelf/storage",
    "TV",
    "Wall lamp",
    "Wardrobe",
    "Window blinds",
    "Wooden or plastic drying rack"
]


# Write the DataFrame to an Excel file
prices_df = get_amazon_prices(items)
prices_df.to_excel('local/prices.xlsx')


