'''
This script contains two lists of items: items from survey and assumed items, 
and inputs them into the function 'get_amazon_prices' from the module 'get_amazon_prices.py'.

The outputs of this script are two excel sheets: prices.xlsx and assumed_prices.xlsx 
containing the mean prices and standard deviations taken from 10 top Amazon items with the corresponding item name.
'''

from get_amazon_prices import get_amazon_prices


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

# List of additional assumed items
assumed_items = [
    "Door",
    "Cooker",
    "Vacuum",
    "Sink",
    "Bath", # Changed to bath to get a realistic price
    "Toilet",
    "Mirror"
]

# Write the item prices to an excel file
prices_df = get_amazon_prices(items)
prices_df.to_excel('local/prices.xlsx')

# Write the assumed items prices to an excel file
prices_df = get_amazon_prices(assumed_items)
prices_df.to_excel('local/assumed_prices.xlsx')