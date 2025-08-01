import pandas as pd
from faker import Faker
import random
import numpy as np
from datetime import datetime, timedelta
import string

print("Starting data expansion script...")

# --- 1. CONFIGURATION ---
NUM_NEW_ORDERS = 9000
NEW_CUSTOMER_COUNT = 800
TARGET_COUNTRIES = [
    'United States', 'Ireland', 'United Kingdom', 'Canada', 'Australia', 
    'Germany', 'France', 'Japan', 'Brazil', 'India', 'Mexico', 'Spain'
]

# --- 2. LOAD EXISTING DATA ---
try:
    orders_df = pd.read_csv('orders.csv', parse_dates=['Order Date'], dayfirst=True)
    customers_df = pd.read_csv('customers.csv')
    products_df = pd.read_csv('products.csv')
    print("Successfully loaded existing CSV files.")
except FileNotFoundError as e:
    print(f"Error: {e}. Make sure source CSVs are in the same folder.")
    exit()

# --- 3. GENERATE NEW CUSTOMERS ---
print(f"Generating {NEW_CUSTOMER_COUNT} new customers...")
fake = Faker()
new_customers = []
for i in range(NEW_CUSTOMER_COUNT):
    part1 = str(random.randint(10000, 99999)); part2 = str(random.randint(10000, 99999)); part3 = ''.join(random.choices(string.ascii_uppercase, k=2))
    cust_id = f"{part1}-{part2}-{part3}"
    country = random.choice(TARGET_COUNTRIES)
    new_customers.append({
        'Customer ID': cust_id, 'Customer Name': fake.name(),
        'Email': fake.email() if random.random() > 0.2 else '',
        'Phone Number': fake.phone_number() if random.random() > 0.2 else '',
        'Address Line 1': fake.street_address(), 'City': fake.city(),
        'Country': country, 'Postcode': fake.postcode(),
        'Loyalty Card': random.choice(['Yes', 'No'])
    })
new_customers_df = pd.DataFrame(new_customers)
customers_expanded_df = pd.concat([customers_df, new_customers_df], ignore_index=True)
print("New customers generated.")

# --- 4. GENERATE NEW ORDERS ---
print(f"Generating {NUM_NEW_ORDERS} new orders...")
new_orders = []
all_customer_ids = customers_expanded_df['Customer ID'].tolist()
all_product_ids = products_df['Product ID'].tolist()
min_date = orders_df['Order Date'].min() 
max_date = datetime.now()
date_range_days = (max_date - min_date).days
weights = ([4] * len(customers_df)) + ([1] * len(new_customers_df))

for i in range(NUM_NEW_ORDERS):
    part1 = ''.join(random.choices(string.ascii_uppercase, k=3)); part2 = str(random.randint(10000, 99999)); part3 = str(random.randint(100, 999))
    order_id = f"{part1}-{part2}-{part3}"
    order_date = min_date + timedelta(days=random.randint(0, date_range_days))
    new_orders.append({
        'Order ID': order_id, 'Order Date': order_date,
        'Customer ID': random.choices(all_customer_ids, weights=weights, k=1)[0],
        'Product ID': random.choice(all_product_ids),
        'Quantity': random.randint(1, 5)
    })
new_orders_df = pd.DataFrame(new_orders)
print("New orders generated.")

# --- 5. INTRODUCE DATA ERRORS ---
print("\n--- Intentionally Adding Realistic Data Errors ---")

# 5.1: Create Full Row Duplicates (Orders)
duplicate_orders = new_orders_df.sample(n=50, random_state=42)
new_orders_df = pd.concat([new_orders_df, duplicate_orders], ignore_index=True)
print(f"-> Added 50 duplicate order rows.")

# 5.2: Create Orphaned Orders
orphaned_orders_list = []
for _ in range(20):
    part1 = ''.join(random.choices(string.ascii_uppercase, k=3)); part2 = str(random.randint(10000, 99999)); part3 = str(random.randint(100, 999))
    order_id = f"{part1}-{part2}-{part3}"
    order_date = min_date + timedelta(days=random.randint(0, date_range_days))
    orphaned_orders_list.append({
        'Order ID': order_id, 'Order Date': order_date,
        'Customer ID': 'ORPHAN-CUST-ID', # A clearly non-existent customer ID
        'Product ID': random.choice(all_product_ids), 'Quantity': 1
    })
orphaned_df = pd.DataFrame(orphaned_orders_list)
new_orders_df = pd.concat([new_orders_df, orphaned_df], ignore_index=True)
print("-> Added 20 orphaned orders (non-existent Customer ID).")

# 5.3: Create Invalid Data (Orders)
indices_to_corrupt = new_orders_df.sample(n=30, random_state=42).index
new_orders_df.loc[indices_to_corrupt, 'Quantity'] = random.choices([0, -1, -2], k=30)
print("-> Added 30 orders with invalid quantities (0 or negative).")

# 5.4: Create Inconsistent Text Formatting (Customers)
indices_to_format_badly = customers_expanded_df.sample(n=15, random_state=42).index
customers_expanded_df.loc[indices_to_format_badly[:5], 'Country'] = '  united states '
customers_expanded_df.loc[indices_to_format_badly[5:10], 'Country'] = 'ireland'
customers_expanded_df.loc[indices_to_format_badly[10:], 'Country'] = 'USA'
print("-> Added 15 customer records with inconsistent country names (whitespace, case, abbreviation).")

# 5.5: Create Missing Critical Data (Customers)
indices_to_blank = customers_expanded_df.sample(n=10, random_state=42).index
customers_expanded_df.loc[indices_to_blank, 'Country'] = np.nan
print("-> Added 10 customer records with a missing country.")

# --- 6. SAVE EXPANDED FILES ---
# Finalize date format before saving
orders_df['Order Date'] = orders_df['Order Date'].dt.strftime('%d/%m/%Y')
new_orders_df['Order Date'] = new_orders_df['Order Date'].dt.strftime('%d/%m/%Y')
orders_expanded_df = pd.concat([orders_df, new_orders_df], ignore_index=True).sample(frac=1).reset_index(drop=True) # Shuffle final data

print("\nSaving expanded and 'dirty' data to new CSV files...")
orders_expanded_df.to_csv('orders_expanded.csv', index=False)
customers_expanded_df.to_csv('customers_expanded.csv', index=False)
products_df.to_csv('products_expanded.csv', index=False)

print("\n--- Process Complete! ---")
print(f"Total Customers: {len(customers_expanded_df)}")
print(f"Total Orders: {len(orders_expanded_df)}")
print("New 'dirty' files created: 'orders_expanded.csv' and 'customers_expanded.csv'")