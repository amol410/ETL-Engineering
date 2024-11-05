import pandas as pd
import sqlite3

def process_sales_data():
    # Step 1: Read data files from both regions
    print("Reading data files...")
    file_path_a = r"C:\Users\Amol\Desktop\Assessment\order_region_a.xlsx"
    file_path_b = r"C:\Users\Amol\Desktop\Assessment\order_region_b.xlsx"

    # Load the data into DataFrames
    data_a = pd.read_excel(file_path_a)
    data_b = pd.read_excel(file_path_b)

    # Add 'region' column to tell the data apart
    data_a['region'] = 'A'
    data_b['region'] = 'B'

    # Combine the two data sets into one
    combined_data = pd.concat([data_a, data_b])

    # Step 2: Calculate total sales for each order
    print("Calculating sales...")
    # Make sure the columns are numeric for calculations
    combined_data['QuantityOrdered'] = pd.to_numeric(combined_data['QuantityOrdered'], errors='coerce').fillna(0)
    combined_data['ItemPrice'] = pd.to_numeric(combined_data['ItemPrice'], errors='coerce').fillna(0)
    combined_data['PromotionDiscount'] = pd.to_numeric(combined_data['PromotionDiscount'], errors='coerce').fillna(0)
    
    # Multiply quantity and price to get the total sales per order
    combined_data['total_sales'] = combined_data['QuantityOrdered'] * combined_data['ItemPrice']

    # Step 4: Get rid of duplicate orders based on OrderId
    print("Removing duplicates...")
    # Only keep the first occurrence of each OrderId
    combined_data = combined_data.drop_duplicates(subset='OrderId', keep='first')

    # Step 5: Calculate net sale after applying the discount
    combined_data['net_sale'] = combined_data['total_sales'] - combined_data['PromotionDiscount']

    # Step 6: Filter out orders with zero or negative net sale
    print("Filtering out negative or zero net sales...")
    combined_data = combined_data[combined_data['net_sale'] > 0]

    # Print out some stats before we save it to the database
    print("\nData Summary:")
    print(f"Total records: {len(combined_data)}")
    print("\nRegion distribution:")
    print(combined_data['region'].value_counts())

    return combined_data

def load_to_database(data):
    # Step 7: Load processed data into the database
    print("\nLoading data to SQLite database...")
    conn = sqlite3.connect(r"C:\Users\Amol\Desktop\Assessment\sales_data.db")
    
    # Create the sales table if it doesn't exist
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS sales (
        OrderId INTEGER PRIMARY KEY,
        QuantityOrdered INTEGER,
        ItemPrice REAL,
        PromotionDiscount REAL,
        total_sales REAL,
        region TEXT,
        net_sale REAL
    )
    '''
    
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    
    # Insert the DataFrame into the database, replacing old data
    data.to_sql('sales', conn, if_exists='replace', index=False)
    
    # Check that data loaded successfully by counting records per region
    cursor.execute("SELECT region, COUNT(*) FROM sales GROUP BY region")
    results = cursor.fetchall()
    print("\nDatabase Summary:")
    for region, count in results:
        print(f"Region {region}: {count} records")
    
    conn.close()

if __name__ == "__main__":
    print("Starting data processing...")
    # Process the data and apply all business rules
    processed_data = process_sales_data()
    
    # Load processed data into the SQLite database
    load_to_database(processed_data)
    
    print("\nData processing and loading completed successfully!")
