from datetime import datetime
import random   
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
load_dotenv()

HOST= os.getenv('host')
# port = os.getenv('port')
USER = os.getenv('user')
PASSWORD = os.getenv('password')
DATABASE = os.getenv('database')
def create_connection():
    try:
        return mysql.connector.connect(
            host=HOST,
            # port=3306,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )
    except mysql.connector.Error as error:
        print(f"Error connecting to the database: {error}")
        return None

def create_table_if_not_exists(database, table_name):
    """
    Checks if a table exists and creates it if it does not.
    
    Args:
        host (str): The host of the MySQL server.
        user (str): The MySQL username.
        password (str): The MySQL password.
        database (str): The database name.
        table_name (str): The name of the table to create.
    """
    try:
        # Connect to the database
        conn = create_connection()
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = '{database}' AND table_name = '{table_name}'
        """)
        table_exists = cursor.fetchone()[0]

        if table_exists:
            return f"Table '{table_name}' already exists."
        else:
            # SQL query to create the table
            create_table_query = f"""
            CREATE TABLE {table_name} (
                product_id INT AUTO_INCREMENT PRIMARY KEY,
                product_name VARCHAR(255) NOT NULL,
                product_sku VARCHAR(150) NOT NULL UNIQUE,
                inventory_status ENUM('In Stock', 'Out of Stock', 'Unknown') NOT NULL DEFAULT 'Unknown',
                quantity_available INT DEFAULT 0,
                quantity_total INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_table_query)
            return f"Table '{table_name}' created successfully!"

    except mysql.connector.Error as error:
        return f"Error: {error}"

    finally:
        # Close the connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def add_product(product_name, product_sku):
    """
    Adds a new product to the 'product' table.
    
    Args:
        host (str): MySQL host.
        user (str): MySQL username.
        password (str): MySQL password.
        database (str): Database name.
        product_name (str): Name of the product.
        product_sku (str): SKU of the product.
        inventory_status (str): Inventory status ('In Stock', 'Out of Stock', 'Pre-Order').
        quantity_available (int): Available quantity.
        quantity_total (int): Total quantity.
    """
    try:
        conn = create_connection()
        cursor = conn.cursor()

        # Insert query
        insert_query = """
        INSERT INTO product (product_name, product_sku)
        VALUES (%s, %s)
        """
        values = (product_name, product_sku)

        # Execute the query
        cursor.execute(insert_query, values)
        conn.commit()

        return f"Product added successfully with ID {cursor.lastrowid}."  # cursor.lastrowid gets the auto-generated ID

    except mysql.connector.Error as error:
        print(f"Error: {error}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def sku_name(data):
   
    try:
        product_sku = data.get("productSku", "N/A")  # Extract productSku or return "N/A" if not found
        product_name = data.get("productName", "N/A")  # Extract productName or return "N/A" if not found
        
        return  product_sku, product_name

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def generate_product_api_body(order_details):
    def get_value(data, key, default=None):
        """Helper function to safely get nested dictionary values."""
        if not data or not isinstance(data, dict):
            return default
        return data.get(key, default)
    
    def get_first_variant(variants):
        """Helper function to safely get the first variant."""
        if not variants or not isinstance(variants, list):
            return {}
        return variants[0] if variants else {}
    
    def map_wine_type(wine_type):
        """Map wine type to valid values."""
        wine_type_mapping = {
            "Sparkling": "Sparkling Wine",
            "Still": "Still Wine",
            "Fortified": "Fortified Wine",
            "Non-Alcoholic": "Non-Alcoholic"
        }
        return wine_type_mapping.get(wine_type, "Still Wine")
    
    def map_slot_equivalent(volume_ml):
        """Map volume to valid slot equivalent values."""
        valid_volumes = [0, 187, 375, 500, 750, 1500, 1800, 2250, 3000, 5000, 6000, 9000]
        if not volume_ml:
            return "750ml"
        # Find the closest valid volume
        volume_str = f"{volume_ml}ml"
        if volume_ml in valid_volumes:
            return volume_str
        return "750ml"  # Default value
    
    def map_wine_origin(region):
        """Map region to full country name if short code is provided, or return 'Unknown' for unrecognized inputs."""
        region_mapping = {
            "AR": "Argentina",
            "AU": "Australia",
            "AT": "Austria",
            "CA": "Canada",
            "CL": "Chile",
            "HR": "Croatia",
            "CZ": "Czech Republic",
            "GB": "England",
            "FR": "France",
            "DE": "Germany",
            "GR": "Greece",
            "HU": "Hungary",
            "IN": "India",
            "IL": "Israel",
            "IT": "Italy",
            "JP": "Japan",
            "LB": "Lebanon",
            "LU": "Luxembourg",
            "MX": "Mexico",
            "MA": "Morocco",
            "NZ": "New Zealand",
            "PT": "Portugal",
            "GE": "Republic of Georgia",
            "RO": "Romania",
            "RS": "Serbia",
            "SI": "Slovenia",
            "ZA": "South Africa",
            "ES": "Spain",
            "CH": "Switzerland",
            "TR": "Turkey",
            "US": "United States",
            "UY": "Uruguay",
        }
        
        # Combine both full names and short codes into the mapping
        full_names = set(region_mapping.values())
        return region_mapping.get(region, region if region in full_names else "Unknown")
    
    def format_weight(weight_value):
        """Format weight to float with 2 decimal places and return as string."""
        try:
            return "{:.2f}".format(float(weight_value))  # Format as a string with 2 decimal places
        except (TypeError, ValueError):
            return "3.00"  # Default weight as string with 2 decimal places
    
    def label_dis(wine):
        return f"{wine} wine"
    
    def COLA_number():
        """Generate a COLA number with a random number appended."""
        random_number = random.randint(1, 100000)
        return f"COLA{random_number}"
    
    # Get payload from order_details if it exists
    payload = order_details.get('payload', order_details)
    
    # Get primary variant
    variant = get_first_variant(payload.get('variants', []))
    wine = get_value(payload, 'wine', {})
    
    # Calculate retail price in dollars (convert from cents)
    retail_price = get_value(variant, 'price', 0) / 100 if get_value(variant, 'price') else 0
    
    # Default container based on volume
    volume_ml = get_value(variant, 'volumeInML', 750)
    container = "12PK750C"
    
    # Ensure alcohol percentage is not None
    alcohol_percentage = get_value(wine, 'alcoholPercentage', 14.0)
    if alcohol_percentage is None or alcohol_percentage == "null":
        alcohol_percentage = 14.0
    
    # Format weight as float with 2 decimal places
    weight = format_weight(get_value(variant, 'weight', 3.00))

    return {
        "clientCode": "CLTUR",
        "productSku": get_value(variant, "sku"),
        "productType": get_value(payload, "type"),
        "productName": get_value(payload, "title"),
        "retailPrice": retail_price,
        "container": container,
        "amountPerContainer": 1,
        "weight": weight,
        "slotEquivalent": map_slot_equivalent(volume_ml),
        "slotCount": 1,
        "shortDescription": get_value(payload, "title"),
        "longDescription": get_value(payload, "teaser"),
        "labelDescription": label_dis(get_value(wine, "type", "Wine")),
        "brandCode": get_value(wine, "countryCode"),
        "backorderWarning": "Y" if get_value(variant, "inventoryPolicy") == "Back Order" else "N",
        "backorderThreshold": 3,
        "serviceFee": 2.5,
        "wineType": map_wine_type(get_value(wine, "type")),
        "pctAlcohol": alcohol_percentage,
        "wineOrigin": map_wine_origin(get_value(wine, "countryCode")),
        "wineBottleSize": volume_ml,
        "wineVineyard": get_value(wine, "region"),
        "wineVintage": get_value(wine, "vintage"),
        "wineCOLA": COLA_number(),
        "wineVarietal": get_value(wine, "varietal")
    }

