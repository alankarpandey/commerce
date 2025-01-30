import mysql.connector
from datetime import datetime, timedelta
import json
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

def check_and_create_table():
    try:    
        conn = create_connection()
        if conn.is_connected():
            cursor = conn.cursor()

            # Check if the table exists
            table_name = "order_detail"
            check_table_query = f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{conn.database}'
            AND TABLE_NAME = '{table_name}';
            """
            cursor.execute(check_table_query)
            table_exists = cursor.fetchone()[0]

            if table_exists:
                return f"Table `{table_name}` already exists."
            else:
                # SQL query to create the table if it does not exist
                create_table_query = f"""
                CREATE TABLE {table_name} (
                    order_id INT AUTO_INCREMENT PRIMARY KEY,
                    order_no VARCHAR(255) NOT NULL,
                    statusMessage VARCHAR(255) NOT NULL,
                    statusCode INT NOT NULL,
                    statusDate VARCHAR(255) NOT NULL
                );
                """
                cursor.execute(create_table_query)
                return f"Table `{table_name}` has been created successfully."

    except Error as e:
        return f"Error while connecting to MySQL: {e}"
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")


def add_order_no(order_no):
    try:
        
        conn = create_connection()
        if conn.is_connected():
            cursor = conn.cursor()

            # Insert order_no into the table
            insert_query = """
            INSERT INTO order_detail (order_no, statusMessage, statusCode, statusDate)
            VALUES (%s,%s,%s,%s);
            """
            # Default values for other columns
            status_message = "Pending"
            status_code = 0

            cursor.execute(insert_query, (order_no, status_message, status_code, status_message))
            conn.commit()  # Commit the transaction
            return f"Order number {order_no} added successfully."

    except Error as e:
        return f"Error while adding order number: {e}"
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")

def check_product_status( product_sku):
    try:
        conn = create_connection()
        cursor = conn.cursor()

        # Query to check product inventory status
        query = "SELECT inventory_status FROM product WHERE product_sku = %s"
        cursor.execute(query, (product_sku,))

        # Fetch the result
        result = cursor.fetchone()

        if result:
            # Return the inventory status
            return str(result[0])
        else:
            return f"No product found with SKU '{product_sku}'."

    except mysql.connector.Error as error:
        return f"Error: {error}"

    finally:
        # Close the connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def generate_status_api_body(product_sku):
    return {  
        "clientCode": "CLTUR",  
        "facility": "CLTURNP",  
        "sku": [  
                product_sku  
                ]  
        }

def extract_quantities(response):
    try:
        # Check if the response has a valid structure
        if not response.get("success", "FALSE").upper() == "TRUE":
            return "Invalid response or operation failed."
        
        # Extract items
        items = response.get("items", [])
        if not items:
            return "No items found in the response."

        # Prepare result
        result = []
        for item in items:
            facility = item.get("facility")
            sku = item.get("sku")
            quantity_available = item.get("quantityAvailable")
            quantity_total = item.get("quantityTotal")

            # Append to the result if relevant fields exist
            result.append({
                "facility": facility,
                "sku": sku,
                "quantityAvailable": quantity_available,
                "quantityTotal": quantity_total,
            })

        return result

    except Exception as e:
        return f"An error occurred: {e}"

def update_inventory_status(product_sku, new_status):
    """
    Updates the inventory_status for a product in the product table.

    Args:
        product_sku (str): The SKU of the product to update.
        new_status (str): The new inventory status to set.

    Returns:
        str: A message indicating the success or failure of the operation.
    """
    try:
        # Connect to the database
        conn = create_connection()
        if not conn:
            return "Failed to connect to the database."

        cursor = conn.cursor()

        # Query to update the inventory status
        update_query = """
            UPDATE product
            SET inventory_status = %s
            WHERE product_sku = %s
        """
        cursor.execute(update_query, (new_status, product_sku))

        # Commit the changes
        conn.commit()

        # Check if any rows were affected
        if cursor.rowcount > 0:
            return f"Inventory status for product SKU '{product_sku}' updated to '{new_status}'."
        else:
            return f"No product found with SKU '{product_sku}'."

    except mysql.connector.Error as error:
        return f"Database error: {error}"

    finally:
        # Close the connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def update_product_details(product_sku, new_status, quantity_available, quantity_total):
    """
    Updates the inventory_status, quantity_available, and quantity_total for a product in the product table.

    Args:
        product_sku (str): The SKU of the product to update.
        new_status (str): The new inventory status to set.
        quantity_available (int): The updated quantity available for the product.
        quantity_total (int): The updated total quantity for the product.

    Returns:
        str: A message indicating the success or failure of the operation.
    """
    try:
        # Connect to the database
        conn = create_connection()
        if not conn:
            return "Failed to connect to the database."

        cursor = conn.cursor()

        # Query to update the product details
        update_query = """
            UPDATE product
            SET inventory_status = %s, 
                quantity_available = %s, 
                quantity_total = %s
            WHERE product_sku = %s
        """
        cursor.execute(update_query, (new_status, quantity_available, quantity_total, product_sku))

        # Commit the changes
        conn.commit()

        # Check if any rows were affected
        if cursor.rowcount > 0:
            return (f"Product SKU '{product_sku}' updated successfully: "
                    f"Inventory Status = '{new_status}', "
                    f"Quantity Available = {quantity_available}, "
                    f"Quantity Total = {quantity_total}.")
        else:
            return f"No product found with SKU '{product_sku}'."

    except mysql.connector.Error as error:
        return f"Database error: {error}"

    finally:
        # Close the connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def generate_order_api_body(data):
    """
    Generate API body from order details, extracting from payload structure.
    
    Args:
        data (str or dict): JSON string or dictionary containing order data
        
    Returns:
        dict: Formatted API body
    """
    # Handle JSON string input
    if isinstance(data, str):
        data = json.loads(data)
    
    # Extract payload
    order_details = data.get('payload', {})
    if not order_details:
        raise ValueError("No payload found in input data")

    def get_value(data, key, default=None):
        """Helper function to get value or return default if not found."""
        return data.get(key, default)
    
    def parse_date(timestamp, days_to_add=0):
        """Parse timestamp and optionally add days."""
        if not timestamp or 'T' not in timestamp:
            return None
            
        try:
            date_part = timestamp.split('T')[0]
            date_obj = datetime.strptime(date_part, "%Y-%m-%d")
            if days_to_add:
                date_obj += timedelta(days=days_to_add)
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            return None

    def format_numeric(value, default="0.00"):
        """Format numeric values with two decimal places."""
        try:
            return f"{float(value):.2f}"
        except (TypeError, ValueError):
            return default

    # Extract key components from payload
    ship_to = get_value(order_details, "shipTo", {})
    bill_to = get_value(order_details, "billTo", {})
    customer = get_value(order_details, "customer", {})
    customer_email = get_value(customer.get("emails", [{}])[0], "email", "")
    # shipping_info = get_value(order_details, "shipping", [{}])[0]
    shipping_info = "UP2"

    
    # Build order items
    # order_items = []
    # for item in get_value(order_details, "items", []):
    #     order_item = {
    #         "productSKU": get_value(item, "sku", ""),
    #         "quantity": get_value(item, "quantity", 1),
    #         "productSupplier": "CLTUR",
    #         "productName": get_value(item, "productTitle", ""),
    #         "tax": format_numeric(get_value(item, "tax", 0)),
    #         "price": format_numeric(get_value(item, "price", 0)),
    #         "pctAlcohol": get_value(item, "alcoholPercentage", ""),
    #         "weight": format_numeric(get_value(item, "weight", 3))
    #     }
    #     order_items.append(order_item)
    
    order_items = []
    
    for item in get_value(order_details, "items", []):
        bundle_items = get_value(item, "bundleItems")
        
        if bundle_items:
            # Process items within the bundle
            for bundle_item in bundle_items:
                if get_value(bundle_item, "type") != "Collateral":  # Skip collateral items
                    order_item = {
                        "productSKU": get_value(bundle_item, "sku", ""),
                        "quantity": get_value(bundle_item, "quantity", 1),
                        "productSupplier": "CLTUR",
                        "productName": get_value(bundle_item, "productTitle", ""),
                        "tax": format_numeric(get_value(bundle_item, "tax", 0)),
                        "price": format_numeric(get_value(bundle_item, "price", 0)),
                        "pctAlcohol": get_value(bundle_item, "alcoholPercentage", ""),
                        "weight": format_numeric(get_value(bundle_item, "weight", 3))
                    }
                    order_items.append(order_item)
        else:
            # Process regular non-bundle items
            order_item = {
                "productSKU": get_value(item, "sku", ""),
                "quantity": get_value(item, "quantity", 1),
                "productSupplier": "CLTUR",
                "productName": get_value(item, "productTitle", ""),
                "tax": format_numeric(get_value(item, "tax", 0)),
                "price": format_numeric(get_value(item, "price", 0)),
                "pctAlcohol": get_value(item, "alcoholPercentage", ""),
                "weight": format_numeric(get_value(item, "weight", 3))
            }
            order_items.append(order_item)
    # Construct API body
    api_body = {
        "formatVersion": "205",
        "clientCode": "CLTUR",
        "order": [{
            "orderNo": get_value(order_details, "orderNumber"),
            "orderType": "",
            "brandCode": "Brand",
            "subClub": "",
            "orderDate": parse_date(get_value(order_details, "orderPaidDate")),
            "packageType": "Each",
            "shipMethod": "UP2",
            "fulfillmentType": "DTC-Direct Ship",
            "fulfillmentLocation": "CLTURNP",
            "manifestLocation": "NAPA",
            "transferDestination": "NAPA",
            "requestedShipDate": parse_date(get_value(order_details, "orderPaidDate"), 2),
            
            "shipToAddress": {
                "firstName": get_value(ship_to, "firstName", ""),
                "lastName": get_value(ship_to, "lastName", ""),
                "company": get_value(ship_to, "company", ""),
                "address1": get_value(ship_to, "address", ""),
                "address2": get_value(ship_to, "address2", ""),
                "city": get_value(ship_to, "city", ""),
                "state": get_value(ship_to, "stateCode", ""),
                "postalCode": get_value(ship_to, "zipCode", ""),
                "country": get_value(ship_to, "countryCode", "US"),
                "workPhone": get_value(ship_to, "phone", ""),
                "homePhone": get_value(ship_to, "phone", ""),
                "mobilePhone": get_value(ship_to, "phone", ""),
                "email": customer_email,
                "dob": get_value(ship_to, "birthDate", "")
            },
            
            "billToAddress": {
                "firstName": get_value(bill_to, "firstName", ""),
                "lastName": get_value(bill_to, "lastName", ""),
                "company": get_value(bill_to, "company", ""),
                "address1": get_value(bill_to, "address", ""),
                "address2": get_value(bill_to, "address2", ""),
                "city": get_value(bill_to, "city", ""),
                "state": get_value(bill_to, "stateCode", ""),
                "postalCode": get_value(bill_to, "zipCode", ""),
                "country": get_value(bill_to, "countryCode", "US"),
                "workPhone": get_value(bill_to, "phone", ""),
                "homePhone": get_value(bill_to, "phone", ""),
                "mobilePhone": get_value(bill_to, "phone", ""),
                "email": customer_email
            } if bill_to else None,
            
             "altPickupAddress": {"alternateLocationType": "HAL",
                                   "locationId": "Loc", 
                                   "firstName": "AltAnkita", 
                                   "lastName": "AltOjha", 
                                   "company": "AltCompany",
                                     "address1": "Alt Address1",
                                       "address2": "Alt Address2",
                                         "city": "New York",
                                           "state": "NY",
                                             "postalCode": "12345",
                                               "country": "US",
                                                 "phone": "8269081003"},

            "onHoldMessage": "On Hold",
            "specialInstructions": get_value(order_details, "shippingInstructions", ""),
            "doNotReconfigure": "",
		     "licenseType": "",
            "gift": "Y" if get_value(order_details, "giftMessage") else "N",
            "giftMessage": get_value(order_details, "giftMessage", ""),
            "orderNo3P": "",
            "shippingTotal": "",
            "freightTax": None,
            "icePack": None,
            "additionalFields": "",
		     "saturdayDelivery": "N",
            "insuranceAmount": format_numeric(get_value(order_details, "total", 0)),
            "lob": None,
            "orderItems": order_items
        }]
    }


    skus = [item["productSKU"] for item in order_items]

    return api_body , skus

def extract_order_no(data):
    try:
        # Check if 'acceptedOrders' exists and is a list
        if 'acceptedOrders' in data and isinstance(data['acceptedOrders'], list):
            # Extract 'orderNo' from each order in 'acceptedOrders'
            order_nos = [order.get('orderNo', 'Unknown OrderNo') for order in data['acceptedOrders']]
            return order_nos
        else:
            return None
    except Exception as e:
        return {"status": "error", "message": f"Unexpected error: {e}"}

def extract_order_info(data):
    """
    Extracts order numbers from acceptedOrders and error messages from rejectedOrders.
    
    Args:
        data (dict): Response containing acceptedOrders and rejectedOrders lists
        
    Returns:
        dict: Dictionary containing accepted order numbers and rejection messages
    """
    try:
        result = {
            'accepted_orders': [],
            'rejected_orders': []
        }
        
        # Process accepted orders
        if 'acceptedOrders' in data and isinstance(data['acceptedOrders'], list):
            result['accepted_orders'] = [
                order.get('orderNo', 'Unknown OrderNo') 
                for order in data['acceptedOrders']
            ]
            
        # Process rejected orders
        if 'rejectedOrders' in data and isinstance(data['rejectedOrders'], list):
            result['rejected_orders'] = [
                {
                    'order_no': order.get('orderNo', 'Unknown OrderNo'),
                    'error': order.get('description', {}).get('error', 'Unknown error')
                }
                for order in data['rejectedOrders']
            ]
            
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Unexpected error: {e}"
        }