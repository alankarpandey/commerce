from datetime import datetime
from typing import Dict, Any, List, Optional
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

def status_api_body(order_no):
    return {
        "clientCode": "CLTUR",
        "detailView": "0",
        "orderNo": [order_no]
    }

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

def get_orders_from_mysql():
    try:
        conn = create_connection()
        
        if conn.is_connected():
            cursor = conn.cursor()
            
            # Execute query to get order numbers
            query = "SELECT order_no FROM order_detail"
            cursor.execute(query)
            
            # Fetch all order numbers
            orders = cursor.fetchall()
            
            # Extract order numbers from tuples
            order_numbers = [order[0] for order in orders]
            
            return order_numbers

    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return []
        
    finally:
        # Close database connections
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed")

data = {'responseStatus': True, 
        'responseStatusCode': 200, 
        'responseObject': {'status': 'success', 
                           'results': [{
                               'orderNo': 'CLTUR-123486', 
                               'statusCode': '1',
                                'statusMessage': 'Created', 
                                'carrierId': 'UPS',
                                'fulfillmentType': '5',
                                'shipDate': 'Sun Jan 19 00:00:00 UTC 2025',
                                'orderTrackingURL': None,
                                 'onHold': '0',
                                'deficientSkus': None,
                                'orderItems': None,
                                               'parcels': [{'internalTrackingNo': None,
                                                             'carrierTrackingNo': None, 'carrierServiceType': None, 'updates': [{'statusCode': '1', 'statusDate': '01-17-2025 06:54:17 000 +0000', 'statusMessage': 'Created', 'city': 'San Francisco', 'state': 'CA'}]}], 'specialInstructions': None, 'manifestLocation': None, 'depletionLocation': None, 'address': None, 'tiveTrackingDetails': None}, {'orderNo': 'CLTUR-CLTUR-test0003', 'statusCode': '1', 'statusMessage': 'Created', 'carrierId': 'UPS', 'fulfillmentType': '5', 'shipDate': 'Thu Jan 16 00:00:00 UTC 2025', 'orderTrackingURL': None, 'onHold': '0', 'deficientSkus': None, 'orderItems': None, 'parcels': [{'internalTrackingNo': None, 'carrierTrackingNo': None, 'carrierServiceType': None, 'updates': [{'statusCode': '1', 'statusDate': '01-16-2025 09:02:42 000 +0000', 'statusMessage': 'Created', 'city': 'adelanto', 'state': 'CA'}]}], 'specialInstructions': None, 'manifestLocation': None, 'depletionLocation': None, 'address': None, 'tiveTrackingDetails': None}, {'orderNo': 'CLTUR-CLTUR-001120189', 'statusCode': '1', 'statusMessage': 'Created', 'carrierId': 'UPS', 'fulfillmentType': '5', 'shipDate': 'Mon Jan 20 00:00:00 UTC 2025', 'orderTrackingURL': None, 'onHold': '0', 'deficientSkus': None, 'orderItems': None, 'parcels': [{'internalTrackingNo': None, 'carrierTrackingNo': None, 'carrierServiceType': None, 'updates': [{'statusCode': '18', 'statusDate': '01-14-2025 17:58:39 000 +0000', 'statusMessage': 'Back Order', 'city': 'Bloomington', 'state': 'CA'}, {'statusCode': '1', 'statusDate': '01-14-2025 18:33:26 000 +0000', 'statusMessage': 'Created', 'city': 'Bloomington', 'state': 'CA'}]}], 'specialInstructions': None, 'manifestLocation': None, 'depletionLocation': None, 'address': None, 'tiveTrackingDetails': None}, {'orderNo': 'CLTUR-2075', 'statusCode': '1', 'statusMessage': 'Created', 'carrierId': 'UPS', 'fulfillmentType': '5', 'shipDate': 'Tue Jan 14 00:00:00 UTC 2025', 'orderTrackingURL': None, 'onHold': '0', 'deficientSkus': None, 'orderItems': None, 'parcels': [{'internalTrackingNo': None, 'carrierTrackingNo': None, 'carrierServiceType': None, 'updates': [{'statusCode': '18', 'statusDate': '01-14-2025 17:56:31 000 +0000', 'statusMessage': 'Back Order', 'city': 'Culver City', 'state': 'CA'}, {'statusCode': '1', 'statusDate': '01-14-2025 18:31:22 000 +0000', 'statusMessage': 'Created', 'city': 'Culver City', 'state': 'CA'}]}], 'specialInstructions': None, 'manifestLocation': None, 'depletionLocation': None, 'address': None, 'tiveTrackingDetails': None}], 'errors': []}}

def check_order_status(data: Dict[str, Any], target_order_no: str) -> Dict[str, Any]:
    try:
        # Check if the response structure is valid
        if not all(key in data for key in ['responseStatus', 'responseStatusCode', 'responseObject']):
            return {
                "status": "error",
                "message": "Invalid response structure"
            }

        # Extract results from response
        results = data.get('responseObject', {}).get('results', [])
        
        # Find the specific order
        target_order = None
        for order in results:
            if order.get('orderNo') == target_order_no:
                target_order = order
                break
                
        if not target_order:
            return {
                "status": "error",
                "message": f"Order {target_order_no} not found"
            }

        # Process parcels and updates
        latest_status = None
        latest_date = None
        status_code = None

        for parcel in target_order.get('parcels', []):
            for update in parcel.get('updates', []):
                status_date = update.get('statusDate')
                if status_date:
                    # Convert status date string to datetime object
                    try:
                        current_date = datetime.strptime(status_date, '%m-%d-%Y %H:%M:%S %f %z')
                        if latest_date is None or current_date > latest_date:
                            latest_date = current_date
                            latest_status = update.get('statusMessage')
                            status_code = update.get('statusCode')
                    except ValueError:
                        continue

        if latest_status:
            return {
                "status": "success",
                "orderNo": target_order_no,
                "latestStatus": latest_status,
                "statusDate": latest_date.strftime('%Y-%m-%d %H:%M:%S') if latest_date else None,
                "statusCode": status_code
            }
        else:
            return {
                "status": "error",
                "message": f"No status updates found for order {target_order_no}"
            }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }

def update_order_status(order_no, status_message, status_code, statusDate):
    try:
        conn = create_connection()
        if conn.is_connected():
            cursor = conn.cursor()
            
            # Current timestamp
            current_time = datetime.now()
            
            # Update query
            update_query = """
                UPDATE order_detail 
                SET status_message = %s,
                    status_code = %s,
                    status_date = %s
                WHERE order_no = %s
            """
            
            # Values to update
            values = (status_message, status_code, statusDate, order_no)
            
            # Execute update
            cursor.execute(update_query, values)
            
            # Commit changes
            conn.commit()
            
            print(f"Order {order_no} status updated successfully")
            return True

    except Error as e:
        print(f"Error updating order status: {e}")
        return False
        
    finally:
        # Close database connections
        if 'connection' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed")

#-----product function----------
def get_product_from_mysql():
    try:
        conn = create_connection()
        
        if conn.is_connected():
            cursor = conn.cursor()
            
            # Execute query to get order numbers
            query = "SELECT product_sku FROM product"
            cursor.execute(query)
            
            # Fetch all order numbers
            orders = cursor.fetchall()
            
            # Extract order numbers from tuples
            order_numbers = [order[0] for order in orders]
            
            return order_numbers

    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return []
        
    finally:
        # Close database connections
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed")            

def inventory_api_body(product_sku):
    return {  
        "clientCode": "CLTUR",  
        "facility": "CLTURNP",  
        "sku": [  
                product_sku  
                ]  
        }

def extract_inventory_quantities(data):
    """
    Extracts quantityAvailable and quantityTotal from the given data structure.
    
    Args:
        data (dict): JSON response containing 'items' list with inventory data
        
    Returns:
        tuple: (quantityAvailable, quantityTotal) from the first item
        
    Raises:
        KeyError: If required keys are missing
        IndexError: If items list is empty
    """
    try:
        # Check if the response structure is valid
        if not all(key in data for key in ['responseStatus', 'responseStatusCode', 'responseObject']):
            return {
                "status": "error",
                "message": "Invalid response structure"
            }
        
        results = data.get('responseObject', {}).get('results', [])

        first_item = results['items'][0]
        return first_item['quantityAvailable'], first_item['quantityTotal']
        
    except (KeyError, IndexError) as e:
        raise Exception("Failed to extract quantities: " + str(e))

def update_product_quantity( product_sku, quantity_available, quantity_total):

    try:
        conn = create_connection()
        if conn.is_connected():
            cursor = conn.cursor()

            # SQL query to update the product table
            update_query = """
            UPDATE product
            SET quantity_available = %s, quantity_total = %s
            WHERE product_sku = %s;
            """
            
            # Execute the query
            cursor.execute(update_query, (quantity_available, quantity_total, product_sku))

            # Commit the changes
            conn.commit()

            # Check if any row was updated
            if cursor.rowcount > 0:
                return f"Product with SKU '{product_sku}' updated successfully."
            else:
                return f"No product found with SKU '{product_sku}'."

    except mysql.connector.Error as e:
        return f"Error: {e}"
    finally:
        # Close the database connection
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and conn.is_connected():
            conn.close()
