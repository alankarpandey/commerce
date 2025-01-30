import requests
from flask import Flask, request , jsonify
from dotenv import load_dotenv
load_dotenv()
# from pyngrok import ngrok
# from flask_cors import CORS
import requests
import json
import mysql.connector
from mysql.connector import Error
from product_create_utile import generate_product_api_body,create_table_if_not_exists,sku_name,add_product
from order_create_utile import generate_order_api_body, check_and_create_table, check_product_status, generate_status_api_body, extract_order_no,extract_quantities, update_inventory_status, update_product_details, add_order_no
from status_utile import get_orders_from_mysql, status_api_body ,check_order_status, update_order_status , get_product_from_mysql, inventory_api_body, extract_inventory_quantities, update_product_quantity
from logger import logging
import traceback
from dotenv import load_dotenv
load_dotenv()
import os
# def create_connection():
    
#     try:
#         return mysql.connector.connect(
#             host="127.0.0.1",
#             port=3306,
#             user="root",
#             password="1234",
#             database="db"
#         )
#     except mysql.connector.Error as error:
#         print(f"Error connecting to the database: {error}")
#         return None

app = Flask(__name__)
# ngrok.set_auth_token('2rFVSsSTDJG097S6OjscIfczJAW_FSuWypNgwL53wpjoffvp')
# public_url = ngrok.connect(5000).public_url
# CORS(app, resources={r"/ask": {"origins": "*"}})



create_product_advatix_url = os.getenv('create_product_advatix_url')
process_order_advatix_url = os.getenv('process_order_advatix_url')
patch_order_advatix_url = os.getenv('patch_order_advatix_url')
fatch_order_status_url = os.getenv('fatch_order_status_url')
fatch_inventory_status_url = os.getenv('fatch_inventory_status_url')

HEADERS = os.getenv('header')


@app.route('/', methods=['GET'])
def home():
    return "Welcome to the Flask app!"


@app.route("/ask/insert_advatix", methods=["POST"])
def insert_product():
    try:
        raw_data = request.get_data()
        print(raw_data)
        logging.info("log product :sucessfuly get raw data of product")
        # Parse the raw data and generate the API body
        data = generate_product_api_body(json.loads(raw_data.decode("utf-8")))
        print({"data":data})
        logging.info("create product body for update in advatix")
        # Make the POST request to the Advatix API
        response = requests.post(create_product_advatix_url, headers=HEADERS, data=json.dumps(data))
        print(response.json())

        create_table_if_not = create_table_if_not_exists(
                                        database='db',  
                                        table_name='product'      
                                    )
        
        print(create_table_if_not)
        logging.info(create_table_if_not)
        # print(create_table_if_not)
        response_json = response.json()
        
        if response_json["message"] == "success":
            product_sku, product_name = sku_name(data)

            print(product_sku)
            print(product_name)
            logging.info(f"sucessfully extract sku and name :{product_sku} | {product_name}")
            add_product_detail = add_product(product_name=product_name, product_sku=product_sku)
            print(add_product_detail)
            logging.info(add_product_detail)
        # if response['message'] == "failure":
        #     return response.json()
        
        # Log and return the response
        response_data = {
            "status": response.status_code,
            "data": response.json()
        }

        print(response_data)
        logging.info(response_data)
        return jsonify(response_data), response.status_code  
    
    except Exception as e:
        # Handle and log any errors
        error_message = {
            "status": "error",
            "message": str(e)
        }
        print(error_message)
        logging.info(error_message)
        return jsonify(error_message), 500  
    

@app.route("/ask/order_process", methods=["POST"])
def process_order():
    try:
        raw_data = request.get_data()
        print({"raw data": raw_data})
        logging.info("log order: get raw data for order")
        # Parse the raw data and generate the API body
        data , skus = generate_order_api_body(json.loads(raw_data.decode("utf-8")))
        print({"data":data})
        logging.info(f"generate api body for order. {data}")
        # product_sku = get_sku(data=data)  
        print(str(skus))
        
        for sku in skus:
            inventory_check = check_product_status(product_sku=sku)

            print(f"check inventory status: {inventory_check}")
            logging.info(f"check inventory status: {inventory_check}")

            if inventory_check == "Unknown":
                status_check = generate_status_api_body(sku)
                response = requests.post(fatch_inventory_status_url, headers=HEADERS, data=json.dumps(status_check))     
                invetory_detail = response.json()
                logging.info(invetory_detail)

                result = extract_quantities(invetory_detail)
                print(result)
                logging.info(result)

                for i in result:
                    if i["quantityTotal"] == None:
                        result = update_inventory_status(product_sku=sku, new_status="Out of Stock")
                        print("code for breske loop and exit from order")
                        logging.info(f"Inventory is not found of this sku : {sku}")
                        return f"Inventory is not found of this sku : {sku}"
                    else:
                        result = update_product_details(product_sku=sku,
                                                            new_status="In Stock",
                                                            quantity_available= int(i['quantityAvailable']),
                                                            quantity_total= int(i['quantity_total']))    
                        
                        logging.info(result)
            
            if inventory_check == "In Stock":
                print(f"stock is found in database: {sku}")
                logging.info(f"stock is found in database: {sku}")

            if inventory_check == "Out of Stock":
                print(f"Out of stock is found in database: {sku}")  
                logging.info(f"Out of stock is found in database: {sku}")  
                return f"Out of stock is found in database: {sku}"
        
        
        # Make the POST request to the Advatix API
        response = requests.post(process_order_advatix_url, headers=HEADERS, data=json.dumps(data))
        

        response_json = response.json()
        logging.info(f"process order response :{response_json}")
        
        order_no = extract_order_no(response_json)
        logging.info(f"extract order no : {str(order_no)}")
        print(order_no)
        if order_no:
            order_table = check_and_create_table()
            print(f"create order table :{order_table}")
            logging.info(f"create order table :{order_table}")
            result = add_order_no(str(order_no[0]))
            print({"order no add": result})
            logging.info(f"order no add: {result}")

        # Log and return the response
        response_data = {
            "status": response.status_code,
            "data": response.json()
        }

        print(f"End == {response_data}")
        logging.info(f"End == {response_data}")
        return jsonify(response_data), response.status_code  

    except Exception as e:
        # Handle and log any errors
        print(traceback.format_exc())
        error_message = {
            "status": "error",
            "message": str(e)
        }
        print(error_message)
        logging.info(error_message)
        return jsonify(error_message), 500      
    

@app.route("/ask/order_status_update", methods=["POST"])
def status_update_corn():
    try:

    #------ product code remaining --------
        product_sku_database = get_product_from_mysql()
        logging.info("load product sku from database")
        for i in product_sku_database:
            payload = inventory_api_body(str(i))
            response = requests.get(fatch_inventory_status_url, headers=headers, json=payload)
            data = response.json()
            result = extract_quantities(response=data)
            logging.info(f"quantityAvailable: {result["quantityAvailable"]} | quantityTotal: {result["quantityTotal"]}")
            if result["quantityTotal"] != None:
                updata_quantity = update_product_quantity(
                    product_sku=str(i),
                    quantity_available=int(result["quantityAvailable"]),
                    quantity_total=int(result["quantityTotal"])
                )

                print(updata_quantity)
                logging.info({"updata_quantity": updata_quantity})

            else:
                logging.info("invetory is not present, continue")
                continue    


    
    #------ order corn job -----------
        order_no_database = get_orders_from_mysql()
        logging.info("load order no. from database")
        for i in order_no_database:
            payload = status_api_body(str(i))
            response = requests.get(fatch_order_status_url, headers=headers, json=payload)
            data = response.json()
            result =  check_order_status(data=data, target_order_no= str(i))
            logging.info(f"result status: {result['status']} | order no.: {result['order_no']}")
            if result['status'] == "success":
                updata_order = update_order_status(
                    order_no=result['order_no'],
                    status_message=result['status_message'],
                    status_code=result['statusCode'],
                    statusDate= result['statusDate']
                )

                print(updata_order)
                logging.info(updata_order)
            else:
                logging.info("result status is not success continue")
                continue

    except Exception as e:
        # Handle and log any errors
        error_message = {
            "status": "error",
            "message": str(e)
        }
        print(error_message)
        logging.info(error_message)
        return jsonify(error_message), 500           


# print(f"To access the global link, please click {public_url}")
# app.run(port=5000)
if __name__ == '__main__':
    app.run(debug=True)


