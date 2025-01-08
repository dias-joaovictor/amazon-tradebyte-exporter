import os
import csv
import shutil

from pymongo import MongoClient
import xml.etree.ElementTree as ET
import uuid
import xml.dom.minidom as minidom
from datetime import datetime


def clear_directory(directory_path):
    """
    Removes all files and subdirectories in the given directory.
    :param directory_path: Path to the directory to be cleared.
    """
    if not os.path.exists(directory_path):
        print(f"Directory {directory_path} does not exist.")
        return

    for item in os.listdir(directory_path):
        item_path = os.path.join(directory_path, item)
        try:
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path)  # Delete the file or symbolic link
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)  # Delete the directory and its contents
        except Exception as e:
            print(f"Failed to delete {item_path}. Reason: {e}")

def parse_txt_to_objects(file_path):
    """
    Parse a tab-delimited TXT file into Python dictionaries.

    :param file_path: Path to the TXT file.
    :return: List of dictionaries, where each dictionary represents a record.
    """
    objects = []
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            row["order_date"] = datetime.fromisoformat(row["purchase-date"])
            objects.append(row)
    return objects


def query_mongodb(database="amazon_orders", collection="orders"):
    """
    Query MongoDB with a specific filter.

    :param database: Database name in MongoDB.
    :param collection: Collection name in MongoDB.
    :return: List of matching documents.
    """
    query = {
        "order_date": {
            "$gt": datetime(2024, 12, 23, 13, 45, 0),  # Start date
            "$lt": datetime(2025, 1, 2, 17, 15, 0)     # End date
        },
        "is-buyer-requested-cancellation": "false"
    }

    client = MongoClient("mongodb://root:root@localhost:27017/")
    db = client[database]
    col = db[collection]

    # Execute the query
    results = list(col.find(query))
    print(f"Found {len(results)} matching records.")
    return results


def save_to_mongodb(data, database="amazon_orders", collection="orders"):
    """
    Save a list of Python dictionaries to MongoDB.

    :param data: List of dictionaries to save.
    :param database: Database name in MongoDB.
    :param collection: Collection name in MongoDB.
    """
    client = MongoClient("mongodb://root:root@localhost:27017/")
    db = client[database]
    col = db[collection]
    col.drop()
    col.insert_many(data)
    print(f"Saved {len(data)} records to MongoDB in the '{collection}' collection of the '{database}' database.")


def save_xml_to_out_directory(xml_data, output_file, formatted_file):
    """
    Save the XML data to the `out/` directory in both formatted and inline formats.

    :param xml_data: The XML data as bytes.
    :param output_file: The name of the output inline XML file.
    :param formatted_file: The name of the formatted output XML file.
    """
    output_dir = "out"
    os.makedirs(output_dir, exist_ok=True)  # Create the `out/` directory if it doesn't exist

    # Save the inline version
    inline_file_path = os.path.join(output_dir, output_file)
    with open(inline_file_path, "wb") as file:
        file.write(xml_data)
    print(f"Inline XML saved to: {inline_file_path}")

    # Save the formatted version
    formatted_file_path = os.path.join(output_dir, formatted_file)
    parsed_xml = minidom.parseString(xml_data)
    pretty_xml = parsed_xml.toprettyxml(encoding="utf-8")
    with open(formatted_file_path, "wb") as file:
        file.write(pretty_xml)
    print(f"Formatted XML saved to: {formatted_file_path}")


def generate_amazon_xml(orders, merchant_id="A2DKZN1W9ZO5KL"):
    """
    Generate XML based on the list of orders.

    :param orders: Dictionary of orders, where keys are order IDs, and values are dictionaries with order data.
    :param merchant_id: Merchant ID for the XML header.
    :return: XML data as bytes.
    """
    # Create the root AmazonEnvelope element
    amazon_envelope = ET.Element("AmazonEnvelope", {
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xsi:noNamespaceSchemaLocation": "amzn-envelope.xsd"
    })

    # Header section
    header = ET.SubElement(amazon_envelope, "Header")
    ET.SubElement(header, "DocumentVersion").text = "1.01"
    ET.SubElement(header, "MerchantIdentifier").text = merchant_id
    ET.SubElement(amazon_envelope, "MessageType").text = "OrderReport"

    # Create a message for each order
    for message_id, order_id in enumerate(orders, start=1):
        order = orders[order_id]["orders"][0]

        message = ET.SubElement(amazon_envelope, "Message")
        ET.SubElement(message, "MessageID").text = str(message_id)

        # OrderReport Section
        order_report = ET.SubElement(message, "OrderReport")
        ET.SubElement(order_report, "AmazonOrderID").text = order_id
        ET.SubElement(order_report, "AmazonSessionID").text = str(uuid.uuid4())
        ET.SubElement(order_report, "OrderDate").text = order["purchase-date"]
        ET.SubElement(order_report, "OrderPostedDate").text = order["payments-date"]

        # Billing Data
        billing_data = ET.SubElement(order_report, "BillingData")
        ET.SubElement(billing_data, "BuyerEmailAddress").text = order["buyer-email"]
        ET.SubElement(billing_data, "BuyerName").text = order["buyer-name"]
        ET.SubElement(billing_data, "BuyerPhoneNumber").text = order.get("buyer-phone-number", "")

        billing_address = ET.SubElement(billing_data, "Address")
        ET.SubElement(billing_address, "Name").text = order["bill-name"]
        ET.SubElement(billing_address, "AddressFieldOne").text = order["bill-address-1"]
        ET.SubElement(billing_address, "AddressFieldTwo").text = order["bill-address-2"]
        ET.SubElement(billing_address, "AddressFieldThree").text = order["bill-address-3"]
        ET.SubElement(billing_address, "City").text = order["bill-city"]
        ET.SubElement(billing_address, "StateOrRegion").text = order["bill-state"]
        ET.SubElement(billing_address, "PostalCode").text = order["bill-postal-code"]
        ET.SubElement(billing_address, "CountryCode").text = order["bill-country"]

        # Fulfillment Data
        fulfillment_data = ET.SubElement(order_report, "FulfillmentData")
        ET.SubElement(fulfillment_data, "FulfillmentMethod").text = "Ship"
        ET.SubElement(fulfillment_data, "FulfillmentServiceLevel").text = order["ship-service-level"]

        fulfillment_address = ET.SubElement(fulfillment_data, "Address")
        ET.SubElement(fulfillment_address, "Name").text = order["recipient-name"]
        ET.SubElement(fulfillment_address, "AddressFieldOne").text = order["ship-address-1"]
        ET.SubElement(fulfillment_address, "AddressFieldTwo").text = order["ship-address-2"]
        ET.SubElement(fulfillment_address, "AddressFieldThree").text = order["ship-address-3"]
        ET.SubElement(fulfillment_address, "City").text = order["ship-city"]
        ET.SubElement(fulfillment_address, "StateOrRegion").text = order["ship-state"]
        ET.SubElement(fulfillment_address, "PostalCode").text = order["ship-postal-code"]
        ET.SubElement(fulfillment_address, "CountryCode").text = order["ship-country"]
        ET.SubElement(fulfillment_address, "PhoneNumber").text = order.get("ship-phone-number", "")

        # Additional Order Details
        ET.SubElement(order_report, "IsBusinessOrder").text = str(order.get("is-business-order", False)).lower()
        ET.SubElement(order_report, "IsPrime").text = str(order.get("is-prime", False)).lower()
        ET.SubElement(order_report, "IsPremiumOrder").text = str(order.get("is-premium-order", False)).lower()
        ET.SubElement(order_report, "IsIba").text = str(order.get("is-iba", False)).lower()

        # Add Items for the Order
        for item in orders[order_id]["orders"]:
            item_element = ET.SubElement(order_report, "Item")
            ET.SubElement(item_element, "AmazonOrderItemCode").text = item["order-item-id"]
            ET.SubElement(item_element, "SKU").text = item["sku"]
            ET.SubElement(item_element, "Title").text = item["product-name"]
            ET.SubElement(item_element, "Quantity").text = str(item["quantity-purchased"])
            ET.SubElement(item_element, "ProductTaxCode").text = "A_GEN_STANDARD"

            # ItemPrice
            item_price = ET.SubElement(item_element, "ItemPrice")

            component_element = ET.SubElement(item_price, "Component")
            ET.SubElement(component_element, "Type").text = "Principal"
            ET.SubElement(component_element, "Amount", {"currency": item["currency"]}).text = str(item["item-price"])

            component_element = ET.SubElement(item_price, "Component")
            ET.SubElement(component_element, "Type").text = "Shipping"
            ET.SubElement(component_element, "Amount", {"currency": item["currency"]}).text = str(item["shipping-price"])

            component_element = ET.SubElement(item_price, "Component")
            ET.SubElement(component_element, "Type").text = "Tax"
            ET.SubElement(component_element, "Amount", {"currency": item["currency"]}).text = str(item["item-tax"])

            component_element = ET.SubElement(item_price, "Component")
            ET.SubElement(component_element, "Type").text = "ShippingTax"
            ET.SubElement(component_element, "Amount", {"currency": item["currency"]}).text = str(item["shipping-tax"])



            # ItemFees
            item_fees = ET.SubElement(item_element, "ItemFees")
            item_fee_element = ET.SubElement(item_fees, "Fee")
            ET.SubElement(item_fee_element, "Type").text = "Commission"
            ET.SubElement(item_fee_element, "Amount", {"currency": item["currency"]}).text = str(item["payment-method-fee"])


    # Return the XML string
    return ET.tostring(amazon_envelope, encoding="utf-8")


def run():
    clear_directory("out/")
    # Folder and file configurations
    input_folder = "in/"
    database = "amazon_orders"
    order_grouped_with_order_lines = "orders"

    # Loop through all TXT files in the input folder
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".txt"):
            file_path = os.path.join(input_folder, file_name)
            print(f"Parsing file: {file_path}")

            # Parse TXT file to objects
            data = parse_txt_to_objects(file_path)
            print(f"Parsed {len(data)} records from {file_name}.")

            # Save parsed data to MongoDB
            save_to_mongodb(data, database, order_grouped_with_order_lines)
            print(f"Saved {len(data)} records to {database}.")

            data = query_mongodb()
            print(f"Retrieved {len(data)} records.")
            
            
            print(f"Finished processing {file_name}.\n")

            collection_per_channel = {}
            for item in data:
                channel = item["sales-channel"]
                if channel not in collection_per_channel:
                    collection_per_channel[channel] = []
                collection_per_channel[channel].append(item)


            counter = 0
            for channel_name, orders in collection_per_channel.items():
                order_grouped_with_order_lines = {}

                for order in orders:
                    order_id = order["order-id"]
                    if order_id not in order_grouped_with_order_lines:
                        order_grouped_with_order_lines[order_id] = {"orders": []}

                    order_grouped_with_order_lines[order_id]["orders"].append(order)

                counter = counter + len(order_grouped_with_order_lines)
                xml_report = generate_amazon_xml(order_grouped_with_order_lines)
                save_xml_to_out_directory(xml_report, f'{channel_name}.xml', f"{channel_name}.formatted.xml")

                print(f"Finished processing {channel_name} channel.")

            print(f"Finished processing. Total {counter} records.")


if __name__ == "__main__":
    run()
