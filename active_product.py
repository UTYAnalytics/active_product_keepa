# Remember to close the browser
import tempfile
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import hashlib
import os
import time
import pandas as pd
import psycopg2
import glob
from supabase import create_client, Client
from datetime import date
import re
import unicodedata
from selenium.common.exceptions import TimeoutException
import imaplib
import email
import re
import chromedriver_autoinstaller
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime, timezone, timedelta
import numpy as np
from pyvirtualdisplay import Display
from urllib.parse import urlparse
from sqlalchemy import create_engine as sql_create_engine
from sqlalchemy.sql import text


def wait_for_value_greater_than_zero(driver, locator):
    # Wait for the element to be present
    element = WebDriverWait(driver, 20).until(EC.presence_of_element_located(locator))

    # Continuously check the value until it's greater than 0
    while True:
        # Get the current value of the element
        current_value = float(element.text)  # Assuming the value is numeric

        if current_value > 0:
            break  # Exit the loop if the condition is met

        # Wait for a short interval before checking again
        WebDriverWait(driver, 5).until(
            EC.text_to_be_present_in_element(locator, str(current_value))
        )


def get_otp_from_email(server, email_address, email_password, subject_filter):
    mail = imaplib.IMAP4_SSL(server)
    mail.login(email_address, email_password)
    mail.select("inbox")

    status, data = mail.search(None, '(SUBJECT "{}")'.format(subject_filter))
    mail_ids = data[0].split()

    latest_email_id = mail_ids[-1]
    status, data = mail.fetch(latest_email_id, "(RFC822)")

    raw_email = data[0][1].decode("utf-8")
    email_message = email.message_from_bytes(data[0][1])

    otp_pattern = re.compile(r"\b\d{6}\b")

    if email_message.is_multipart():
        for part in email_message.walk():
            content_type = part.get_content_type()
            if "text/plain" in content_type or "text/html" in content_type:
                email_content = part.get_payload(decode=True).decode()
                match = otp_pattern.search(email_content)
                if match:
                    return match.group(0)
    else:
        email_content = email_message.get_payload(decode=True).decode()
        match = otp_pattern.search(email_content)
        if match:
            return match.group(0)

    return None


def export_file(seller_id):
    # Initialize the Chrome driver with the options
    driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

    # Open Keepa
    driver.get("https://keepa.com/#!")
    time.sleep(20)

    wait = WebDriverWait(driver, 2000000)
    # Login process
    try:
        login_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="panelUserRegisterLogin"]'))
        )
        login_button.click()
        print("login button clicked")
        time.sleep(2)

        username_field = wait.until(
            EC.visibility_of_element_located((By.ID, "username"))
        )
        username_field.send_keys(username)
        print("enter use_name")
        time.sleep(0.5)

        password_field = driver.find_element(By.ID, "password")
        password_field.send_keys(password)
        print("enter password")
        time.sleep(1)

        password_field.send_keys(Keys.RETURN)
        print("press ENTER to login")
        time.sleep(10)
    except:
        raise Exception
        # print("Error during login:", e)

    # Navigate to the product_finder
    try:
        data_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="topMenu"]/li[4]/a/span'))
        )
        data_button.click()
        print("data clicked")
        time.sleep(2)
        sellerlookup_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="subPanel"]/ul[3]/li[5]/a'))
        )
        sellerlookup_button.click()
        print("sellerlookup clicked")
        time.sleep(1.5)

        seller_id_field = wait.until(
            EC.visibility_of_element_located((By.ID, "sellerLookupId"))
        )
        seller_id_field.send_keys(seller_id)
        print("seller_id sended")
        time.sleep(1)

        submit_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="sellerLookupSubmit"]'))
        )
        submit_button.click()
        print("submit clicked")
        time.sleep(5)

        storefront_button = driver.find_element(
            By.XPATH, '//*[@id="icon-text-tab-bar"]/a[2]/span'
        )
        storefront_button.click()
        print("storefront clicked")
        time.sleep(2)
        # Logic to handle the presence of a specific popup
        try:
            # Wait for a certain amount of time for the popup to appear
            WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.ID, "popup3"))
            )
            raise Exception("Popup detected, skipping to next retailer")
        except TimeoutException:
            showrow_button = wait.until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        '//*[@id="grid-tools-storefront"]/div[1]/span[2]/span/span',
                    )
                )
            )
            showrow_button.click()
            print("showrow clicked")
            time.sleep(1)

            allrow_button = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="tool-row-menu"]/ul/li[7]')
                )
            )
            allrow_button.click()
            print("allowrow clicked")
            time.sleep(60)

            export_button = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="grid-tools-storefront"]/div[1]/span[3]/span')
                )
            )
            export_button.click()
            print("export clicked")
            time.sleep(5)
            final_download_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="exportSubmit"]'))
            )
            final_download_button.click()
            print("download clicked")
            time.sleep(5)
            driver.quit()
    except Exception as e:
        driver.quit()
        raise e


def get_newest_file(directory):
    files = glob.glob(os.path.join(directory, "*"))
    if not files:  # Check if the files list is empty
        return None
    newest_file = max(files, key=os.path.getmtime)
    return newest_file


def transform_data(newest_file_path):

    # Get the current UTC time
    current_utc_time = datetime.utcnow()

    # Calculate the time difference for GMT+7
    gmt7_offset = timedelta(hours=7)

    # Get the current date and time in GMT+7
    current_time_gmt7 = current_utc_time + gmt7_offset
    if newest_file_path:
        data = pd.read_csv(newest_file_path)
        data["sys_run_date"] = current_time_gmt7.strftime("%Y-%m-%d %H:%M:%S")
        # Proceed with the database insertion
    else:
        print("No files found in the specified directory.")

    print("transform header")

    def format_header(header):
        # Convert to lowercase
        header = header.lower()
        # Replace spaces with underscores
        header = header.replace(" ", "_")
        # Remove Vietnamese characters by decomposing and keeping only ASCII
        header = (
            unicodedata.normalize("NFKD", header)
            .encode("ASCII", "ignore")
            .decode("ASCII")
        )
        return header

    # Extract the header row
    headers = [
        "Locale",
        "Image_URLs",
        "Title",
        "Sales_Rank_Current",
        "Sales_Rank_90_Days_Avg",
        "Sales_Rank_90_Days_Drop_Percent",
        "Sales_Rank_Drops_Last_90_Days",
        "Sales_Rank_Reference",
        "Sales_Rank_Subcategory_Sales_Ranks",
        "Bought_Past_Month",
        "Reviews_Rating",
        "Reviews_Review_Count",
        "Reviews_Review_Count_90_Days_Drop_Percent",
        "Ratings_Format_Specific",
        "Review_Count_Format_Specific",
        "Last_Price_Change",
        "Buy_Box_Current_Price",
        "Buy_Box_90_Days_Avg_Price",
        "Buy_Box_90_Days_Drop_Percent",
        "Buy_Box_Stock",
        "Buy_Box_90_Days_OOS_Percent",
        "Buy_Box_Seller",
        "Buy_Box_Percent_Amazon_90_Days",
        "Buy_Box_Percent_Top_Seller_90_days",
        "Buy_Box_Winner_Count_90_days",
        "Buy_Box_Is_FBA",
        "Buy_Box_Unqualified",
        "Amazon_Current_Price",
        "Amazon_90_Days_Avg_Price",
        "Amazon_90_Days_Drop_Percent",
        "Amazon_90_Days_OOS_Percent",
        "New_Current_Price",
        "New_90_Days_Avg_Price",
        "New_90_Days_Drop_Percent",
        "New_90_Days_OOS_Percent",
        "New_3rd_Party_FBA_Current_Price",
        "New_3rd_Party_FBA_90_Days_Avg_Price",
        "New_3rd_Party_FBA_90_Days_Drop_Percent",
        "FBA_PickAndPack_Fee",
        "Referral_Fee_Percent",
        "Referral_Fee_Current_Price",
        "New_3rd_Party_FBM_Current_Price",
        "New_3rd_Party_FBM_90_Days_Avg_Price",
        "New_3rd_Party_FBM_90_Days_Drop_Percent",
        "New_Prime_Exclusive_Current_Price",
        "New_Prime_Exclusive_90_Days_Avg_Price",
        "New_Prime_Exclusive_90_Days_Drop_Percent",
        "Lightning_Deals_Current_Price",
        "Lightning_Deals_Upcoming_Deal_Price",
        "Used_Current_Price",
        "Used_90_Days_Avg_Price",
        "Used_90_Days_Drop_Percent",
        "Used_90_Days_OOS_Percent",
        "Used_Like_New_Current_Price",
        "Used_Like_New_90_Days_Avg_Price",
        "Used_Like_New_90_Days_Drop_Percent",
        "Used_Very_Good_Current_Price",
        "Used_Very_Good_90_Days_Avg_Price",
        "Used_Very_Good_90_Days_Drop_Percent",
        "Used_Good_Current_Price",
        "Used_Good_90_Days_Avg_Price",
        "Used_Good_90_Days_Drop_Percent",
        "Used_Acceptable_Current_Price",
        "Used_Acceptable_90_Days_Avg_Price",
        "Used_Acceptable_90_Days_Drop_Percent",
        "Warehouse_Deals_Current_Price",
        "Warehouse_Deals_90_Days_Avg_Price",
        "Warehouse_Deals_90_Days_Drop_Percent",
        "List_Price_Current",
        "List_Price_90_Days_Avg",
        "List_Price_90_Days_Drop_Percent",
        "Rental_Current_Price",
        "Rental_90_Days_Avg_Price",
        "Rental_90_Days_Drop_Percent",
        "New_Offer_Count_Current",
        "New_Offer_Count_90_Days_Avg",
        "Count_of_Retrieved_Live_Offers_New_FBA",
        "Count_of_Retrieved_Live_Offers_New_FBM",
        "Used_Offer_Count_Current",
        "Used_Offer_Count_90_Days_Avg",
        "Tracking_Since",
        "Listed_Since",
        "Categories_Root",
        "Categories_Sub",
        "Categories_Tree",
        "Categories_Launchpad",
        "ASIN",
        "Product_Codes_EAN",
        "Product_Codes_UPC",
        "Product_Codes_PartNumber",
        "Parent_ASIN",
        "Variation_ASINs",
        "Freq_Bought_Together",
        "Type",
        "Manufacturer",
        "Brand",
        "Product_Group",
        "Model",
        "Variation_Attributes",
        "Color",
        "Size",
        "Edition",
        "Format",
        "Author",
        "Contributors",
        "Binding",
        "Number_of_Items",
        "Number_of_Pages",
        "Publication_Date",
        "Release_Date",
        "Languages",
        "Package_Dimension_cm3",
        "Package_Weight_g",
        "Package_Quantity",
        "Item_Dimension_cm3",
        "Item_Weight_g",
        "Hazardous_Materials",
        "Is_Hazmat",
        "Adult_Product",
        "Trade_In_Eligible",
        "Prime_Eligible",
        "Subscribe_and_Save",
        "One_Time_Coupon_Absolute",
        "One_Time_Coupon_Percentage",
        "Subscribe_and_Save_Coupon_Percentage",
        "sys_run_date",
    ]

    # Helper function to remove $ and convert to float
    def clean_currency(value):
        try:
            if pd.isna(value) or value == "-":
                return 0
            if isinstance(value, str):
                return float(value.replace("$", "").replace(",", "").strip())
            return float(value)
        except:
            return 0.00

    # Helper function to remove % and convert to percentage
    def clean_percentage(value):
        try:
            if pd.isna(value) or value == "-":
                return 0
            if isinstance(value, str):
                return float(value.replace("%", "").strip()) / 100
            return float(value)
        except:
            return 0.00

    headers = [format_header(h) for h in headers]
    # data=data.to_dict(orient='records')
    # Convert column headers
    data.columns = headers
    print("changed columns")

    # List of columns to apply the cleaning functions
    currency_columns = [
        "Buy_Box_Current_Price",
        "Buy_Box_90_Days_Avg_Price",
        "Amazon_Current_Price",
        "Amazon_90_Days_Avg_Price",
        "New_Current_Price",
        "New_90_Days_Avg_Price",
        "New_3rd_Party_FBA_Current_Price",
        "New_3rd_Party_FBA_90_Days_Avg_Price",
        "FBA_PickAndPack_Fee",
        "Referral_Fee_Current_Price",
        "New_3rd_Party_FBM_Current_Price",
        "New_3rd_Party_FBM_90_Days_Avg_Price",
        "New_Prime_Exclusive_Current_Price",
        "New_Prime_Exclusive_90_Days_Avg_Price",
        "Lightning_Deals_Current_Price",
        "Used_Current_Price",
        "Used_90_Days_Avg_Price",
        "Used_Like_New_Current_Price",
        "Used_Like_New_90_Days_Avg_Price",
        "Used_Very_Good_Current_Price",
        "Used_Very_Good_90_Days_Avg_Price",
        "Used_Good_Current_Price",
        "Used_Good_90_Days_Avg_Price",
        "Used_Acceptable_Current_Price",
        "Used_Acceptable_90_Days_Avg_Price",
        "Warehouse_Deals_Current_Price",
        "Warehouse_Deals_90_Days_Avg_Price",
        "List_Price_Current",
        "List_Price_90_Days_Avg",
        "Rental_Current_Price",
        "Rental_90_Days_Avg_Price",
        "One_Time_Coupon_Absolute",
    ]

    percentage_columns = [
        "Sales_Rank_90_Days_Drop_Percent",
        "Buy_Box_90_Days_Drop_Percent",
        "Buy_Box_90_Days_OOS_Percent",
        "Reviews_Review_Count_90_Days_Drop_Percent",
        "Amazon_90_Days_Drop_Percent",
        "Amazon_90_Days_OOS_Percent",
        "New_90_Days_Drop_Percent",
        "New_90_Days_OOS_Percent",
        "New_3rd_Party_FBA_90_Days_Drop_Percent",
        "New_3rd_Party_FBM_90_Days_Drop_Percent",
        "New_Prime_Exclusive_90_Days_Drop_Percent",
        "Used_90_Days_Drop_Percent",
        "Used_Like_New_90_Days_Drop_Percent",
        "Used_Very_Good_90_Days_Drop_Percent",
        "Used_90_Days_OOS_Percent",
        "Used_Good_90_Days_Drop_Percent",
        "Used_Acceptable_90_Days_Drop_Percent",
        "Warehouse_Deals_90_Days_Drop_Percent",
        "List_Price_90_Days_Drop_Percent",
        "Rental_90_Days_Drop_Percent",
        "Reviews_Review_Count_90_Days_Drop_Percent",
        "Referral_Fee_Percent",
        "One_Time_Coupon_Percentage",
        "Subscribe_and_Save_Coupon_Percentage",
    ]

    integer_columns = [
        "Sales_Rank_Current",
        "Sales_Rank_90_Days_Avg",
        "Sales_Rank_Drops_Last_90_Days",
        "Bought_Past_Month",
        "Reviews_Review_Count",
        "Ratings_Format_Specific",
        "Review_Count_Format_Specific",
        "Buy_Box_Stock",
        "New_Offer_Count_Current",
        "New_Offer_Count_90_Days_Avg",
        "Count_of_Retrieved_Live_Offers_New_FBA",
        "Count_of_Retrieved_Live_Offers_New_FBM",
        "Used_Offer_Count_Current",
        "Used_Offer_Count_90_Days_Avg",
        "Number_of_Items",
        "Number_of_Pages",
        "Package_Dimension_cm3",
        "Package_Weight_g",
        "Package_Quantity",
        "Item_Dimension_cm3",
        "Item_Weight_g",
    ]

    string_columns = [
        "Product_Codes_EAN",
        "Product_Codes_UPC",
    ]

    # Apply cleaning functions to the specified columns
    for col in currency_columns:
        data[format_header(col)] = data[format_header(col)].apply(clean_currency)

    for col in percentage_columns:
        data[format_header(col)] = data[format_header(col)].apply(clean_percentage)

    for col in integer_columns:
        data[format_header(col)] = (
            data[format_header(col)].astype(float).fillna(0).astype(int)
        )

    # for col in string_columns:
    #     data[format_header(col)] = data[format_header(col)].apply(
    #         lambda x: "{:.0f}".format(x)
    #     )

    selected_columns = [
        "locale",
        "image_urls",
        "title",
        "sales_rank_current",
        "reviews_rating",
        "reviews_review_count",
        "ratings_format_specific",
        "review_count_format_specific",
        "last_price_change",
        "buy_box_current_price",
        "buy_box_stock",
        "buy_box_seller",
        "buy_box_is_fba",
        "buy_box_unqualified",
        "amazon_current_price",
        "new_current_price",
        "new_3rd_party_fba_current_price",
        "fba_pickandpack_fee",
        "referral_fee_current_price",
        "new_3rd_party_fbm_current_price",
        "new_prime_exclusive_current_price",
        "lightning_deals_current_price",
        "used_current_price",
        "used_like_new_current_price",
        "used_very_good_current_price",
        "used_good_current_price",
        "used_acceptable_current_price",
        "warehouse_deals_current_price",
        "list_price_current",
        "rental_current_price",
        "new_offer_count_current",
        "count_of_retrieved_live_offers_new_fba",
        "count_of_retrieved_live_offers_new_fbm",
        "used_offer_count_current",
        "categories_root",
        "categories_sub",
        "categories_tree",
        "categories_launchpad",
        "asin",
        "product_codes_ean",
        "product_codes_upc",
        "product_codes_partnumber",
        "parent_asin",
        "variation_asins",
        "freq_bought_together",
        "type",
        "manufacturer",
        "brand",
        "product_group",
        "model",
        "variation_attributes",
        "color",
        "size",
        "edition",
        "format",
        "author",
        "contributors",
        "binding",
        "number_of_items",
        "number_of_pages",
        "publication_date",
        "release_date",
        "languages",
        "package_dimension_cm3",
        "package_weight_g",
        "package_quantity",
        "item_dimension_cm3",
        "item_weight_g",
        "hazardous_materials",
        "is_hazmat",
        "adult_product",
        "trade_in_eligible",
        "prime_eligible",
        "subscribe_and_save",
        "one_time_coupon_absolute",
        "subscribe_and_save_coupon_percentage",
        "sys_run_date",
    ]
    return data[selected_columns]


def insert_to_database(data):
    table = "active_product_keepa_tracking"
    print("insert into table: ", table)

    data.to_sql(table, engine, index=False, if_exists="append", chunksize=50000)


# Replace these with your Keepa username and password
username = "utytra1"
password = "SgN2N.yvY@iR2kg"

connection_string = "postgresql://postgres.sxoqzllwkjfluhskqlfl:5giE*5Y5Uexi3P2@aws-0-us-west-1.pooler.supabase.com:6543/postgres"

engine = sql_create_engine(connection_string)

# display = Display(visible=0, size=(800, 800))
# display.start()

# chromedriver_autoinstaller.install()  # Check if the current version of chromedriver exists

# Create a temporary directory for downloads
with tempfile.TemporaryDirectory() as download_dir:
    # and if it doesn't exist, download it automatically,
    # then add chromedriver to path
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options = [
        # Define window size here
        "--window-size=1920,1080",
        "--ignore-certificate-errors",
        "--headless=new",
        "--disable-gpu",
        "--no-sandbox",
        "--disable-dev-shm-usage",
    ]
    chrome_options.add_experimental_option("prefs", prefs)
    for option in options:
        chrome_options.add_argument(option)

    chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
    chrome_service = Service(os.environ.get("CHROMEDRIVER_PATH"))


if __name__ == "__main__":
    # Extract retailer_ids from the result
    seller_id = "A10RWILGPNWWBU"
    export_file(seller_id)

    print("Get newest file")
    newest_file_path = get_newest_file(download_dir)
    data = transform_data(newest_file_path)
    insert_to_database(data)
