import csv
import json
import os
import random
import string
import urllib.parse
from datetime import datetime
import sqlite3

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

import requests
from bs4 import BeautifulSoup

from threading import Lock
file_lock = Lock()

headers = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
}

def send_email(subject, body, to_email, attachment_file):
    # Email configuration
    sender_email = 'avsender12@gmail.com'  # Replace with your Gmail address
    sender_password = 'gwymmehkpmbjxyfs'  # Replace with your Gmail password or an app-specific password
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587  # Port for TLS

    # Create the email message
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = to_email
    message['Subject'] = subject

    # Add the email body

    # check if attachment file exists
    if os.path.isfile(attachment_file):
        message.attach(MIMEText(body, 'plain'))
        # Attach the CSV file
        with open(attachment_file, 'rb') as file:
            attachment = MIMEApplication(file.read(), _subtype="csv")
            attachment.add_header('Content-Disposition', 'attachment', filename=attachment_file)
            message.attach(attachment)
    else:
        print("attachment file not found")
        body += "\n\nAttachment file not found. Most likely there is no new data for this zip code."
        message.attach(MIMEText(body, 'plain'))

    print(body)

    try:
        # Connect to the SMTP server
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()

        # Login to your Gmail account
        server.login(sender_email, sender_password)

        # Send the email
        server.sendmail(sender_email, to_email, message.as_string())

        # Close the SMTP server connection
        server.quit()

        print("Email sent successfully")
    except Exception as e:
        print(f"Email sending failed: {e}")


def create_visited_urls_table(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS visited_urls (
            url TEXT PRIMARY KEY
        )
        """
    )
    conn.commit()


def load_visited_urls(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM visited_urls")
    return set(row[0] for row in cursor.fetchall())


def insert_visited_url(conn, url):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO visited_urls (url) VALUES (?)", (url,))
    conn.commit()


def get_region_sugesstion(zip_code):
    params = {
        'q': zip_code,
        'abKey': 'ff015103-447b-4bf7-af0c-ed16eb20e49d',
        'clientId': 'homepage-render',
    }

    response = requests.get('https://www.zillowstatic.com/autocomplete/v3/suggestions', params=params, headers=headers)

    results = response.json()["results"]

    for result in results:
        if result["display"] == zip_code:
            return result["metaData"]["regionId"]


rand_str = ""
filename = ""


def save_to_csv(data, zip_code):

    fieldnames = [
        "street_address", "price", "rent_by_price", "estimated_rent",
        "monthly_payment", "profit_per_month", "profit_per_year", "cap_rate",
        "cash_on_cash_return", "gross_yield", "beds", "baths", "area", "zestimate", "detail_url", "page"
    ]

    file_exists = os.path.isfile(filename)

    with open(filename, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        writer.writerow(data)


def load_detail_page(url):
    print(url)
    response = requests.get(url, headers=headers)

    soup = BeautifulSoup(response.text, 'html.parser')
    emc_block = soup.find('div', {'class': 'klZWLi'})

    try:
        estimated_monthly_cost = emc_block.find('span', {'class': 'Text-c11n-8-84-3__sc-aiai24-0'}).text
        estimated_monthly_cost = estimated_monthly_cost.split('$')[1].split('/')[0].replace(',', '')
    except AttributeError:
        estimated_monthly_cost = ""

    return estimated_monthly_cost


def create_map_bounds(zip_code, region_id):
    params = {
        "usersSearchTerm": zip_code,
        "regionSelection": [{"regionId": region_id, "regionType": 7}],
    }

    wants = {"cat1": ["listResults", "mapResults"], "cat2": ["total"]}

    response = requests.get(
        f'https://www.zillow.com/search/GetSearchPageState.htm?searchQueryState={urllib.parse.quote(str(params))}&wants={urllib.parse.quote(str(wants))}',
        headers=headers,
    )

    print("map bounds response:")
    print(response.status_code)
    print(response.text)

    map_bounds = response.json()["regionState"]["regionBounds"]
    return map_bounds


def get_pricing_filter(p_min, p_max):
    if p_min is not None and p_max is not None:
        p_dict = {"min": p_min, "max": p_max}
    elif p_min is not None:
        p_dict = {"min": p_min}
    elif p_max is not None:
        p_dict = {"max": p_max}
    else:
        p_dict = {}

    return p_dict


def parse_data(zip_code, region_id, page=1, for_rent=True, is_all_homes=True, price_min=None,
               price_max=None,
               monthly_payment_min=None,
               monthly_payment_max=None, monthly_cost_payment_min=None, monthly_cost_payment_max=None,
               is_coming_soon=False,
               is_auction=False, is_new_construction=False, list_price_active=False, is_townhouse=True,
               is_multi_family=True, is_condo=True, is_lot_land=True, is_apartment=True, is_manufactured=True,
               is_apartment_or_condo=True, max_hoa=None, beds=None, baths=None):
    is_for_sale_by_agent, is_for_sale_by_owner, is_for_sale_foreclosure = True, True, True
    if for_rent:
        is_for_sale_by_agent, is_for_sale_by_owner, is_for_sale_foreclosure = False, False, False

    price = get_pricing_filter(price_min, price_max)
    monthly_payment = get_pricing_filter(monthly_payment_min, monthly_payment_max)
    monthly_cost_payment = get_pricing_filter(monthly_cost_payment_min, monthly_cost_payment_max)

    if beds is not None:
        beds = {"min": beds}
    else:
        beds = {}

    if baths is not None:
        baths = {"min": baths}
    else:
        baths = {}

    if max_hoa is not None:
        hoa = {"max": max_hoa}
    else:
        hoa = {}

    params = {
        "pagination": {"currentPage": page},
        "usersSearchTerm": zip_code,
        "mapBounds": create_map_bounds(zip_code, region_id),
        "regionSelection": [{"regionId": region_id, "regionType": 7}],
        "isMapVisible": False,
        "filterState": {
            "sortSelection": {"value": "days"},
            "isAllHomes": {"value": is_all_homes},
            "price": price,
            "monthlyPayment": monthly_payment,
            "monthlyCostPayment": monthly_cost_payment,
            "isForRent": {"value": for_rent},
            "isForSaleByAgent": {"value": is_for_sale_by_agent},
            "isForSaleByOwner": {"value": is_for_sale_by_owner},
            "isForSaleForeclosure": {"value": is_for_sale_foreclosure},
            "isNewConstruction": {"value": is_new_construction},
            "isComingSoon": {"value": is_coming_soon},
            "isAuction": {"value": is_auction},
            "isTownhouse": {"value": is_townhouse},
            "isMultiFamily": {"value": is_multi_family},
            "isCondo": {"value": is_condo},
            "isLotLand": {"value": is_lot_land},
            "isApartment": {"value": is_apartment},
            "isManufactured": {"value": is_manufactured},
            "isApartmentOrCondo": {"value": is_apartment_or_condo},
            "hoa": hoa,
            "beds": beds,
            "baths": baths
        },
        "isListVisible": True,
        "mapZoom": 14,
        "listPriceActive": list_price_active
    }

    wants = {"cat1": ["listResults"], "cat2": ["total"]}

    response = requests.get(
        f'https://www.zillow.com/search/GetSearchPageState.htm?searchQueryState={urllib.parse.quote(str(params))}&wants={urllib.parse.quote(str(wants))}&requestId=38',
        headers=headers,
    )

    data = response.json()

    # with open("../res.json", 'w') as f:
    #     f.write(json.dumps(data))

    # parse data
    try:
        properties = data["cat1"]["searchResults"]["listResults"]
    except KeyError:
        properties = data["cat1"]["searchResults"]["mapResults"]

    if len(properties) <= 0:
        try:
            properties = data["cat1"]["searchResults"]["relaxedResults"]
        except KeyError:
            properties = []

    conn = sqlite3.connect("output/visited_urls.db")
    create_visited_urls_table(conn)

    visited_urls = load_visited_urls(conn)

    for property in properties:
        # print(property)
        try:
            detailUrl = property["detailUrl"]
        except KeyError:
            detailUrl = ""

        if "zillow.com" not in detailUrl:
            detailUrl = "https://www.zillow.com" + detailUrl

        if detailUrl:
            if detailUrl in visited_urls:
                continue

            visited_urls.add(detailUrl)
            # ... process the property and save data ...

            insert_visited_url(conn, detailUrl)

        try:
            street_address = property["hdpData"]["homeInfo"]["streetAddress"]
        except KeyError:
            street_address = ""

        try:
            price = property["price"].replace(",", "").replace("$", "").replace("+", "").split("/")[0]
        except KeyError:
            price = ""

        try:
            estimated_rent = property["hdpData"]["homeInfo"]["rentZestimate"]
        except KeyError:
            estimated_rent = ""

        if estimated_rent and price:
            rent_by_price = (float(estimated_rent) / float(price)) * 100
            gross_yield = ((float(estimated_rent) * 12) / float(price)) * 100
        else:
            rent_by_price = ""
            gross_yield = ""

        try:
            zestimate = property["hdpData"]["homeInfo"]["zestimate"]
        except KeyError:
            zestimate = ""

        try:
            beds = property["hdpData"]["homeInfo"]["bedrooms"]
        except KeyError:
            beds = ""

        try:
            baths = property["hdpData"]["homeInfo"]["bathrooms"]
        except KeyError:
            baths = ""

        try:
            area = property["hdpData"]["homeInfo"]["city"]
        except KeyError:
            area = ""

        monthly_payment = ""
        if detailUrl:
            monthly_payment = load_detail_page(detailUrl)

        if monthly_payment and estimated_rent:
            profit_per_month = float(estimated_rent) - float(monthly_payment)
            profit_per_year = profit_per_month * 12

        else:
            profit_per_month = ""
            profit_per_year = ""

        if profit_per_year and price:
            cap_rate = (profit_per_year / float(price)) * 100
            cash_on_cash_return = (profit_per_year / (float(price) * 0.25)) * 100
        else:
            cap_rate = ""
            cash_on_cash_return = ""

        print("=====================================")

        output_data = {
            "street_address": street_address,
            "price": price,
            "rent_by_price": rent_by_price,
            "estimated_rent": estimated_rent,
            "monthly_payment": monthly_payment,
            "profit_per_month": profit_per_month,
            "profit_per_year": profit_per_year,
            "cap_rate": cap_rate,
            "cash_on_cash_return": cash_on_cash_return,
            "gross_yield": gross_yield,
            "beds": beds,
            "baths": baths,
            "area": area,
            "zestimate": zestimate,
            "detail_url": detailUrl,
            "page": page,
        }

        print(output_data)

        save_to_csv(output_data, zip_code)

        # print("=====================================")

        # break

    total_pages = data["cat1"]["searchList"]["totalPages"]

    print("total_pages:", total_pages)

    for i in range(page+1, total_pages + 1):
        if i >= 2:
            parse_data(zip_code, region_id, page=i)
            print("page:", i)

    conn.close()


def start_parse(zipcode, for_rent, is_all_homes,
                price_min, price_max, monthly_payment_min,
                monthly_payment_max,
                monthly_cost_payment_min,
                monthly_cost_payment_max, is_coming_soon,
                is_auction, is_new_construction,
                list_price_active, is_townhouse,
                is_multi_family, is_condo, is_lot_land,
                is_apartment, is_manufactured,
                is_apartment_or_condo, max_hoa, beds, baths, email):

    with file_lock:
        global rand_str, filename
        # print all params
        print("Start scraping for zipcode:", zipcode)
        # print("zipcode:", zipcode)
        # print("for_rent:", for_rent)
        # print("is_all_homes:", is_all_homes)
        # print("price_min:", price_min)
        # print("price_max:", price_max)
        # print("monthly_payment_min:", monthly_payment_min)
        # print("monthly_payment_max:", monthly_payment_max)
        # print("monthly_cost_payment_min:", monthly_cost_payment_min)
        # print("monthly_cost_payment_max:", monthly_cost_payment_max)
        # print("is_coming_soon:", is_coming_soon)
        # print("is_auction:", is_auction)
        # print("is_new_construction:", is_new_construction)
        # print("list_price_active:", list_price_active)
        # print("is_townhouse:", is_townhouse)
        # print("is_multi_family:", is_multi_family)
        # print("is_condo:", is_condo)
        # print("is_lot_land:", is_lot_land)
        # print("is_apartment:", is_apartment)
        # print("is_manufactured:", is_manufactured)
        # print("is_apartment_or_condo:", is_apartment_or_condo)
        # print("max_hoa:", max_hoa)
        # print("beds:", beds)
        # print("baths:", baths)


        # generate random string
        rand_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

        # create_file_name_with_datetime_and_zip_code
        current_datetime = datetime.now().strftime("%Y%m%d")
        filename = f"output/zillow_{zipcode}_{current_datetime}_{rand_str}.csv"

        regionid = str(get_region_sugesstion(zipcode))

        print("region_id:", regionid)
        if regionid:
            parse_data(zipcode, regionid, for_rent=for_rent, is_all_homes=is_all_homes,
                       price_min=price_min, price_max=price_max, monthly_payment_min=monthly_payment_min,
                       monthly_payment_max=monthly_payment_max,
                       monthly_cost_payment_min=monthly_cost_payment_min,
                       monthly_cost_payment_max=monthly_cost_payment_max, is_coming_soon=is_coming_soon,
                       is_auction=is_auction, is_new_construction=is_new_construction,
                       list_price_active=list_price_active, is_townhouse=is_townhouse,
                       is_multi_family=is_multi_family, is_condo=is_condo, is_lot_land=is_lot_land,
                       is_apartment=is_apartment, is_manufactured=is_manufactured,
                       is_apartment_or_condo=is_apartment_or_condo, max_hoa=max_hoa, beds=beds, baths=baths)

        # send email
        send_email(f"zillow_{zipcode}_{current_datetime}_{rand_str} data", f"Got data for zillow_{zipcode}_{current_datetime}_{rand_str}", email, filename)
        print("Scraping finished successfully for zipcode:", zipcode)

# if __name__ == "__main__":
#     zipcode = '84003'  # input("Enter zipcode: ")
#     print("zip_code:", zipcode)
#     output_filename = "../zillow.csv"
#     regionid = str(get_region_sugesstion(zipcode))
#
#     print("region_id:", regionid)
#     if regionid:
#         parse_data(zipcode, regionid, for_rent=False, is_all_homes=True, price_min=None,
#                    price_max=None,
#                    monthly_payment_min=None,
#                    monthly_payment_max=None, monthly_cost_payment_min=None, monthly_cost_payment_max=None,
#                    is_coming_soon=False,
#                    is_auction=False, is_new_construction=False, list_price_active=False, is_townhouse=True,
#                    is_multi_family=True, is_condo=True, is_lot_land=True, is_apartment=True, is_manufactured=True,
#                    is_apartment_or_condo=True, max_hoa=None, beds=None, baths=None)
