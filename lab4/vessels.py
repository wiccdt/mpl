from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import time
import random
import sys
import os

sys.stdout.reconfigure(line_buffering=True)

def process_link(url):
    global success_count
    try:
        #time.sleep(random.uniform(3, 8))
        response = session.get(url, timeout=60)
        if response.status_code != 200:
            print("failed in main page - " + str(response.status_code), flush=True)
            failed_links.append((url, response.status_code))
            return False
        soup = BeautifulSoup(response.text, "html.parser")
        if process_main_page(soup):
            success_count += 1
            return True
        else:
            return False
        #time.sleep(random.uniform(2, 5))
    except Exception as e:
        failed_links.append((url, response.status_code))
        print(f"ИСКЛЮЧЕНИЕ: {type(e).__name__}: {e}", flush=True)
        return False

def process_main_page(soup):
    no_results = soup.find("div", {"class": "no-result-row"})
    if no_results:
        return True
    vessel_num_str = soup.find("div", {"class": "pagination-totals"}).text
    vessel_num = int(re.search(r"(\d+)", vessel_num_str).group(1))
    if vessel_num == 1:
        name = soup.find("div", {"class": "slna"}).text
        vessel_type = soup.find("div", {"class": "slty"}).text
        ship_link = soup.find("a", {"class": "ship-link"})
        match = re.search(r"/details/(\d+)", ship_link.get("href"))
        imo = match.group(1)
        vessels["Name"].append(name)
        vessels["IMO"].append(imo)
        vessels["Type"].append(vessel_type)
        url = f"https://www.vesselfinder.com/ru/vessels/details/{imo}"
        response = session.get(url, timeout=60)
        if response.status_code != 200:
            print("failed in details page - " + str(response.status_code), flush=True)
            return False
        soup = BeautifulSoup(response.text, "html.parser")
        process_details_page(soup)
    return True

def process_details_page(soup):
    script_tags = soup.find_all("script")
    for script in script_tags:
        if script.string:
            match = re.search(r"var MMSI=(\d+)", script.string)
            if match:
                mmsi = match.group(1)
            else:
                continue
            vessels["MMSI"].append(mmsi)
            break

vessels = {"Name": [], "IMO": [], "MMSI": [], "Type": []}
failed_links = []
success_count = 0
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
})
home_response = session.get("https://www.vesselfinder.com/", timeout=10)
time.sleep(2)
links = pd.read_excel("Links.xlsx", sheet_name = 0)
i = 1
for link in links["Ссылка"]:
    print(str(i) + (" success" if process_link(link) else " failed"), flush=True)
    i += 1
vessel_df = pd.DataFrame(vessels, columns = ["Name", "IMO", "MMSI", "Type"])
print(vessel_df, flush=True)
vessel_df.to_excel('output_vessels.xlsx', index=False)
print(len(failed_links), flush=True)
print(failed_links, flush=True)