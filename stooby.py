import requests
from bs4 import BeautifulSoup
import pandas as pd
from alive_progress import alive_bar
import time
import snax_targets

def get_links_from_category(category, baseurl):

    """Get the full URL for each product in a category

    Takes: a category and baseURL from snax_tagets
    Returns: a pandas series of the product URLs
    """

    page_number = 1
    product_links = []
    page_string = "?pageNo="
    category_name = category.split("/")[-1]

    print(f"Acquiring product links for {category_name}")
    with alive_bar() as bar:
        while True:

            target = requests.get(baseurl + category + page_string + str(page_number)).text

            soup = BeautifulSoup(target, 'html.parser')

            #~ retrieve the link text element for all products on page
            product_list = soup.find_all("a", {"class": "product_name_link product_view_gtm"})

            if len(product_list) == 0:
                print(f"OK, {len(product_links)} product links retrieved from {page_number - 1} pages")
                break

            for product in product_list:
                link = product.get("href")
                product_links.append(link)

            page_number += 1
            bar()

    return pd.Series(product_links)


all_links = pd.Series(dtype=str)

for category in snax_targets.categories:
    product_links = get_links_from_category(category, snax_targets.baseurl)
    all_links = all_links.append(product_links, ignore_index=True)

all_links = all_links.drop_duplicates()

all_links.to_csv("links.csv")
all_links.to_json("links.json")

