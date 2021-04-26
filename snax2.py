import sys
import time
import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
from alive_progress import alive_bar
import re
import snax_targets2 # this will be the template in published version
import lxml      # alt to html.parser, with cchardet >> speed up
import cchardet  # character recognition

#! todo - sleepy, argparse, category parse,

def get_links_from_category(category, baseurl):

    """Get the full URL for each product in a category

    Args: a category and baseURL from snax_tagets
    Rets: a pandas series of the product URLs"""

    page_number = 1
    product_links = []
    page_string = "?pageNo=" #~ the URL page counter
    #~ the final section of the category URL - often inconsistent
    category_name = category.split("/")[-1]

    with alive_bar(0, f"Acquiring product links for {category_name}") as bar:
        while True:
            #~ pull down the webpage
            target = requests.get(baseurl + category + page_string + str(page_number)).text
            #~ init BS object
            # soup = BeautifulSoup(target, "html.parser")
            soup = BeautifulSoup(target, "lxml")
            #~ retrieve the link text element for all products on page
            product_list = soup.find_all("a", {"class": "product_name_link product_view_gtm"})
            #~ incrementing to an empty product page means we are done here
            if len(product_list) == 0:
                print(f"OK, {len(product_links)} product links retrieved [{page_number - 1} pages]")
                break
            #~ add to a list of the href URLs
            for product in product_list:
                link = product.get("href")
                product_links.append(link)
                bar() #~ increment progress bar
            #~ increment pagination
            page_number += 1
            #! sleep for a random length taken from the sleepy arg?
    #~ turn the list into a series and return
    return pd.Series(product_links)


def extract_fields(dataframe, fields_to_extract):

    """Takes a dataframe where column 0 = URLs, generated by
    the get links function. Puts the contents of each
    field of interest into a dataframe.

    Args: dataframe column 0 = a URL,
          list of fields (a list of lists)
    Rets: populated dataframe"""

    total_snax = len(fields_to_extract) * dataframe.shape[0]
    with alive_bar(total_snax,
                   f"""Acquiring {len(fields_to_extract)}
                   fields for {dataframe.shape[0]} products""") as bar:
        for index in range(dataframe.shape[0]):
            #~ pull down the full product page
            target = requests.get(dataframe.at[index, "product_link"]).text
            #~ init BSoup object
            soup = BeautifulSoup(target, "html.parser")
            for field in fields_to_extract:
                #~ use field identifiers to get values
                # field_value = soup.find(field[0], {field[1]: field[2]}).get_text()
                try:
                    if field[3] == "nested":
                        #~ this is tailored for the final field with "active ingredients" #! fix this!
                        try:
                            field_value = soup.find_all(field[0], attrs={field[1]: field[2]})[-1].get_text(strip=True) #find('h3', 'id'="product_active_ingredients").find('p').get_text(strip=True)
                        except IndexError:
                            print("Ingredients field not found")
                            continue
                    else:
                        field_value = soup.find(field[0], attrs={field[1]: field[2]}).get_text(strip=True) #'div', attrs={'class':'category5'}):
                except AttributeError:
                    print("Field not found")
                    continue
                # field_value = soup.find(field[0], class_=field[1]).get_text().strip()
                # field_value = soup.find(field[0]).get_text()
                dataframe.loc[index, field[2]] = field_value
                bar()

    return dataframe


def get_product_links():
    all_links = pd.Series(dtype=str)
    for category in snax_targets2.categories:
        product_links = get_links_from_category(category, snax_targets2.baseurl)
        all_links = all_links.append(product_links, ignore_index=True)
        #! sleep for a random length taken from the sleepy arg
    all_links = all_links.drop_duplicates()
    all_links = all_links.to_frame()
    all_links.columns =["product_link"]
    return all_links


def main():
    #~ start dataframe with column of all product links
    snax = get_product_links()
    #! this will be moved to targets / config
    fields_to_extract = [["div", "class", "productid", "no_nest"],
                         ["div", "id", "PDP_productPrice", "no_nest"],
                         ["div", "class", "details", "no_nest"],
                         ["div", "class", "product_long_description_subsection", "nested"],]

    #~ using links df, build new columns for each field
    try:
        snax = extract_fields(snax, fields_to_extract)
    except KeyboardInterrupt:
        print(snax)
        sys.exit(0)
    print(snax)
    snax.to_csv("output/" + datetime.datetime.now().replace(microsecond=0).isoformat() + ".csv")

if __name__ == "__main__":
    main()

