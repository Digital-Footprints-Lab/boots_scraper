import os
import sys
import time
import datetime
import requests
import logging
import re
import glob
from retry import retry
from random import randint
import pandas as pd
from bs4 import BeautifulSoup
from alive_progress import alive_bar
import lxml      # alt to html.parser, with cchardet >> speed up
import cchardet  # character recognition

import targets       # this will be the template in published version
import scraper_meta  # things like user agents, in case we need to rotate


def get_links_from_one_category(category, baseurl) -> pd.Series:

    """Get the full URL for each product in a category

    Args: a category and baseURL from snax_tagets
    Rets: a pandas series of the product URLs"""

    page_number = 1
    product_links = []
    #~ the final section of the category URL
    category_name = category.split("/")[-1]

    # def get_product_list(target, **attrs):
    #     return soup.find_all(target, attrs)

    # def get_product_list(target, **attrs):
    #     # if "css_class" in attrs:
    #     #     css_classes = attrs.pop("css_class")
    #         # attrs["class"] = css_classes
    #     print(f"target: {target}, attrs: {attrs}")
    #     return soup.find_all(target=target, attrs=attrs)

    # get_product_list("a", "class", "product_name_link product_view_gtm")
    # get_product_list(target="a", attrs={"class" : "item__productName ClickSearchResultEvent_Class"})

    with alive_bar(0, f"Acquiring product links for {category_name}") as bar:
        while True:
            #~ pull down the category page
            category_page = baseurl + category + targets.page_string + str(page_number)
            target = requests.get(category_page, headers=scraper_meta.user_agent).text
            #~ init BS object
            soup = BeautifulSoup(target, "lxml")
            #~ retrieve the link text element for all products on page
            # product_list = get_product_list(target="a", attrs={"class" : "item__productName ClickSearchResultEvent_Class"})
            # product_list = get_product_list(target="a", attrs="item__productName ClickSearchResultEvent_Class")
            # print(product_list)
            # get_product_list(target="a", attrs=[("class", "item__productName ClickSearchResultEvent_Class"), ])
            #! SOLVE THIS WITH DECORATOR
            #! boots
            product_list = soup.find_all("a", {"class": "product_name_link product_view_gtm"})
            #! sd
            # product_list = soup.find_all("a", {"class": "item__productName ClickSearchResultEvent_Class"})
            #~ incrementing to an empty product page means we are done here
            if len(product_list) == 0:
                print(f"OK, {len(product_links)} {category_name} links retrieved [{page_number - 1} pages]")
                break
            #~ add to a list of the href URLs
            for product in product_list:
                link = product.get("href")
                product_links.append(link)
                bar() #~ increment progress bar
            #~ increment pagination
            page_number += 1

    #~ turn the list into a series and return
    linx = pd.Series(product_links, dtype=object)
    return linx


def make_dataframe_of_links_from_all_categories(start_time,
                                                categories) -> pd.DataFrame:

    """
    Rets: DF with first column as product URLs
    """

    all_links = pd.Series(dtype=str)
    print("\n" + f".oO Finding links for {len(categories)} product categories")

    for category in categories:
        product_links = get_links_from_one_category(category, targets.baseurl)
        all_links = all_links.append(product_links, ignore_index=True)

    #~ add some links that don't appear in categories
    # uncat = pd.Series(targets.uncategorised)
    # all_links = all_links.append(uncat, ignore_index=True)
    #~ clean dups and re-index
    all_links = all_links.drop_duplicates().reset_index(drop=True)
    #~ send series to DF
    all_links = all_links.to_frame()
    #~ label column one
    all_links.columns = ["product_link"]

    return all_links


def populate_links_df_with_extracted_fields(dataframe,
                                            fields_to_extract,
                                            start_time) -> pd.DataFrame:

    """Takes a dataframe where column 0 = URLs, generated by
    the get links function. Puts the contents of each
    field of interest into a dataframe.

    Args: dataframe column 0 = a URL,
          list of fields (a list of lists)
    Rets: populated dataframe"""

    total_snax = len(fields_to_extract) * dataframe.shape[0]
    regex = re.compile(r"[\n\r\t]+") #~ whitespace cleaner
    print("\n" + f".oO Requesting {total_snax} product details")

    with alive_bar(total_snax,
                   f"""Acquiring {len(fields_to_extract)}
                   fields for {dataframe.shape[0]} products""") as bar:

        for index in range(dataframe.shape[0]):

            @retry(ConnectionResetError, tries=3, delay=10, backoff=10)
            def get_target_page(index):
                #~ pull down the full product page
                return requests.get(dataframe.at[index, "product_link"], headers=scraper_meta.user_agent).text

            try:
                target = get_target_page(index)
            except ConnectionResetError: #~ try giving the server a break
                print("\n" + f".oO Issue getting page: {e}, sleeping for 15 minutes.")
                time.sleep(900)
                continue

            #~ init BSoup object
            soup = BeautifulSoup(target, "lxml")

            for field in fields_to_extract:

                field_value = ""
                try:
                    if field[0] == "multi": #~ nested aquire from "Product details" div
                        try:
                            full_div = soup.find_all(field[1], attrs={field[2]: field[3]})
                            for i in full_div:
                                field_value += i.text.strip() + " "
                                field_value = regex.sub(" ", field_value)
                        except Exception as e:
                            print(f"Field \"{field[3]}\" not found", e)
                            continue
                    else: #~ just get the target field
                        field_value = soup.find(field[1], attrs={field[2]: field[3]}).get_text(strip=True)
                except AttributeError:
                    print(f"Field \"{field[3]}\" not found")
                    continue

                dataframe.loc[index, field[3]] = field_value
                bar()

                time.sleep(randint(1, 3)) #~ relax a little

    return dataframe


def select_long_description_field(dataframe) -> pd.DataFrame:

    """Columns named 13 and 14 and called that because
    those are the nested div names on the boots website;
    it is unclear which will be the true field, so both are acquired.
    We need to take the longer field, the shorter always being PDF or
    ordering details, or other crap that we don't want."""

    with alive_bar(dataframe.shape[0],
                   f""".oO IDing long_description field for {dataframe.shape[0]} products""") as bar:

        for index in range(dataframe.shape[0]):

            #~ compare fields
            longer_field = max([dataframe.iloc[index]["13"]], [dataframe.iloc[index]["14"]])
            dataframe.loc[index, "long_description"] = longer_field
            bar()

    #~ remove candidate fields
    dataframe = dataframe.drop(["13", "14"], axis=1)

    return dataframe


def main():

    try:

        start_time = datetime.datetime.now().replace(microsecond=0).isoformat()
        start_counter = time.perf_counter()

        print(f"\n.oO Starting snax2 @ {start_time} - target base URL is {targets.baseurl}")

        snax = make_dataframe_of_links_from_all_categories(start_time,
                                                           targets.categories)
        if snax.empty:
            print(f"\n.oO No links retrieved... Stopping.")
            sys.exit(0)

        snax.to_csv("output/linx_" + start_time + ".csv") #~ save links

        snax = populate_links_df_with_extracted_fields(snax,
                                                       targets.fields_to_extract,
                                                       start_time)
        snax = select_long_description_field(snax)
        snax.to_csv("output/snax_" + start_time + ".csv") #~ save full output


        end_time = datetime.datetime.now().replace(microsecond=0).isoformat()
        end_counter = time.perf_counter()
        elapsed = datetime.timedelta(seconds=(end_counter - start_counter))

        print(f".oO OK, finished scrape @ {end_time}, taking {elapsed}")

    except KeyboardInterrupt:
        print("\n.oO OK, dropping. That run was not saved.")
        sys.exit(0)


if __name__ == "__main__":

    main()

