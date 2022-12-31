from requests import Session
from lxml import html
import re
import json
import pandas as pd
import csv
import requests
from bs4 import BeautifulSoup as bs
f = open("charcoalclothing_imgs_coats_jackets.csv","w",encoding="utf-8")
writer = csv.writer(f)
writer.writerow(["Retailer","path",'search_term',"prd_id","images","prd_url"])
s = Session()
s.headers["User-Agent"] = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:97.0) Gecko/20100101 Firefox/97.0"

def pagination_page(url,query):
    # try:
    m_url = url.format(1,query)
    r = s.get(m_url)
    pages = re.findall('\(\{"total_product":(.*?),"total_collection":',r.text)
    if pages:
        pages = int(pages[0])/25
    else:
        pages = 1
    return pages


def crawling_detail_page(url):
    m_url = "https://www.charcoalclothing.com.au/collections/all/products/{}".format(url)
    r = s.get(m_url)
    soup = bs(r.content,"html.parser")
    tree = html.fromstring(r.text)
    l = []
    d = dict()
    for img in tree.xpath("//a[@class='text-link product-single__thumbnail product-single__thumbnail--product-template']//@data-zoom"):
        l.append("https:{}".format(img))
    product_id = re.findall('\,\sproduct_id\s:\s(.*?)\,\sname\s:',r.text)
    find_sku = re.findall('\,\ssku\s:\s"(.*?)"\,\sbrand\s:',r.text)
    if product_id:
        sku = product_id[0]
    else:
        sku = find_sku[0]

    d["sku"] = sku
    d["images"] = '|'.join(l)
    return d




def crawl_link_imgs(query,row):
    # try:
    path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
    url = "https://services.mybcapps.com/bc-sf-filter/search?t=1646489150734&page={}&q={}&shop=charcoal-online.myshopify.com&limit=25&sort=relevance&display=grid&collection_scope=0&product_available=false&variant_available=false&build_filter_tree=false&check_cache=false&callback=BCSfFilterCallback&event_type=page"
    print("Searhc term :- {}".format(query))
    pages = pagination_page(url,query)
    print("Total pages for Search term :- {}".format(int(pages)+1))
    for page in range(1,int(pages+1)):
        print("Currant page :- {}".format(page))
        r = s.get(url.format(page,query))
        products = re.findall('"handle":"(.*?)","compare_at_price_min":',r.text)
        for prod in products:
            d = dict()
            d = crawling_detail_page(prod)
            d["Retailer"] = "charcoalclothing"
            d["path"] = path
            d["search_term"] = query
            d["prd_id"] = d["sku"]
            d["images"] = d["images"]
            d["prd_url"] = "https://www.charcoalclothing.com.au/collections/all/products/{}".format(prod)
            print(d)
            writer.writerow([
                "charcoalclothing",
                path,
                query,
                d["sku"],
                d["images"],
                "https://www.charcoalclothing.com.au/collections/all/products/{}".format(prod)
            ])                
    # except:
    #     pass


def crawl_query(row):
    querys = row['Query'].split(',')
    for query in querys:
        crawl_link_imgs(query,row)

df = pd.read_excel('Query_Coats_and_Jackets.xlsx')
for i in range(len(df)):
    row = df.iloc[i].to_dict()
    crawl_query(row)

