from requests import Session
from lxml import html
import re
import json
import pandas as pd
import csv
import requests
from bs4 import BeautifulSoup as bs
f = open("prettylittlething_imgs_coats_jackets.csv","w",encoding="utf-8")
writer = csv.writer(f)
writer.writerow(["Retailer","path",'search_term',"prd_id","images","prd_url"])
s = Session()
s.headers["User-Agent"] = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:97.0) Gecko/20100101 Firefox/97.0"

def pagination_page(url,query):
    # try:
    m_url = url.format(query,1)
    r = s.get(m_url)
    js = r.json()
    pages = js.get("number_of_items")
    pages = int((pages/48)+1)
    return pages
    # except:
    #     pass


def crawling_detail_page(url):
    r = s.get(url)
    soup = bs(r.content,"html.parser")
    tree = html.fromstring(r.text)
    l = []
    d = dict()
    for img in tree.xpath("//div[@class='slide gallery-thumbnail']//img//@src"):
        l.append(img.replace("width=60","width=1024"))
    sku = ''.join(tree.xpath("//span[@class='sku-pos']//text()"))
    d["sku"] = sku
    d["images"] = '|'.join(l)
    return d




def crawl_link_imgs(query,row):
    # try:
    path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
    url = "https://www.prettylittlething.us/merchandiser/request/search?q={}&page={}&ajax=1"
    print("Searhc term :- {}".format(query))
    pages = pagination_page(url,query)
    print("Total pages for Search term :- {}".format(pages))
    for page in range(1,pages):
        r = s.get(url.format(query,page))
        print(r.url)
        js = r.json()
        data = js.get("lister")
        # tree = html.fromstring(r.text)
        soup = bs(data,"html.parser")
        for prod in soup.find_all("a","product-url js-click-product js-category-product__link"):
            d = dict()
            d = crawling_detail_page(prod.get("href"))
            d["Retailer"] = "prettylittlething"
            d["path"] = path
            d["search_term"] = query
            d["prd_id"] = d["sku"]
            d["images"] = d["images"]
            d["prd_url"] = prod.get("href")
            print(d)
            writer.writerow([
                "prettylittlething",
                path,
                query,
                d["sku"],
                d["images"],
                prod.get("href")
            ])                
    # except:
    #     pass


def crawl_query(row):
    querys = row['Query'].split(',')
    for query in querys:
        crawl_link_imgs(query,row)

df = pd.read_excel('Query_Coats_and_Jackets.xlsx')
for i in range(len(df)):
    print("---------------")
    row = df.iloc[i].to_dict()
    crawl_query(row)

f.close()
