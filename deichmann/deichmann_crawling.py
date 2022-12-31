from requests import Session
from lxml import html
import re
import json
import pandas as pd
s = Session()

s.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'
all_data = []

def crawl_detail(url):
    try:
        r = s.get(url)
        print("Product url :- {}".format(url))
        tree= html.fromstring(r.text)
        all_imgs = []
        for img in tree.xpath("//img[@loading='lazy']//@src"):
            if "p_mosaic_pd" in img:
                all_imgs.append(img)
        sku = re.findall(',"sku":"(.*?)",',r.text)
        d = {"sku":sku[0], "images":'|'.join(all_imgs)}
        return d
    except:
        pass


def pagination_pages(response):
    try:
        tree = html.fromstring(response)
        total_pages = ''.join(tree.xpath("//h1[@class='message-pipe']//following-sibling::p//text()")).strip().replace(" ","").replace("results","")
        pages = int(int(total_pages)/48)+1
    except:
        pages = 1
    return pages


def crawl_link(row,query):
    try:
        path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
        quary_url = "https://www.deichmann.com/en-gb/search?text={}&page={}".format(query,1)
        r = s.get(quary_url)
        total_pages = pagination_pages(r.text)
        print("Total pages :- {}".format(total_pages))
        for page in range(1,total_pages):
            quary_url = "https://www.deichmann.com/en-gb/search?text={}&page={}"
            response  = s.get(quary_url.format(query,page))
            print("Landing page :- {}-------------******------------".format(response.url))
            tree = html.fromstring(response.text)
            products = tree.xpath("//article//a//@href")
            for product in products:
                output = dict()
                d = crawl_detail("https://www.deichmann.com{}".format(product))
                output["Retailer"] = "deichmann"
                output["path"] = path
                output["search_term"] = query
                output["prd_id"] = d["sku"]
                output["images"] = d["images"]
                output["url"] = "https://www.deichmann.com{}".format(product)
                print(output)
                all_data.append(output)
    except:
        pass


def crawl_query(row):
    querys = row['Query'].split(',')
    for query in querys:
        crawl_link(row,query)

df = pd.read_excel('QueryList_v1.xlsx.xlsx')
for i in range(len(df)):
    row = df.iloc[i].to_dict()
    crawl_query(row)

df = pd.DataFrame(all_data)
df.to_csv("deichmann_imgs.csv",sep=",",index=False)