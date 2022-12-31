from requests import Session
from lxml import html
import re
import json
import pandas as pd
import csv
from tqdm import tqdm

s = Session()
s.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'

all_data = []

output = open('shoesandsox_imgs.csv','w')
o_row = csv.writer(output)
o_row.writerow(['Retailer','path','search_term','prd_id','images','url']) 


def crawl_detail(url):
    try:
        r = s.get(url)
        tree = html.fromstring(r.text)
        prid = "".join(tree.xpath("//div[@class='sns-product-desc uk-container uk-padding-small uk-padding-remove-top uk-padding-remove-bottom productView']/@data-fb-product-sku"))
        imgs = "|".join(tree.xpath("//div[@class='uk-flex uk-flex-top bc-slider-flex']//img[@class='uk-width-1-1 uk-background-cover']//@src"))
        return {"imgs":imgs,"prd_id":prid}
    except:
        return ""


def crawl_link(query,row):
    path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
    query = query.strip()
    url = f"https://shoesandsox.com.au/search.php?page=1&section=product&search_query={query}&in_stock=1"
    while url:
        try:
            r = s.get(url)
            tree = html.fromstring(r.text)
            products = tree.xpath("//a[@class='uk-width-1-1 uk-height-1-1 uk-inline uk-animation-toggle']/@href")
            for product in tqdm(products):
                d = crawl_detail(product)
                output = {
                    "Retailer":'shoesandsox',
                    "path":path,
                    "search_term":query.strip(),
                    "prd_id":d["prd_id"],
                    "images":d["imgs"],
                    "url":product
                }
                o_row.writerow(output.values())
                # all_data.append(output)
                # print(output)
                # if len(all_data) % 20 == 0:
                #     pd.DataFrame(all_data).to_csv("shoesandsox_imgs.csv",sep=",",index=False)
            rel_url = "".join(tree.xpath("//span[@uk-pagination-next]//ancestor::a/@href"))
            if(rel_url):
                url = "https://shoesandsox.com.au"+rel_url
            else:
                url = False
        except:
            pass

def crawl_query(row):
    querys = row['Query'].split(',')
    for i in querys:
        crawl_link(i,row)

df = pd.read_excel('QueryList_v1.xlsx.xlsx')

for i in range(len(df)):
    row = df.iloc[i].to_dict()
    crawl_query(row)

# df = pd.DataFrame(all_data)
# df.to_csv("shoesandsox_imgs.csv",sep=",",index=False)
