from requests import Session
from lxml import html
import re
import json
import pandas as pd
import csv

s = Session()
s.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'


all_data = []

def crawl_detail(url):
    try:
        imgs = []
        r = s.get(url)
        prd_id = re.search('"sku":"(.*?)"',r.text).group(1)
        for i in json.loads("["+re.search('"galleryMediaZoom":\[(.*?)\]',r.text).group(1)+"]"):
            imgs.append(i['uri'])
        return {"prd_id":prd_id,"imgs":"|".join(list(set(imgs)))}
    except Exception as e:
        return ""


def crawl_link(query,row):
    path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
    query = query.strip()
    
    current_page = 1 
    nop = 10

    while current_page!=nop:
        try:
            url = f"https://www.zalando.co.uk/catalog/?q={query}&p={current_page}"
            r = s.get(url)
            tree = html.fromstring(r.text)
            products = re.findall('\],"uri":"(.*?)","inW',r.text)
            current_page = re.search('currentPage":(.*?),',r.text).group(1)
            nop = re.search('numberOfPages":(.*?)\}',r.text).group(1)
            
            for product in products:

                d = crawl_detail(product)
                output = {
                    "Retailer":'zalando_uk',
                    "path":path,
                    "search_term":query.strip(),
                    "prd_id":d["prd_id"],
                    "images":d["imgs"],
                    "url":product
                }
                all_data.append(output)
                print(output)
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


df = pd.DataFrame(all_data)
df.to_csv("zalando_uk_imgs.csv",sep=",",index=False)