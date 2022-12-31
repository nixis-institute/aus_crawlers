from requests import Session
from lxml import html
import re
import json
import pandas as pd
import csv
s = Session()
import pdb

all_data = []

s.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'
all_data = []
def crawl_detail(url):
    try:
        r = s.get(url)
        data = re.findall('window.__PRELOADED_STATE__ =\s(.*?)\}\};',r.text)
        if len(data) > 0:
            # pdb.set_trace()
            js = json.loads(data[0]+"}}")
            # pdb.set_trace()
            all_imgs = []
            imgs = js.get("product").get("productDetails").get("images")
            for i in imgs:
                if i['format'] == "superZoomPdp":
                    all_imgs.append(i['url'])
            return {
                "images":"|".join(all_imgs)
            }
    except Exception as e:
        print(str(e))

def crawling_pages(url,query):
    try:
        m_url = url.format(1,query,query)
        r = s.get(m_url)
        js = r.json()
        pages = js.get("pagination").get("totalPages")
        return pages
    except:
        pass

def crawl_link(query,row):
    try:
        path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
        url = "https://www.ajio.com/api/search?fields=SITE&currentPage={}&pageSize=45&format=json&query={}%3Arelevance&sortBy=relevance&text={}&gridColumns=3&advfilter=true&platform=site"
        pages = crawling_pages(url,query)
        for page in range(1,pages+1):
            formatted_url = url.format(page,query.replace(" ", "%20"),query.replace(" ", "%20"))
            r = s.get(formatted_url)
            js = r.json()
            # with open("temp.json", "w") as f:
            #     json.dump(js, f)
            # return

            products = js.get("products")
            for i in products:
                detail_url = "https://www.ajio.com"+i.get("url")
                d = crawl_detail(detail_url)
                output = {
                    "Retailer":'Ajio',
                    "path":path,
                    "search_term":query,
                    "prd_id":i['code'],
                    "images":d['images'],
                    "url":detail_url
                }
                all_data.append(output)
                if len(all_data) % 20 == 0:
                    pd.DataFrame(all_data).to_csv("ajio_sarees.csv",index=False)

    except Exception as e:
        print(e)
def crawl_query(row):
    querys = row['Query'].split(',')
    for query in querys:
        crawl_link(query,row)

df = pd.read_csv('Query_Ajio_Saree.csv')
for i in range(len(df)):
    row = df.iloc[i].to_dict()
    crawl_query(row)

df = pd.DataFrame(all_data)
df.to_csv("ajio_sarees.csv",index=False)