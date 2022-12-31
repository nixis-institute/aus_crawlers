from cmath import exp
from requests import Session
from lxml import html
import re
import json
import pandas as pd
import requests
import csv


s = Session()
s.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'

all_data = []



def crawl_detail(url):
    
    try:
        r = s.get(url)
        tree = html.fromstring(r.text)
        main = "".join(tree.xpath('//button[@data-placement="product-gallery"]/@data-image-src'))
        imgs = "|".join(tree.xpath("//span[@class='alternate_image_url']//text()")).replace('//cdn.shopify','https://cdn.shopify')
        return {
            "images":main + "|" + imgs
        }
    except:
        return{
            "images":""
        }




def crawl_link(query,row):
    path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
    totalPage = 10
    pagenum = 0
    print(query)
    query = query.strip()
    while totalPage>=pagenum + 2:
        try:
            params = (
                ('x-algolia-agent', 'Algolia for JavaScript (4.3.0); Browser'),
            )
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:97.0) Gecko/20100101 Firefox/97.0',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'x-algolia-api-key': '0e7364c3b87d2ef8f6ab2064f0519abb',
                'x-algolia-application-id': 'XN5VEPVD4I',
                'content-type': 'application/x-www-form-urlencoded',
                'Origin': 'https://www.fashionnova.com',
                'Connection': 'keep-alive',
                'Referer': 'https://www.fashionnova.com/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'cross-site',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
            }
            url = "https://xn5vepvd4i-3.algolianet.com/1/indexes/products/query"
            data = '{"query":"'+query+'","userToken":"anonymous-8777e2c7-1373-4df9-bb99-59d4ebc2024e","ruleContexts":["all"],"analyticsTags":["all","desktop","Returning","India"],"clickAnalytics":true,"distinct":1,"page":'+str(pagenum)+',"hitsPerPage":48,"facetFilters":[],"facetingAfterDistinct":true,"attributesToRetrieve":["handle","image","title"],"personalizationImpact":0}'
            response = requests.post(url, headers=headers, params=params, data=data)
            
            js = response.json()
            if(pagenum == 0):
                totalPage =  js['nbHits']/48
            
            for i in js['hits']:
                detail_url = f"https://www.fashionnova.com/products/{i['handle']}"
                d = crawl_detail(detail_url)
                output = {
                    "Retailer":'Fashionnova',
                    "path":path,
                    "search_term":query.strip(),
                    "prd_id":i['handle'],
                    "images":d['images'],
                    "url":detail_url
                }
                all_data.append(output)
                print(output)
            pagenum = pagenum + 1
        except Exception as e:
            print(e)
            pass



def crawl_query(row):
    querys = row['Query'].split(',')
    for i in querys:
        crawl_link(i,row)


df = pd.read_excel('Query_Coats_and_Jackets.xlsx')
for i in range(len(df)):
    row = df.iloc[i].to_dict()
    crawl_query(row)


df = pd.DataFrame(all_data)
df.to_csv('fashionnova_imgs_coats_jackets.csv',index=False)
