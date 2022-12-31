from requests import Session
from lxml import html
import re
import json
import pandas as pd
import csv

s = Session()
s.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'
# f = open("clarksusa_file.csv","w")
# writer = csv.writer(f)




all_data = []

def crawl_detail(prd_id):
    try:
        url = f"https://clarks.scene7.com/is/image/Pangaea2Build/{prd_id}_SET?req=set,json"
        r = s.get(url)
        js = json.loads(r.text.replace('/*jsonp*/s7jsonResponse(',"").replace(',"");',""))
        imgs = []
        for i in js['set']['item']:
            img_id = i['s']['n']
            img_format = f"https://clarks.scene7.com/is/image/{img_id}?wid=2000&hei=2000&fmt=jpg"
            imgs.append(img_format)

        return {"imgs":"|".join(imgs)}
    except Exception as e:
        return ""


def crawl_link(query,row):
    path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
    query = query.strip()
    url = f"https://www.clarksusa.com/search-ajax?query={query}"
    try:
        r = s.get(url)
        products = r.json()['products']
        for product in products:
            detail_page = "https://www.clarksusa.com"+product['url']
            prd_id = product["code"]
            d = crawl_detail(prd_id)
            print(d)
            output = {
                "Retailer":'clarksusa',
                "path":path,
                "search_term":query,
                "prd_id":prd_id,
                "images":d["imgs"],
                "url":detail_page
            }
            all_data.append(output)
            print(output)
            # writer.writerow([
            #     "clarksusa",
            #     path,
            #     query,
            #     prd_id,
            #     d["imgs"],
            #     detail_page
            # ])
            # rel_url = "".join(tree.xpath("//span[@uk-pagination-next]//ancestor::a/@href"))
            # if(rel_url):
            #     url = "https://shoesandsox.com.au"+rel_url
            # else:
            #     url = False
    except Exception as e:
        # print(e)
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
df.to_csv("clarksusa_imgs.csv",sep=",",index=False)
