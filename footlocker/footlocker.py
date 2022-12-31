from requests import Session
from lxml import html
import re
import json
import pandas as pd
import csv

s = Session()
s.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'
# f = open("footlocker.csv","w")
# writer = csv.writer(f)




all_data = []

def crawl_detail(prd_id):
    try:
        url = f"https://images.footlocker.com/is/image/EBFL2/{prd_id}/?req=set,json&id={prd_id}&handler=altset_{prd_id}"
        r = s.get(url)
        
        imgs = []
        for i in list(set(re.findall('\{"n":"(.*?)"',r.text))):
            img_id = i
            img_format = f"https://images.footlocker.com/is/image/{img_id}?wid=2000&hei=2000&fmt=png-alpha"
            imgs.append(img_format)

        return {"imgs":"|".join(imgs)}
    except Exception as e:
        print(e)
        return ""


def crawl_link(query,row):
    path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
    query = query.strip()
    totalPage = 10
    pagenum = 0
    url = f"https://www.footlocker.com/api/products/search?query={query}&currentPage={pagenum}&pageSize=48&timestamp=6"
    while totalPage>=pagenum:
        try:
            r = s.get(url)
            js = r.json()
            if pagenum == 0:
                totalPage = js['pagination']['totalPages']
            for product in js['products']:
                prd_id = product['sku']
                product_url = f"https://www.footlocker.com/product/~/{prd_id}.html"
                d = crawl_detail(prd_id)
                output = {
                    "Retailer":'footlocker',
                    "path":path,
                    "search_term":query,
                    "prd_id":prd_id,
                    "images":d['imgs'],
                    "url":product_url
                }
                all_data.append(output)
                print(output)
                # writer.writerow([
                #     "footlocker",
                #     path,
                #     query,
                #     prd_id,
                #     d["imgs"],
                #     product_url

                # ])
            # rel_url = "".join(tree.xpath("//span[@uk-pagination-next]//ancestor::a/@href"))
            # if(rel_url):
            #     url = "https://shoesandsox.com.au"+rel_url
            # else:
            #     url = False
            pagenum +=1
        except Exception as e:
            print(e)
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
df.to_csv("footlocker_imgs.csv",sep=",",index=False)
