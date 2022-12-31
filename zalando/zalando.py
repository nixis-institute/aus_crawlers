from requests import Session
from lxml import html
import re
import json
import pandas as pd
import csv

s = Session()
s.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'
# f = open("zalando_imgs.csv","w")
# writer = csv.writer(f)
# writer.writerow(["Retailer","path","search_term","prd_id","images","url"])



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
    

    # totalPage = 10
    # pagenum = 1
    current_page = 1 
    nop = 10

    while current_page!=nop:
        try:
            url = f"https://en.zalando.de/catalogue/?q={query}&p={current_page}"
            r = s.get(url)
            tree = html.fromstring(r.text)
            # js = json.loads("".join(tree.xpath('//script[@class="re-data-el-hydrate"]//text()')))
            # end_cursor = "".join(re.findall('"endCursor":"(.*?)"',r.text))
            products = re.findall('\],"uri":"(.*?)","inW',r.text)
            current_page = re.search('currentPage":(.*?),',r.text).group(1)
            nop = re.search('numberOfPages":(.*?)\}',r.text).group(1)
            
            # r = s.post('https://api.dsw.com/content/api/v1/page',params=params,json=json_data)
            # products = r.json()['widgets'][0]['products']
            # if pagenum == 1:
                # totalPage = products['page_count']
            
            for product in products:
                # prd_id = product['id']
                # prd_url = f"https://www.dsw.com/en/us/product/~/{prd_id}"
                # color_ids = product['color_code_list']

                d = crawl_detail(product)
                output = {
                    "Retailer":'zalando',
                    "path":path,
                    "search_term":query.strip(),
                    "prd_id":d["prd_id"],
                    "images":d["imgs"],
                    "url":product
                }
                all_data.append(output)
                # writer.writerow(output.values())
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
df.to_csv("zalando_imgs.csv",sep=",",index=False)