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
        for img in re.findall('\,"full"\:"(.*?)"\,"caption"\:',r.text):
                all_imgs.append(img)
        sku = re.findall('{"sku":"(.*?)",',r.text)
        d = {"sku":sku[0], "images":'|'.join(all_imgs)}
        return d
    except:
        pass


def pagination_pages(response):
    try:
        tree = html.fromstring(response)
        total_products = ''.join(set(tree.xpath("//div[@class='toolbar__prod-count prod-count-wrapper']//text()"))).replace(" ","").replace("results","")
        total_pages = int(int(total_products)/54)+1
    except:
        total_pages = 1
    if total_pages == 1:
        total_pages = 2
    return total_pages


def crawl_link(row,query):
    try:
        path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
        quary_url = "https://www.midasshoes.com.au/catalogsearch/result/index/?p={}&q={}".format(1,query)
        r = s.get(quary_url)
        total_pages = pagination_pages(r.text)
        print("Total pages :- {}".format(total_pages))
        for page in range(1,total_pages):
            quary_url = "https://www.midasshoes.com.au/catalogsearch/result/index/?p={}&q={}"
            response  = s.get(quary_url.format(page,query))
            print("Landing page :- {}-------------******------------".format(response.url))
            tree = html.fromstring(response.text)
            products = tree.xpath("//ol[@class='product-list']//li//a[@class='product-tile__link  product-tile__link--name']//@href")
            
            for product in products:
                output = dict()
                d = crawl_detail(product)
                output["Retailer"] = "midasshoes"
                output["path"] = path
                output["search_term"] = query
                output["prd_id"] = d["sku"]
                output["images"] = d["images"]
                output["url"] = product
                print(output)
                all_data.append(output)
    except:
        pass


def crawl_query(row):
    querys = row['Query'].split(',')
    for query in querys:
        crawl_link(row,query)

df = pd.read_excel('QueryList_v1.xlsx.xlsx')
df = df[1:3]
for i in range(len(df)):
    row = df.iloc[i].to_dict()
    crawl_query(row)

df = pd.DataFrame(all_data)
df.to_csv("midasshoes_imgs.csv",sep=",",index=False)