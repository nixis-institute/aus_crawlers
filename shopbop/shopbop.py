from requests import Session
from lxml import html
import pandas as pd
s = Session()

s.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'
all_data = []

def crawl_detail(url):
    try:
        r = s.get(url)
        tree= html.fromstring(r.text)
        all_imgs = []
        for img in tree.xpath("//ul[@id='display-list']//li//img//@src"):
            # if "p_mosaic_pd" in img:
            all_imgs.append(img)
        sku = tree.xpath("//meta[@itemprop='sku']//@content")
        d = {"sku":sku[0], "images":'|'.join(all_imgs)}
        return d
        

        
    except:
        pass

def crawl_link(url_with_query,row):
    try:
        path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
        r = s.get(url_with_query)
        query = url_with_query.split("=")[-2].split("&")[0]
        tree = html.fromstring(r.text)
        products = tree.xpath("//ul[@class='products flex-flow-inline celwidget']//li//a[@class='photo']//@href")
        for product in products:
            print(product)
            output = dict()
            d = crawl_detail("https://www.shopbop.com{}".format(product))
            output["Retailer"] = "shopbop"
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
        quary_url = "https://www.shopbop.com/products?query={}&searchSuggestion=false".format(query)
        crawl_link(quary_url,row)

df = pd.read_excel('QueryList_v1.xlsx.xlsx')
for i in range(len(df)):
    row = df.iloc[i].to_dict()
    crawl_query(row)

df = pd.DataFrame(all_data)
df.to_csv("shopbop_imgs.csv",sep=",",index=False)