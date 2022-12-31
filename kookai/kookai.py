from requests import Session
import re
import pandas as pd
import csv
import mysql.connector
from lxml import html
from tqdm import tqdm
import json

class KooKai:
    def __init__(self) -> None:
        username = 'root'
        password = 'nixis123'
        database_name = 'crawling'
        self.retailer = 'kookai'
        self.conn = mysql.connector.connect(host='localhost',user = username, passwd = password,db = database_name)
        self.cursor = self.conn.cursor(dictionary=True)        
        self.input_file = 'Query_Coats_and_Jackets.xlsx'
        output_file = f'{self.retailer}_imgs.csv'

        try:
            open(output_file,'r')
            output = open(output_file,'a')
            self.o_row = csv.writer(output)
        except:
            output = open(output_file,'a')
            self.o_row = csv.writer(output)
            self.o_row.writerow(['Retailer','path','search_term','prd_id','images','url'])    

        self.s = Session()
        self.s.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'
    def create_table(self) -> None:
        try:
            self.cursor.execute('create table log(retailer varchar(255),attribute varchar(255),tags varchar(255),category varchar(255),query varchar(255),row_num int,query_num int)')
            self.conn.commit()
            print('table log created...')
        except:
            # print('table log already exists')
            pass
    
    
    def crawl_detail(self,url) -> dict or str:
        try:
            imgs = []
            r = self.s.get(url)
            tree = html.fromstring(r.text)
            imgs = "|".join(["https:"+i for i in tree.xpath('//div[@class="product__thumbnails"]//img/@data-src')])
            return {"imgs":imgs}
        except Exception as e:
            return {}


    def crawl_link(self,query,row) -> None:
        path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
        query = query.strip()
        # url = f'https://us.princesspolly.com/pages/search-results?q={query}'
        url = f'https://www.kookai.com.au/search?&q={query}'
        
        r = self.s.get(url)
        
        keys = self.get_keys(r.text)

        api_url = "https://search.unbxd.io/{}/{}/search?=&q={}&rows=48&view=grid&variants.groupby=v_colour&variants.count=5&start={}&format=json&version=V2&facet.multilevel=categoryPath&f.categoryPath.displayName=category&f.categoryPath.max.depth=4&f.categoryPath.facet.limit=100&stats=price&fields=title,uniqueId,published_at,imageUrl2,v_colour,imageUrl1,unbxd_price,price,sku,imageUrl,productUrlParent,categoryPath,relevantDocument,imageUrlMain,imageUrlHover,colours,collections,unbxd_parentcolours,v_unbxd_parentcolours,categoryPath1,categoryPath1_fq,categoryPath2_fq,productUrl,variantId,id,unbxd_sellingprice,v_sellingprice,variant_metadata,is_available,v_price&facet.multiselect=true&indent=off&variants=true&device-type=Desktop&unbxd-url=https://www.kookai.com.au/search?type=product&q={}&unbxd-referrer=&rows=24&view=grid&variants.groupby=v_colour&variants.count=5&start=0&user-type=new&api-key={}&uid=uid-1651076863490-31111"
        r = self.s.get(api_url.format(keys['apiKey'],keys['siteName'],query,0,query,keys['apiKey']))
        js = r.json()
        total_count = js['response']['numberOfProducts']
        print(query,total_count)
        for i in tqdm(range(0,total_count,48)):
            # api_url = f"https://search.unbxd.io/{keys['apiKey']}/{keys['siteName']}/search?=&q={query}&view=Grid&rows=59&start={i}&format=json&stats=price&fields=title,uniqueId,collection_tag,plain_tags,publishedAt,secondaryImageUrl,documentType,imageUrl,price,sellingPrice,priceMax,sku,imageUrl,sizeMap,relevantDocument,productUrl,variantId,brand,availability,altImageUrl,altImageTag&facet.multiselect=true&indent=off&variants=true&variants.count=1&filter=documentType:product&filter=publishedAt:*&device-type=Desktop&unbxd-url=https://bluebungalow.com.au/search?q={query}&unbxd-referrer=&user-type=new&api-key={keys['apiKey']}"
            # r = self.s.get(api_url.format(query,i,keys['store_id'],keys['uuid']))
            r = self.s.get(api_url.format(keys['apiKey'],keys['siteName'],query,i,query,keys['apiKey']))
            js = r.json()
            # js = json.loads(r.text.replace('ispSearchResult(','').replace(');',''))

            for product in tqdm(js['response']['products']):
                unique_id = product['uniqueId']
                product_url = "https://www.kookai.com.au"+product['productUrlParent']

                d = self.crawl_detail(product_url)
                if d:
                    output = {
                        "Retailer":self.retailer,
                        "path":path,
                        "search_term":query.strip(),
                        "prd_id":unique_id,
                        "images":d['imgs'],
                        "url":product_url
                    }
                    self.o_row.writerow(list(output.values()))
                    # print(output)

    def crawl_query(self,row,row_index,query_num) -> None:
        querys = row['Query'].split(',')
        for query_index,i in enumerate(querys[query_num:]):
            query = 'insert into log(retailer,attribute,tags,category,query,row_num,query_num) values(%s,%s,%s,%s,%s,%s,%s)'
            values = [self.retailer,row['Attribute'],row['Tags'],row['Category'],i,int(row_index),int(query_index+query_num)]
            self.cursor.execute(query,values)
            self.conn.commit()
            self.crawl_link(i,row)
        query_num = 0
        return query_num

    def get_keys(self,pageContent):
        # store_id = re.search('"shopId":(.*?),',pageContent).group(1)
        # uuid = re.search('UUID=(.*?)\&',pageContent).group(1)
        siteName = re.search('UnbxdSiteName = "(.*?)"',pageContent).group(1)
        apiKey = re.search('UnbxdApiKey = "(.*?)"',pageContent).group(1)
        return {
            "siteName":siteName,
            "apiKey":apiKey
        }

    def remove_log(self) -> None:
        query = f'delete from log where retailer="{self.retailer}"'
        self.cursor.execute(query)
        self.conn.commit()

    def start_requests(self) -> None:
        df = pd.read_excel(self.input_file)

        query = f'select * from log where retailer = "{self.retailer}"'
        self.cursor.execute(query)
        d = self.cursor.fetchall()
        if d:
            log = pd.DataFrame(d)
            inf = log.iloc[-1].to_dict()
            row_num = inf['row_num']
            query_num = inf['query_num']
        else:
            row_num = 0
            query_num = 0

        for i in range(len(df[row_num:])):
            row = df.iloc[row_num+i].to_dict()
            query_num = self.crawl_query(row,row_num+i,query_num)

d = KooKai()
d.create_table()
d.start_requests()
d.remove_log()