from requests import Session
import re
import pandas as pd
import csv
import mysql.connector
from lxml import html
from tqdm import tqdm

class Bluebungalow:
    def __init__(self) -> None:
        username = 'root'
        password = 'nixis123'
        database_name = 'crawling'
        self.conn = mysql.connector.connect(host='localhost',user = username, passwd = password,db = database_name)
        self.cursor = self.conn.cursor(dictionary=True)        
        self.input_file = 'Query_Coats_and_Jackets.xlsx'
        output_file = 'bluebungalow_imgs.csv'

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
            imgs = "|".join(["https:"+i for i in tree.xpath('//a[@class="MagicZoom"]/@href')])
            return {"imgs":imgs}
        except Exception as e:
            return {}


    def crawl_link(self,query,row) -> None:
        path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
        query = query.strip()
        url = f'https://bluebungalow.com.au/search?q={query}'
        r = self.s.get(url)
        
        keys = self.get_keys(r.text)

        api_url = f"https://search.unbxd.io/{keys['apiKey']}/{keys['siteName']}/search?&q={query}&view=Grid&rows=59&start={0}&format=json&stats=price&fields=title,uniqueId,collection_tag,plain_tags,publishedAt,secondaryImageUrl,documentType,imageUrl,price,sellingPrice,priceMax,sku,imageUrl,sizeMap,relevantDocument,productUrl,variantId,brand,availability,altImageUrl,altImageTag&facet.multiselect=true&indent=off&variants=true&variants.count=1&filter=documentType:product&filter=publishedAt:*&device-type=Desktop&unbxd-url=https://bluebungalow.com.au/search?q={query}&unbxd-referrer=&user-type=new&api-key={keys['apiKey']}"
        
        r = self.s.get(api_url)
        js = r.json()
        total_count = js['response']['numberOfProducts']
        
        # nop = 1
        # start = 0
        # page_number = 0
        print(total_count)
        print(query)
        for i in tqdm(range(0,total_count,59)):
        # while page_number<nop:            
            api_url = f"https://search.unbxd.io/{keys['apiKey']}/{keys['siteName']}/search?=&q={query}&view=Grid&rows=59&start={i}&format=json&stats=price&fields=title,uniqueId,collection_tag,plain_tags,publishedAt,secondaryImageUrl,documentType,imageUrl,price,sellingPrice,priceMax,sku,imageUrl,sizeMap,relevantDocument,productUrl,variantId,brand,availability,altImageUrl,altImageTag&facet.multiselect=true&indent=off&variants=true&variants.count=1&filter=documentType:product&filter=publishedAt:*&device-type=Desktop&unbxd-url=https://bluebungalow.com.au/search?q={query}&unbxd-referrer=&user-type=new&api-key={keys['apiKey']}"
            r = self.s.get(api_url)
            js = r.json()
            for product in tqdm(js['response']['products']):
                unique_id = product['uniqueId']
                product_url = "https://bluebungalow.com.au"+product['productUrl']

                d = self.crawl_detail(product_url)
                if d:
                    output = {
                        "Retailer":'bluebungalow',
                        "path":path,
                        "search_term":query.strip(),
                        "prd_id":unique_id,
                        "images":d['imgs'],
                        "url":"https://bluebungalow.com.au"+product_url
                    }
                    self.o_row.writerow(list(output.values()))
                    # print(output)

    def crawl_query(self,row,row_index,query_num) -> None:
        querys = row['Query'].split(',')
        for query_index,i in enumerate(querys[query_num:]):
            query = 'insert into log(retailer,attribute,tags,category,query,row_num,query_num) values(%s,%s,%s,%s,%s,%s,%s)'
            values = ['bluebungalow',row['Attribute'],row['Tags'],row['Category'],i,int(row_index),int(query_index+query_num)]
            self.cursor.execute(query,values)
            self.conn.commit()
            self.crawl_link(i,row)
        query_num = 0
        return query_num

    def get_keys(self,pageContent):
        siteName = re.search('var UnbxdSiteName = "(.*?)"',pageContent).group(1)
        apiKey = re.search('var UnbxdApiKey = "(.*?)"',pageContent).group(1)
        return {
            "siteName":siteName,
            "apiKey":apiKey
        }

    def remove_log(self) -> None:
        query = 'delete from log where retailer="bluebungalow"'
        self.cursor.execute(query)
        self.conn.commit()

    def start_requests(self) -> None:
        df = pd.read_excel(self.input_file)

        query = 'select * from log where retailer = "bluebungalow"'
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

d = Bluebungalow()
d.create_table()
d.start_requests()
d.remove_log()