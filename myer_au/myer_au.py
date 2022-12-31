from requests import Session
import re
import pandas as pd
import csv
import mysql.connector
from lxml import html
import math
from tqdm import tqdm

class Myer_AU:
    def __init__(self) -> None:
        username = 'root'
        password = 'nixis123'
        database_name = 'crawling'
        self.retailer = 'myer_au'
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
    
    
    def crawl_detail(self,prd_id) -> dict or str:
        try:
            url = f"https://images.footlocker.com/is/image/EBFL2/{prd_id}/?req=set,json&id={prd_id}&handler=altset_{prd_id}"
            r = self.s.get(url)
            
            imgs = []
            for i in list(set(re.findall('\{"n":"(.*?)"',r.text))):
                img_id = i
                img_format = f"https://images.footlocker.com/is/image/{img_id}?wid=2000&hei=2000&fmt=png-alpha"
                imgs.append(img_format)

            return {"imgs":"|".join(imgs)}
        except Exception as e:
            print(e)
            return ""


    def crawl_link(self,query,row) -> None:
        path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
        query = query.strip()
        # url = "https://www.footlocker.com/api/products/search?query={}&currentPage={}&pageSize=48&timestamp=6"
        url = "https://api-online.myer.com.au/v3/product/search?pageNumber={}&pageSize=48&facets=&query={}"
        
        r = self.s.get(url.format(1,query))
        pagePage = math.ceil(r.json()['productTotalCount']/48)
        totalCount = r.json()['productTotalCount']
        print(query,totalCount)
        for i in tqdm(range(1,pagePage+1)):
            r = self.s.get(url.format(i,query))
            js = r.json()
            for product in js['productList']:
                try:
                    prd_id = product['id']
                    product_url = 'https://www.myer.com.au/p/'+product['seoToken']
                    # d = self.crawl_detail(prd_id)
                    imgs = "|".join(['https://myer-media.com.au/wcsstore/MyerCatalogAssetStore/'+i['baseUrl'].replace('{{size}}','720x928') for i in product['media']])
                    output = {
                        "Retailer":self.retailer,
                        "path":path,
                        "search_term":query,
                        "prd_id":prd_id,
                        "images":imgs,
                        "url":product_url
                    }
                    self.o_row.writerow(list(output.values()))
                except:
                    pass

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

d = Myer_AU()
d.create_table()
d.start_requests()
d.remove_log()