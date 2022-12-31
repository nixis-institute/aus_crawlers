from requests import Session
import re
import pandas as pd
import csv
import mysql.connector
from lxml import html
from tqdm import tqdm
import math

class BambiniFashion:
    def __init__(self) -> None:
        username = 'root'
        password = 'nixis123'
        database_name = 'crawling'
        self.retailer = 'bambinifashion'
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
            r = self.s.get(url)
            tree = html.fromstring(r.text)
            imgs = "|".join(tree.xpath('//a[@class="product-single-carousel--main-slide-link"]/@href'))
            # sku = re.search('"ttGuid":"(.*?)"',r.text).group(1)
            
            return {"imgs":imgs}
        except Exception as e:
            return {}


    def crawl_link(self,query,row) -> None:
        path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
        query = query.strip()
        url = 'https://shop.bambinifashion.com/search/{}/page={}'
        
        r = self.s.get(url.format(query,1))
        # tree = html.fromstring(r.text)
        
        
        try:
            total_count = int(re.search('<h2 class="category-list-title">(.*?) ',r.text).group(1))

            total_page = math.ceil(total_count/60)
            print(query,total_count)
            for i in tqdm(range(1,total_page+1)):

                r = self.s.get(url.format(query,i))
                tree = html.fromstring(r.text)
                products = tree.xpath('//a[@class="product-card-link"]/@href')
                for product in tqdm(products):
                    # try:
                        product_url = "https://shop.bambinifashion.com"+product
                        prd_id = product.split('-')[-1].replace('.html','')

                        d = self.crawl_detail(product_url)
                        if d:
                            output = {
                                "Retailer":self.retailer,
                                "path":path,
                                "search_term":query.strip(),
                                "prd_id":prd_id,
                                "images":d['imgs'],
                                "url":product_url
                            }
                            self.o_row.writerow(list(output.values()))
        except Exception as e:
            print(query)
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

d = BambiniFashion()
d.create_table()
d.start_requests()
d.remove_log()