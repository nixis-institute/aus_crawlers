from requests import Session
import re
import pandas as pd
import csv
import mysql.connector
from lxml import html
from tqdm import tqdm
import json
from bs4 import BeautifulSoup as BS
import math

class Cottonon:
    def __init__(self) -> None:
        username = 'root'
        password = 'root'
        database_name = 'crawling'
        self.retailer = 'cottonon'
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
            imgs = "|".join([json.loads(i)['src'] for i in tree.xpath('//img[@class="media-source"]/@data-pssrc')])            
            return {"imgs":imgs}
        except Exception as e:
            return {}


    def crawl_link(self,query,row) -> None:
        path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
        query = query.strip()
        url = f'https://cottonon.com/AU/co/?lang=en_AU&q={query}'
        # url = f'https://cottonon.com/AU/kids/?lang=en_AU&q=boys%20blazer%20jackets
        
        r = self.s.get(url.format(query))
        tree = html.fromstring(r.text)
        
        
        try:
            total_count = int("".join(tree.xpath('//span[@class="total-product-count"]//text()')))
            # total_page = math.ceil(total_count/24)
            total_page = total_count
            print(query,total_count)
            for i in tqdm(range(0,total_page+1,24)):
                url = "https://cottonon.com/AU/co/?q={}&start={}&sz=24"
                r = self.s.get(url.format(query,i))
                js = json.loads(re.search('var dataLayerArr = \[(.*?)\];',r.text).group(1))
                products = js['ecommerce']['impressions']
                for product in tqdm(products):
                    # try:
                        unique_id = product['dimension9']
                        product_url = "https://cottonon.com/AU/"+product['name'].replace(' ','-').lower()+"/"+product['dimension9']+".html"
                        

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
                    # except Exception as e:
                    #     pass
        except:
            print(query,'not found')
            print(url)
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

d = Cottonon()
d.create_table()
d.start_requests()
d.remove_log()