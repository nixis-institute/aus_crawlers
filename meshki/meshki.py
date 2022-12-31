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

class Meshki_AU:
    def __init__(self) -> None:
        username = 'root'
        password = 'nixis123'
        database_name = 'crawling'
        self.retailer = 'meshki'
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
            soup = BS(r.text,'html.parser')
            imgs = []
            for i in soup.find_all('img','component-image__image'):
                if i.get('data-src') == None:
                    imgs.append('https:'+i.get('src'))
                else:
                    imgs.append('https:'+i.get('data-src'))
            return {"imgs":"|".join(imgs)}
        except Exception as e:
            return {}


    def crawl_link(self,query,row) -> None:
        path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
        query = query.strip()
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:99.0) Gecko/20100101 Firefox/99.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'content-type': 'application/x-www-form-urlencoded',
        'Origin': 'https://www.meshki.com.au',
        'Connection': 'keep-alive',
        'Referer': 'https://www.meshki.com.au/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache'}
        data = '{"requests":[{"indexName":"shopify_au_products","params":"highlightPreTag=%3Cais-highlight-0000000000%3E&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&hitsPerPage=12&distinct=true&attributesToRetrieve=%5B%22handle%22%2C%22title%22%2C%22id%22%2C%22named_tags%22%2C%22sku%22%2C%22vendor%22%2C%22variants_min_price%22%2C%22variants_max_price%22%2C%22type%22%2C%22meta%22%2C%22tags%22%2C%22published_at%22%2C%22price%22%2C%22objectID%22%2C%22collections%22%2C%22product_image%22%2C%22id%22%2C%22compare_at_price%22%5D&query='+query+'&maxValuesPerFacet=100&page='+'0'+'&facets=%5B%22named_tags.Sub%22%2C%22named_tags.colour%22%2C%22options.size%22%2C%22named_tags.occasion%22%2C%22named_tags.fabric%22%5D&tagFilters="}]}'
        r = self.s.post('https://0eyfrdt3sh-2.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.11.0)%3B%20Browser%20(lite)%3B%20JS%20Helper%20(3.6.2)%3B%20react%20(16.8.0)%3B%20react-instantsearch%20(6.15.0)&x-algolia-api-key=256a69825394fdb320b26527d4a7ce6e&x-algolia-application-id=0EYFRDT3SH', headers=headers, data=data)

        try:
            total_count = r.json()['results'][0]['nbHits']
            total_pages = r.json()['results'][0]['nbPages']
            print(query,total_count)
            for i in tqdm(range(0,total_pages+1)):
                data = '{"requests":[{"indexName":"shopify_au_products","params":"highlightPreTag=%3Cais-highlight-0000000000%3E&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&hitsPerPage=12&distinct=true&attributesToRetrieve=%5B%22handle%22%2C%22title%22%2C%22id%22%2C%22named_tags%22%2C%22sku%22%2C%22vendor%22%2C%22variants_min_price%22%2C%22variants_max_price%22%2C%22type%22%2C%22meta%22%2C%22tags%22%2C%22published_at%22%2C%22price%22%2C%22objectID%22%2C%22collections%22%2C%22product_image%22%2C%22id%22%2C%22compare_at_price%22%5D&query='+query+'&maxValuesPerFacet=100&page='+str(i)+'&facets=%5B%22named_tags.Sub%22%2C%22named_tags.colour%22%2C%22options.size%22%2C%22named_tags.occasion%22%2C%22named_tags.fabric%22%5D&tagFilters="}]}'
                r = self.s.post('https://0eyfrdt3sh-2.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.11.0)%3B%20Browser%20(lite)%3B%20JS%20Helper%20(3.6.2)%3B%20react%20(16.8.0)%3B%20react-instantsearch%20(6.15.0)&x-algolia-api-key=256a69825394fdb320b26527d4a7ce6e&x-algolia-application-id=0EYFRDT3SH', headers=headers, data=data)
                products = r.json()['results'][0]['hits']
                for product in tqdm(products):
                    # try:
                        unique_id = product['sku']
                        product_url = "https://www.meshki.com.au/products/"+product['handle']

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
        except:
            print(query,'not found')
            # print(url)
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

d = Meshki_AU()
d.create_table()
d.start_requests()
d.remove_log()