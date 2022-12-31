from requests import Session
import re
import pandas as pd
# import csv
# import mysql.connector
from lxml import html
from tqdm import tqdm
from bs4 import BeautifulSoup as BS
import math
from pymongo import MongoClient
# import json
class Revolve:
    def __init__(self) -> None:
        # username = 'root'
        # password = 'nixis123'
        # database_name = 'crawling'
        self.retailer = 'revolve'
        # self.conn = mysql.connector.connect(host='localhost',user = username, passwd = password,db = database_name)
        # self.cursor = self.conn.cursor(dictionary=True)        
        self.input_file = 'Query_Coats_and_Jackets.xlsx'
        # output_file = f'{self.retailer}_imgs.csv'
        # self.product_output = open("product.json","w")
        # self.outfit = open("outfit.json","w")
        uri = "mongodb://localhost:27017/?readPreference=primary&ssl=false"
        client = MongoClient(uri)
        mydb = client['crawling']
        self.product_col = mydb['product']
        self.outfit_col = mydb['outfit']



        # try:
        #     open(output_file,'r')
        #     output = open(output_file,'a')
        #     self.o_row = csv.writer(output)
        # except:
        #     output = open(output_file,'a')
        #     self.o_row = csv.writer(output)
        #     self.o_row.writerow(['Retailer','path','search_term','prd_id','images','url'])    

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


    def complete_look(self,pageContent) -> list:
        tree = html.fromstring(pageContent)
        links = set("https://www.revolve.com"+i for i in tree.xpath('//div[@class="u-padding-rl--lg carousel js-carousel__track"]//a/@href'))
        looks = []
        print('number of look',len(links))
        for link in tqdm(links):
            r = self.s.get(link)
            if self.parse_info(r,link):
                looks.append(self.parse_info(r,link))
        return looks
    
    def get_variants(self,pageContent):
        tree = html.fromstring(pageContent)
        return ["https://www.revolve.com"+i for i in tree.xpath('//li[@class="product-swatches__swatch js-product-swatch"]/@data-swatch-url')]


    def parse_info(self,r,url) -> dict:
        try:
            soup = BS(r.text,'html.parser')
            sku = re.search('"sku":"(.*?)"',r.text).group(1)
            tree = html.fromstring(r.text)
            title = soup.find('h1').next.strip()
            # imgs = "|".join(set(tree.xpath("//div[@class='slideshow__pager']//button/@data-image")))
            imgs = []
            for i in set(tree.xpath("//div[@class='slideshow__pager']//button/@data-image")):
                if sku in i:
                    imgs.append(i)
            imgs = "|".join(imgs)
            
            price = "".join(tree.xpath('//span[@id="retailPrice"]//text()'))
            description = ",".join(tree.xpath('//div[@id="product-details__description"]//li//text()'))
            data = {
                # sku:{
                    "product_uuid":sku,
                    "product_title":title,
                    "product_url":url,
                    "image_urls":imgs,
                    "product_description":description,
                    "product_price":price
                # }
            }
            return data
        except:
            return []


    def write_to_json(self,data:dict,looks:list):
        
        try:
            for i in looks:
                try:
                    self.product_col.insert_one(i)
                except:
                    pass                    
            uuid = data['product_uuid']
            self.outfit_col.insert_one({
                uuid:[i['product_uuid'] for i in looks]
            })

            self.product_col.insert_one(
                data
            )

        except Exception as e:
            print(e)
            print('existed')
    
    def crawl_detail(self,url,_variants=True) -> None:
        # try:
            print(url)
            r = self.s.get(url)
            data = self.parse_info(r,url)
            looks = self.complete_look(r.text)
            self.write_to_json(data,looks)

            if _variants:
                variants = self.get_variants(r.text)
                for i in variants:
                    if data['product_uuid'] not in  i:
                        r = self.s.get(i)
                        v_data = self.parse_info(r,i)
                        v_looks = self.complete_look(r.text)
                        self.write_to_json(v_data,v_looks)


    def crawl_link(self) -> None:
        url = "https://www.revolve.com/dresses/br/a8e981/?navsrc=main"
        
        r = self.s.get(url)
        tree = html.fromstring(r.text)
        
        
        # try:
        total_count = int("".join(tree.xpath('//span[@class="js-item-count"]//text()')).replace(',',''))
        total_page = math.ceil(total_count/500)
        print("total pages",total_page)
        for i in tqdm(range(1,total_page+1)):
            url = "https://www.revolve.com/r/BrandsContent.jsp?aliasURL=dresses/br/a8e981&s=c&c=Dresses&navsrc=main&pageNum={}"

            r = self.s.get(url.format(i))
            tree = html.fromstring(r.text)
            products = tree.xpath('//a[@class="js-plp-pdp-link plp__image-link plp__image-link--lg"]/@href')
            print("product per page",len(products))
            for product in tqdm(products):
                    product_url = "https://www.revolve.com"+product
                    self.crawl_detail(product_url)

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



d = Revolve()
# d.create_table()
# d.start_requests()
d.crawl_link()
d.remove_log()

