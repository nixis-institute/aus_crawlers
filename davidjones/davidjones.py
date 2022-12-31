from requests import Session
import re
import pandas as pd
import csv
import mysql.connector
from lxml import html
from tqdm import tqdm
import math

class Davidjones:
    def __init__(self) -> None:
        username = 'root'
        password = 'nixis123'
        database_name = 'crawling'
        self.retailer = 'davidjones'
        self.conn = mysql.connector.connect(host='localhost',user = username, passwd = password,db = database_name,auth_plugin='mysql_native_password')
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
        self.s.headers['host'] = 'www.davidjones.com'
        # self.s.headers['cookie'] = "visid_incap_1686039=8N5vkzXNS3ORWFyNXmJjz01hj2IAAAAAQUIPAAAAAABAuilJNqkrbU9onYZRqYdN; optimizelyEndUserId=oeu1653563729550r0.16047262158581255; _ga=GA1.2.1381498126.1653563733; _gid=GA1.2.702722311.1653563733; _gcl_au=1.1.2097389786.1653563733; _ga_7ZEZ2L98N2=GS1.1.1653627366.5.0.1653627397.29; _ga_2LJLJKYZ3R=GS1.1.1653627366.5.0.1653627397.0; FPLC=EPe7FT4LyBaix91pJ91yfBoregsV2n56oiwv58ts8kNFh5%2BTT0%2BVa4c10MdGSLFQCpNua1FQNX4vGXeu2EHXX3OHx7yLg6U7wBHZi0ZS4lvvzyplmnLqLBNa%2BJDczQ%3D%3D; FPID=FPID2.2.RRBg8f7l…r=765; IR_gbd=davidjones.com; IR_5504=1653627366747%7C406511%7C1653627366747%7C%7C; _uetsid=294f31e0dce511ecafdceda156e6071c; _uetvid=294f38e0dce511ecac6bc3b1050bf9c9; BVBRANDID=f1e2e0bc-02aa-4ab7-80ca-a3bc1e636a0e; BVBRANDSID=472d84b3-e06e-408c-bf52-6bedac2c2665; _gat=1; _gat_UA-489931-23=1; _gat_UA-489931-18=1; _hjIncludedInSessionSample=0; _hjSession_873034=eyJpZCI6IjEzMzlhMTY2LTc5MjAtNGYzZS04ODg4LWRhNzljMzdmNTE0OSIsImNyZWF0ZWQiOjE2NTM2MjczNjkzMzYsImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=1".encode('utf-8')
        self.cookies = {
            'visid_incap_1686039': '8N5vkzXNS3ORWFyNXmJjz01hj2IAAAAAQkIPAAAAAACACYqkAWfw7xT4u3ZvynwbahHNRike8+EQ',
            'optimizelyEndUserId': 'oeu1653563729550r0.16047262158581255',
            '_ga': 'GA1.2.1381498126.1653563733',
            '_gid': 'GA1.2.702722311.1653563733',
            '_gcl_au': '1.1.2097389786.1653563733',
            '_ga_7ZEZ2L98N2': 'GS1.1.1653633437.6.1.1653633454.43',
            '_ga_2LJLJKYZ3R': 'GS1.1.1653633437.6.1.1653633454.0',
            'FPLC': 'EPe7FT4LyBaix91pJ91yfBoregsV2n56oiwv58ts8kNFh5%2BTT0%2BVa4c10MdGSLFQCpNua1FQNX4vGXeu2EHXX3OHx7yLg6U7wBHZi0ZS4lvvzyplmnLqLBNa%2BJDczQ%3D%3D',
            'FPID': 'FPID2.2.RRBg8f7lDyMKi6qDdPMV%2FEbpuEM1%2FzA0lXWtCg4DRxM%3D.1653563733',
            'NoCookie': 'true',
            '_pin_unauth': 'dWlkPU9EUTVZekZqWVRrdE5tWmpOUzAwWldWakxXSTRZekV0TldGak5UUTNZVGc1TjJJNA',
            'IR_PI': '29e5a587-dce5-11ec-8630-d1eb97e26cf1%7C1653719853428',
            'inside-au': '1098575643-f01db6465d5efa01d14b6ea6618a2b8a2eed15135a27cdb47d9c7fdf728ad3b7-0-0',
            '_hjSessionUser_873034': 'eyJpZCI6Ijc1ZDA1NjI1LTUzZDYtNTAzNC1hZTc4LTgzYWM4NTNiN2M5NyIsImNyZWF0ZWQiOjE2NTM1NjM3MzY2NzAsImV4aXN0aW5nIjp0cnVlfQ==',
            '_clck': '1trt4xn|1|f1t|0',
            '_fbp': 'fb.1.1653563737208.1301971781',
            '_y2': '1%3AeyJjIjp7IjE0NjY2NSI6LTE0NzM5ODQwMDAsIjE0NjY3MiI6LTE0NzM5ODQwMDAsIjE1NTM4NiI6LTE0NzM5ODQwMDAsIjE1ODEzMSI6LTE0NzM5ODQwMDAsIjE2Mzg4NCI6LTE0NzM5ODQwMDAsIjE2NDQ3MiI6LTE0NzM5ODQwMDAsIjE2NDQ3NSI6LTE0NzM5ODQwMDAsIjE2NjQ5OCI6LTE0NzM5ODQwMDAsIjE2ODI2NSI6LTE0NzM5ODQwMDAsIjE3MTg3NSI6LTE0NzM5ODQwMDAsIjE3NjcwNCI6LTE0NzM5ODQwMDB9fQ%3D%3D%3ALTE5NjU3ODQwMA%3D%3D%3A99',
            '_yi': '1%3AeyJsaSI6bnVsbCwic2UiOnsiYyI6NCwiZWMiOjQxLCJsYSI6MTY1MzYzMzQ1OTkxOCwicCI6NSwic2MiOjQ3NDl9LCJ1Ijp7ImlkIjoiOTFhOGY3ZDYtMDBjOC00YzIxLThlZmQtMGI0NmU3ZTQ4NDU5IiwiZmwiOiIwIn19%3ALTE0MzE4NDYxMTI%3D%3A99',
            '_clsk': 'l5zquz|1653633460151|3|0|h.clarity.ms/collect',
            'ASP.NET_SessionId': 'x1nb3sp54uozadomfzdeovdz',
            'iSAMS': 'jtgbQIFHzKCCi3GWB+8k2c7X0Zx/iWXVgxNbHDGPIPBsUqVzH4MQQcQaQzMAOAR82XYe+8UrOF4wjM50mQdnsw==',
            'incap_ses_746_1686039': 'zTxUdhVKexXNROF6sVNaCpxxkGIAAAAAoZhk+9g+iRSZj9eKffZFCw==',
            'fpGUID': '6c7801aa-9459-4291-a05d-9337e3a2b038',
            'fpFingerprint': '6c6c66567b7d3dd0fdb33469e2b70e2a54be7dd2046eb7a9f56c4811d62d7182',
            'run_fs_for_user': '765',
            'IR_gbd': 'davidjones.com',
            'IR_5504': '1653633453428%7C406511%7C1653633453428%7C%7C',
            '_gat': '1',
            '_gat_UA-489931-23': '1',
            '_gat_UA-489931-18': '1',
            '_hjIncludedInSessionSample': '0',
            '_hjSession_873034': 'eyJpZCI6ImQxZThhZDg4LWQ0MTAtNGFiMi1hZDA0LWQyOTI3ZWEzZWE0MSIsImNyZWF0ZWQiOjE2NTM2MzM0NDg0NDEsImluU2FtcGxlIjpmYWxzZX0=',
            '_hjAbsoluteSessionInProgress': '0',
            'BVBRANDID': '1320afa3-87d2-4a83-a2e7-dc28eb87c778',
            'BVBRANDSID': 'a1f76d71-e41f-4074-9193-48febc24d095',
            '_uetsid': '294f31e0dce511ecafdceda156e6071c',
            '_uetvid': '294f38e0dce511ecac6bc3b1050bf9c9',
        }



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
            r = self.s.get(url,cookies=self.cookies)
            tree = html.fromstring(r.text)
            imgs = []
            for i in tree.xpath('//img[@loading="lazy"]/@src'):
                if '/productthumb/' in i:
                    imgs.append("https://www.davidjones.com"+i.replace('/productthumb/','/magnify/'))
            imgs = "|".join(imgs)
            sku = "".join(tree.xpath('//div[@itemscope="itemscope"]/@data-product-sku'))
            
            return {"imgs":imgs,"sku":sku}
        except Exception as e:
            return {}


    def crawl_link(self,query,row) -> None:
        path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
        query = query.strip()
        # url = 'https://www.davidjones.com/women/clothing/{}?src=fh&size=90&offset={}'
        url = 'https://www.davidjones.com/search?q={}&size=90&offset={}'
        
        r = self.s.get(url.format(query,1),cookies=self.cookies)
        tree = html.fromstring(r.text)
        
        
        # try:
        # print("ds","".join(tree.xpath('//span[@class="search-numbers"]//text()')))
        try:
            total_count = int("".join(tree.xpath('//span[@class="search-numbers"]//text()')))
        except:
            total_count = 0
        total_page = math.ceil(total_count/20)
        print(query,total_count)
        # url = 'https://www.autographfashion.com.au/ProductList/Search?FF=&P.StartPage=1&P.LoadToPage={}&Query={}&sorting=Relevance'
        
        # r = self.s.get(url.format(total_page,query))
        # tree = html.fromstring(r.text)
        # products = tree.xpath("//a[@class='product-detail']/@href")

        for i in tqdm(range(0,total_count,90)):

            r = self.s.get(url.format(query,i),cookies=self.cookies)
            tree = html.fromstring(r.text)
            products = tree.xpath('//h4//a/@href')
            for product in tqdm(products):
                # try:
                    product_url = product
                    

                    d = self.crawl_detail(product_url)
                    if d:
                        output = {
                            "Retailer":self.retailer,
                            "path":path,
                            "search_term":query.strip(),
                            "prd_id":d['sku'],
                            "images":d['imgs'],
                            "url":product_url
                        }
                        self.o_row.writerow(list(output.values()))
                    # except Exception as e:
                    #     pass

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

d = Davidjones()
d.create_table()
d.start_requests()
d.remove_log()