from requests import Session
import re
import pandas as pd
import csv
import mysql.connector
from lxml import html
from tqdm import tqdm
import math

class Mollini:
    def __init__(self) -> None:
        username = 'root'
        password = 'nixis123'
        database_name = 'crawling'
        self.retailer = 'williamsshoes'
        self.conn = mysql.connector.connect(host='localhost',user = username, passwd = password,db = database_name)
        self.cursor = self.conn.cursor(dictionary=True)        
        self.input_file = 'query_autographfashion_footwear.xlsx'
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
        self.cookies = {
            '__uzma': '9a7bc10b-54c2-491a-b523-5460bbea91c2',
            '__uzmb': '1652809165',
            '__uzmc': '840183110256',
            '__uzmd': '1652873758',
            '__ssds': '3',
            '_privy_358A877C4F7EE95C615EF9C3': '%7B%22uuid%22%3A%22d7fcf4df-e26b-486e-badd-cf30ab4da4a4%22%7D',
            '__ssuzjsr3': 'a9be0cd8e',
            '__uzmaj3': '78a59d3a-daca-4556-b488-8a738ffa8b1e',
            '__uzmbj3': '1652809244',
            '__uzmcj3': '124331960535',
            '__uzmdj3': '1652873763',
            'cd_user_id': '180d31ba2c52d-06a350087e5a658-402e2c34-fa000-180d31ba2c6bd3',
            '_ga': 'GA1.3.1771607092.1652809246',
            '_gid': 'GA1.3.511670969.1652809246',
            '_fw_crm_v': '51e5b88f-3997-4dab-bb94-d9b389c352b8',
            '_tt_enable_cookie': '1',
            '_ttp': 'f4b25b94-47b7-49d4-be89-ad98ca9b047d',
            'PHPSESSID': '3pd6a6nbm8trtdjup064nabgpj',
            'private_content_version': '18ae72cb42d540f3f9a09ba9de43eaf2',
            'X-Magento-Vary': '97d170e1550eee4afc0af065b78cda302a97674c',
            'scarab.visitor': '%2253538A49CACE8AF9%22',
            'firstVisit': 'true',
            'form_key': 'gdg7Tc7q1djPX3Vd',
            'mage-cache-storage': '%7B%7D',
            'mage-cache-storage-section-invalidation': '%7B%7D',
            'mage-cache-sessid': 'true',
            'mage-messages': 'null',
            'recently_viewed_product': '%7B%7D',
            'recently_viewed_product_previous': '%7B%7D',
            'recently_compared_product': '%7B%7D',
            'recently_compared_product_previous': '%7B%7D',
            'product_data_storage': '%7B%7D',
            'section_data_ids': '%7B%22messages%22%3A1652809317%2C%22directory-data%22%3A1652809316%7D',
            'form_key': 'gdg7Tc7q1djPX3Vd',
            'visited': 'true',
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:100.0) Gecko/20100101 Firefox/100.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            # Requests sorts cookies= alphabetically
            # 'Cookie': '__uzma=9a7bc10b-54c2-491a-b523-5460bbea91c2; __uzmb=1652809165; __uzmc=103762817986; __uzmd=1652873470; __ssds=3; _privy_358A877C4F7EE95C615EF9C3=%7B%22uuid%22%3A%22d7fcf4df-e26b-486e-badd-cf30ab4da4a4%22%7D; __ssuzjsr3=a9be0cd8e; __uzmaj3=78a59d3a-daca-4556-b488-8a738ffa8b1e; __uzmbj3=1652809244; __uzmcj3=722061675928; __uzmdj3=1652873474; cd_user_id=180d31ba2c52d-06a350087e5a658-402e2c34-fa000-180d31ba2c6bd3; _ga=GA1.3.1771607092.1652809246; _gid=GA1.3.511670969.1652809246; _fw_crm_v=51e5b88f-3997-4dab-bb94-d9b389c352b8; _tt_enable_cookie=1; _ttp=f4b25b94-47b7-49d4-be89-ad98ca9b047d; PHPSESSID=3pd6a6nbm8trtdjup064nabgpj; private_content_version=92a1387181db382880d2d7287256162d; X-Magento-Vary=97d170e1550eee4afc0af065b78cda302a97674c; scarab.visitor=%2253538A49CACE8AF9%22; firstVisit=true; form_key=gdg7Tc7q1djPX3Vd; mage-cache-storage=%7B%7D; mage-cache-storage-section-invalidation=%7B%7D; mage-cache-sessid=true; mage-messages=null; recently_viewed_product=%7B%7D; recently_viewed_product_previous=%7B%7D; recently_compared_product=%7B%7D; recently_compared_product_previous=%7B%7D; product_data_storage=%7B%7D; section_data_ids=%7B%22messages%22%3A1652809317%2C%22directory-data%22%3A1652809316%7D; form_key=gdg7Tc7q1djPX3Vd; visited=true',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
        }        

        self.s.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'
        self.s.headers.update(headers)
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
            r = self.s.get(url,cookies = self.cookies)
            tree = html.fromstring(r.text)
            imgs = "|".join([i.replace('\\','') for i in re.findall('"full":"(.*?)"',r.text)])
            sku = re.search('"ProductID": "(.*?)"',r.text).group(1)
            
            return {"imgs":imgs,"sku":sku}
        except Exception as e:
            return {}


    def crawl_link(self,query,row) -> None:
        path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
        query = query.strip()
        url = "https://www.williamsshoes.com.au/catalogsearch/result/?q={}&page={}"
        
        r = self.s.get(url.format(query,1),cookies = self.cookies)
        tree = html.fromstring(r.text)
        try:
            total_count = int("".join(set(tree.xpath('//div[@class="toolbar__prod-count prod-count-wrapper"]//text()'))).split()[0])        
            total_page = math.ceil(total_count/48)
            print(query,total_count)
            for i in tqdm(range(1,total_page+1)):

                r = self.s.get(url.format(query,1),cookies = self.cookies)
                tree = html.fromstring(r.text)
                products = tree.xpath('//h2[@class="product-tile__title"]//a/@href')
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
        except Exception as e:
            pass
            print(e)

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

d = Mollini()
d.create_table()
d.start_requests()
d.remove_log()