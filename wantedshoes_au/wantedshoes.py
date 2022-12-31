from requests import Session
import re
import pandas as pd
import csv
import mysql.connector
from lxml import html
from tqdm import tqdm
import math

class Wantedshoes:
    def __init__(self) -> None:
        username = 'root'
        password = 'nixis123'
        database_name = 'crawling'
        self.retailer = 'wantedshoes'
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
        self.cookies = {
            '__uzma': '38ea7f0e-807d-4b8c-8807-124dd59fd412',
            '__uzmb': '1652783303',
            '__uzmc': '510361380255',
            '__uzmd': '1652783303',
            '__ssds': '3',
            'visited': 'true',
            '__ssuzjsr3': 'a9be0cd8e',
            '__uzmaj3': '04f71a3d-743d-4779-ba4f-10916cc665bc',
            '__uzmbj3': '1652783307',
            '__uzmcj3': '770871091157',
            '__uzmdj3': '1652783307',
            'PHPSESSID': 'v1gpu1d6l98gpmk6lb9t1j7sjp',
            'private_content_version': 'ae1c4efd4934edcce3ddf0672991fae6',
            'X-Magento-Vary': '97d170e1550eee4afc0af065b78cda302a97674c',
            'scarab.visitor': '%2253538A49CACE8AF9%22',
            'firstVisit': 'true',
            'cd_user_id': '180d18fe3c363-036b6c1694db2-402e2c34-fa000-180d18fe3c4f6',
            'form_key': 'UTZRXpE9kvzLGDMo',
            'mage-cache-storage': '%7B%7D',
            'mage-cache-storage-section-invalidation': '%7B%7D',
            'mage-cache-sessid': 'true',
            'mage-messages': 'null',
            'recently_viewed_product': '%7B%7D',
            'recently_viewed_product_previous': '%7B%7D',
            'recently_compared_product': '%7B%7D',
            'recently_compared_product_previous': '%7B%7D',
            'product_data_storage': '%7B%7D',
            'form_key': 'UTZRXpE9kvzLGDMo',
            'GUEST_WISHLIST': '%7B%22wishlist_id%22%3A%2265557874%22%7D',
            'cto_bundle': '04Kefl9ockhsRUMxNFpiNWF2UlpiZENmMGI2WElYNjVtMUVxYlNWTmFqU1dOVm5ZUTdzcnlrQiUyQlRwMkREUzM5REpwQzBtTGMzMlM0JTJCRHl6UzhpWkl5S3JhMFczU3dOdXVaZ2w0akElMkJ0Q2Q4WUUwOGI0UnM4ejJkOHdtSjFhaHR5MjVLdGl5SXlzOWpsdmRNV1d1NDl1NnR5VVhNdzZ4S1EyVEQ3WjBHOGNyOVl1UTM5QVp3cUJ5MG9ac1RPV3ZGMSUyRktwRU40V2dKMyUyRlElMkJDTlk4azZ5JTJCelVJV1ElM0QlM0Q',
            'section_data_ids': '%7B%22directory-data%22%3A1652783310%2C%22messages%22%3A1652783330%2C%22customer%22%3A1652783310%2C%22compare-products%22%3A1652783310%2C%22last-ordered-items%22%3A1652783310%2C%22cart%22%3A1652783310%2C%22captcha%22%3A1652783310%2C%22wishlist%22%3A1652783310%2C%22instant-purchase%22%3A1652783310%2C%22loggedAsCustomer%22%3A1652783310%2C%22multiplewishlist%22%3A1652783310%2C%22persistent%22%3A1652783310%2C%22review%22%3A1652783310%2C%22top-line%22%3A1652783310%2C%22recently_viewed_product%22%3A1652783310%2C%22recently_compared_product%22%3A1652783310%2C%22product_data_storage%22%3A1652783310%2C%22paypal-billing-agreement%22%3A1652783310%7D',
            '_ga': 'GA1.3.1013590607.1652783311',
            '_gid': 'GA1.3.372554123.1652783311',
            '_gat_UA-2219099-1': '1',
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:100.0) Gecko/20100101 Firefox/100.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.wantedshoes.com.au/',
            # Requests sorts cookies= alphabetically
            # 'Cookie': '__uzma=38ea7f0e-807d-4b8c-8807-124dd59fd412; __uzmb=1652783303; __uzmc=510361380255; __uzmd=1652783303; __ssds=3; visited=true; __ssuzjsr3=a9be0cd8e; __uzmaj3=04f71a3d-743d-4779-ba4f-10916cc665bc; __uzmbj3=1652783307; __uzmcj3=770871091157; __uzmdj3=1652783307; PHPSESSID=v1gpu1d6l98gpmk6lb9t1j7sjp; private_content_version=ae1c4efd4934edcce3ddf0672991fae6; X-Magento-Vary=97d170e1550eee4afc0af065b78cda302a97674c; scarab.visitor=%2253538A49CACE8AF9%22; firstVisit=true; cd_user_id=180d18fe3c363-036b6c1694db2-402e2c34-fa000-180d18fe3c4f6; form_key=UTZRXpE9kvzLGDMo; mage-cache-storage=%7B%7D; mage-cache-storage-section-invalidation=%7B%7D; mage-cache-sessid=true; mage-messages=null; recently_viewed_product=%7B%7D; recently_viewed_product_previous=%7B%7D; recently_compared_product=%7B%7D; recently_compared_product_previous=%7B%7D; product_data_storage=%7B%7D; form_key=UTZRXpE9kvzLGDMo; GUEST_WISHLIST=%7B%22wishlist_id%22%3A%2265557874%22%7D; cto_bundle=04Kefl9ockhsRUMxNFpiNWF2UlpiZENmMGI2WElYNjVtMUVxYlNWTmFqU1dOVm5ZUTdzcnlrQiUyQlRwMkREUzM5REpwQzBtTGMzMlM0JTJCRHl6UzhpWkl5S3JhMFczU3dOdXVaZ2w0akElMkJ0Q2Q4WUUwOGI0UnM4ejJkOHdtSjFhaHR5MjVLdGl5SXlzOWpsdmRNV1d1NDl1NnR5VVhNdzZ4S1EyVEQ3WjBHOGNyOVl1UTM5QVp3cUJ5MG9ac1RPV3ZGMSUyRktwRU40V2dKMyUyRlElMkJDTlk4azZ5JTJCelVJV1ElM0QlM0Q; section_data_ids=%7B%22directory-data%22%3A1652783310%2C%22messages%22%3A1652783330%2C%22customer%22%3A1652783310%2C%22compare-products%22%3A1652783310%2C%22last-ordered-items%22%3A1652783310%2C%22cart%22%3A1652783310%2C%22captcha%22%3A1652783310%2C%22wishlist%22%3A1652783310%2C%22instant-purchase%22%3A1652783310%2C%22loggedAsCustomer%22%3A1652783310%2C%22multiplewishlist%22%3A1652783310%2C%22persistent%22%3A1652783310%2C%22review%22%3A1652783310%2C%22top-line%22%3A1652783310%2C%22recently_viewed_product%22%3A1652783310%2C%22recently_compared_product%22%3A1652783310%2C%22product_data_storage%22%3A1652783310%2C%22paypal-billing-agreement%22%3A1652783310%7D; _ga=GA1.3.1013590607.1652783311; _gid=GA1.3.372554123.1652783311; _gat_UA-2219099-1=1',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            # Requests doesn't support trailers
            # 'TE': 'trailers',
        }    
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
        url = "https://www.wantedshoes.com.au/catalogsearch/result/index/?p={}&q={}"
        
        r = self.s.get(url.format(1,query),cookies = self.cookies)
        tree = html.fromstring(r.text)
        try:
            total_count = int("".join(set(tree.xpath('//div[@class="toolbar__prod-count prod-count-wrapper"]//text()'))).split()[0])        
            total_page = math.ceil(total_count/48)
            print(query,total_count)
            for i in tqdm(range(1,total_page+1)):

                r = self.s.get(url.format(i,query),cookies = self.cookies)
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
            # print(e)

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

d = Wantedshoes()
d.create_table()
d.start_requests()
d.remove_log()