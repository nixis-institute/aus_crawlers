from cmath import exp
from requests import Session
from lxml import html
import re
import json
import pandas as pd


s = Session()
s.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'

all_data = []



def crawl_detail(url):
    try:
        r = s.get(url)
        # js = json.loads(re.search('window.__INITIAL_STATE__ = (.*?);</scr',r.text).group(1))
        # imgs = re.findall('background-image:url\((.*?)\)',r.text)
        imgs = "|".join([i.replace('{@width}','900').replace('{@height}','600').replace('?q={@quality}','')  for i in re.findall('"IMAGE","source":"ORGANIC","url":"(.*?)"',r.text)])
        try:
            name = re.search('"newTitle":"(.*?)"',r.text).group(1)
        except:
            name = url
        return {
            "name":name,
            "images":imgs
        }
    except:
        return{
            "name":"",
            "images":""
        }




def crawl_link(query,row):
    path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
    url = f"https://www.flipkart.com/search?q={query.strip()}"
    while url:
        try:
            r = s.get(url)
            tree = html.fromstring(r.text)
            links = tree.xpath("//a[@class='_2UzuFa']/@href")
            for i in links:
                detail_url = "https://www.flipkart.com"+i
                d = crawl_detail(detail_url)
                output = {
                    "Retailer":'Flipkart',
                    "path":path,
                    "search_term":query.strip(),
                    "prd_id":re.search('/p/(.*?)\?',i).group(1),
                    "images":d['images'],
                    "url":detail_url
                }
                print(output)
                all_data.append(output)
            rel_url = "".join(tree.xpath('//a[@class="_1LKTO3"]//span[contains(string(),"Next")]//ancestor::a/@href'))
            if(rel_url):
                url = "https://www.flipkart.com"+rel_url
            else:
                url = False
        except:
            pass



def crawl_query(row):
    querys = row['Query'].split(',')
    # for query in querys:
    for i in querys:
        # query = 
        crawl_link(i,row)


df = pd.read_excel('Query_Coats_and_Jackets.xlsx')
for i in range(len(df)):
    row = df.iloc[i].to_dict()
    crawl_query(row)


df = pd.DataFrame(all_data)
df.to_csv('flipkart_imgs_coats_jackets.csv',index=False)


