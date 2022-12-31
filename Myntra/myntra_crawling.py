from requests import Session
import pandas as pd
import requests
s = Session()

all_data = []
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:97.0) Gecko/20100101 Firefox/97.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'cross-site',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'TE': 'trailers',
}

params = (
    ('p', '3'),
    ('rows', '50'),
    ('o', '99'),
    ('plaEnabled', 'false'),
)
def pagination_page(url,query):
    try:
        m_url = url.format(query)
        # r = s.get(m_url)
        params = (
        ('p', '1'),
        ('rows', '50'),
        ('o', '99'),
        ('plaEnabled', 'false'),)
        r = requests.get(m_url, headers=headers, params=params, cookies=cookie)
        js = r.json()
        pages = js.get("totalCount")
        pages = int((pages/100)+1)
        return pages
    except Exception as e:
        print(e)
        pass




def crawl_link_imgs(query,row):
    try:
        path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
        url = "https://www.myntra.com/gateway/v2/search/{}"
        print("Searhc term :- {}".format(query))
        pages = pagination_page(url,query)
        print("Total pages for Search term :- {}".format(pages))
        for page in range(1,pages):
            params = (
                ('p', '{}'.format(page)),
                ('rows', '100'),
                ('o', '99'),
                ('plaEnabled', 'false'),
            )
            r = requests.get(url.format(query), headers=headers, params=params, cookies=cookie)
            js = r.json()
            products = js.get("products")
            for prod in products:
                all_images = []
                for img in prod.get("images"):
                    all_images.append(img.get("src"))
                output = {
                    "Retailer":'Myntra',
                    "path":path,
                    "search_term":query,
                    "prd_id":prod.get("productId"),
                    "prd_name":prod.get("productName"),
                    "images":'|'.join(all_images),
                    "url":"https://www.myntra.com/dresses/sangria"+prod.get("landingPageUrl")
                }
                print(output)
                all_data.append(output)
    except Exception as e:
        print(e)
        pass


def crawl_query(row):
    querys = row['Query'].split(',')
    for query in querys:
        crawl_link_imgs(query,row)


url = "https://www.myntra.com/womens-mini-dresses?p=2"
ss = Session()
ss.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'
res = ss.get(url)
cookie = res.cookies.get_dict()

df = pd.read_excel('Query_Coats_and_Jackets.xlsx')
for i in range(len(df)):
    row = df.iloc[i].to_dict()
    crawl_query(row)

df = pd.DataFrame(all_data)
df.to_csv('myntra_imgs_coats_jackets.csv',index=False)