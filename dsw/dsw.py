from requests import Session
from lxml import html
import re
import json
import pandas as pd
import csv
from tqdm import tqdm

s = Session()
s.headers['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0'



all_data = []

def crawl_detail(prd_id,color_ids):
    try:
        imgs = []
        for color_id in color_ids:
            url = f"https://images.dsw.com/is/image/DSWShoes/?imageset={prd_id}_{color_id}_ss_01,{prd_id}_{color_id}_ss_02,{prd_id}_{color_id}_ss_03,{prd_id}_{color_id}_ss_04,{prd_id}_{color_id}_ss_05,{prd_id}_{color_id}_ss_06,{prd_id}_{color_id}_ss_07,{prd_id}_{color_id}_ss_08,{prd_id}_{color_id}_ss_09,{prd_id}_{color_id}_ss_010"
            r = s.get(url)
            js = re.findall('"n":"(.*?)"',r.text)
            for i in js:
                if "Image_Not_Available" not in i:
                    img_format = f"https://images.dsw.com/is/image/{i}"
                    imgs.append(img_format)

        return {"imgs":"|".join(list(set(imgs)))}
    except Exception as e:
        return ""


def crawl_link(query,row):
    path = row['Attribute'] + "/" + row['Tags'] + "/" + row['Category']
    query = query.strip()
    url = f"https://shoesandsox.com.au/search.php?page=1&section=product&search_query={query}&in_stock=1"
    totalPage = 10
    pagenum = 1
    while totalPage>=pagenum:
        json_data = {
            'widgets': [
                {
                    'id': 'list_search',
                    'type': 'search',
                },
                {
                    'id': 'content_search_header',
                    'type': 'content',
                },
                {
                    'id': 'recs_search_rv',
                    'type': 'recommendations',
                },
            ],
            'context': {
                'user': {
                    'atg_profile_id': '13008685235',
                    'rfk_uuid': '262300170-5w-hi-4c-1p-p5cmyat3atblemog1ly8-1647150809620',
                    'segments': [],
                },
                'device': {
                    'app_type': 'browser',
                    'device_type': 'desktop',
                    'app_version': '2.0.0',
                },
                'page': {
                    'referrer': '',
                },
            },
            'content': {
                'products': {
                    'page_size': 60,
                    'page_number': pagenum,
                    'fields': [
                        'id',
                        'name',
                        'category_list',
                        'gender',
                        'brand',
                        'default_sku',
                        'default_color_code',
                        'color_code',
                        'release_date',
                        'color_code_list',
                        'color_list',
                        'web_type',
                        'is_image_animated',
                        'is_any_clearance',
                        'is_show_price_in_cart',
                        'review_rating',
                        'review_count',
                        'price_min',
                        'price_max',
                        'clearance_price_min',
                        'clearance_price_max',
                        'msrp_list',
                        'stock_quantity',
                        'is_brand_logo_card_available',
                        'is_brand_logo_tile_available',
                        'price',
                        'clearance_price',
                        'fulfillment_mode',
                        'badges',
                        'is_drop_ship_none',
                    ],
                },
                'facets': {
                    'include_product_count': True,
                    'keys': {
                        'brand': {
                            'include_product_count': True,
                            'max_results': 1000,
                            'sort': {
                                'type': 'text',
                                'order': 'asc',
                            },
                        },
                        'color_family': {
                            'include_product_count': True,
                            'max_results': 20,
                            'sort': {
                                'type': 'text',
                                'order': 'asc',
                            },
                        },
                        'dimension': {
                            'include_product_count': True,
                            'max_results': 10,
                            'sort': {
                                'type': 'dimension',
                                'order': 'asc',
                            },
                        },
                        'features': {
                            'include_product_count': True,
                            'max_results': 100,
                            'sort': {
                                'type': 'text',
                                'order': 'asc',
                            },
                        },
                        'filter_size': {
                            'include_product_count': True,
                            'max_results': 100,
                            'sort': {
                                'type': 'size',
                                'order': 'asc',
                            },
                        },
                        'gender': {
                            'include_product_count': True,
                            'max_results': 10,
                            'sort': {
                                'type': 'gender',
                                'order': 'asc',
                            },
                        },
                        'heel_height': {
                            'include_product_count': True,
                            'max_results': 10,
                            'sort': {
                                'type': 'heel_height',
                                'order': 'asc',
                            },
                        },
                        'item_type': {
                            'include_product_count': True,
                            'max_results': 100,
                            'sort': {
                                'type': 'text',
                                'order': 'asc',
                            },
                        },
                        'occasion': {
                            'include_product_count': True,
                            'max_results': 100,
                            'sort': {
                                'type': 'text',
                                'order': 'asc',
                            },
                        },
                        'price_label': {
                            'include_product_count': True,
                            'max_results': 10,
                        },
                        'shaft_height': {
                            'include_product_count': True,
                            'max_results': 10,
                            'sort': {
                                'type': 'shaft_height',
                                'order': 'asc',
                            },
                        },
                        'style': {
                            'include_product_count': True,
                            'max_results': 200,
                            'sort': {
                                'type': 'text',
                                'order': 'asc',
                            },
                        },
                        'toe_shape': {
                            'include_product_count': True,
                            'max_results': 10,
                            'sort': {
                                'type': 'toe_shape',
                                'order': 'asc',
                            },
                        },
                    },
                },
                'suggestions': {
                    'keys': {
                        'brand': {
                            'max_results': 1,
                        },
                        'keyphrase': {
                            'max_results': 1,
                        },
                    },
                },
            },
            'query': {
                'keyphrase': [
                    f'{query}',
                ],
            },
        }
        params = (
            ('rfk_account', 'dswus_prod'),
            ('banner', 'dswus'),
            ('uri', f'/browse/{query}'),
        )
        try:
            r = s.post('https://api.dsw.com/content/api/v1/page',params=params,json=json_data)
            products = r.json()['widgets'][0]['products']
            if pagenum == 1:
                totalPage = products['page_count']
            for product in tqdm(products['value']):
                prd_id = product['id']
                prd_url = f"https://www.dsw.com/en/us/product/~/{prd_id}"
                color_ids = product['color_code_list']

                d = crawl_detail(prd_id,color_ids)
                output = {
                    "Retailer":'dsw',
                    "path":path,
                    "search_term":query.strip(),
                    "prd_id":prd_id,
                    "images":d["imgs"],
                    "url":prd_url
                }
                all_data.append(output)

                if len(all_data) % 20 == 0:
                    df = pd.DataFrame(all_data)
                    df.to_csv("dsw_imgs.csv",sep=",",index=False)

            pagenum+=1
        except:
            pass



def crawl_query(row):
    querys = row['Query'].split(',')
    for i in querys:
        crawl_link(i,row)


df = pd.read_excel('QueryList_v1.xlsx.xlsx')

for i in range(len(df)):
    row = df.iloc[i].to_dict()
    crawl_query(row)


df = pd.DataFrame(all_data)
df.to_csv("dsw_imgs.csv",sep=",",index=False)


