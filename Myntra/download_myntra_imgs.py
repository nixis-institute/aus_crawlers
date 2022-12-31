import pandas as pd
import os
from glob import glob
from distutils.dir_util import copy_tree


def copy_folder(source,destination,num):
    retailer = destination['Retailer']
    path = destination['path']
    search_term = destination['search_term']
    s_retailer = source['Retailer']
    s_path = source['path']
    s_search_term = source['search_term']    

    try:
        prd_name = destination['prd_id']
    except:
        prd_name = f'{num}'
    try:
        os.mkdir(retailer)
    except:
        pass
    try:
        os.mkdir(retailer+"/"+path+"/"+search_term)
    except:
        pass

    try:
        os.mkdir(retailer+"/"+path+"/"+search_term+"/"+prd_name)
    except:
        pass  

    path_to_img = f"{s_retailer}/{s_path}/{s_search_term}"
    for g in glob(path_to_img+"/*"):
        pr_name = source['prd_id']
        if(pr_name in g):
            download_img_path = g

    des_img_path = f'{retailer}/{path}/{search_term}/{prd_name}'
    print(des_img_path)
    copy_tree(download_img_path,des_img_path)

def _download(dic):
    row = dic[0]
    num = dic[1]
    try:
        if row["images"]:
            retailer = row['Retailer']
            path = row['path']
            search_term = row['search_term']
            try:
                prd_name = row['prd_id']
            except:
                prd_name = f'{num}'
            images = row['images'].split("|")

            try:
                os.mkdir(retailer)
            except:
                pass

            try:
                os.mkdir(retailer+"/"+path+"/"+search_term)
            except:
                pass

            try:
                os.mkdir(retailer+"/"+path+"/"+search_term+"/"+prd_name)
            except:
                pass    

            img_download = None
            path_to_img = f"{retailer}/{path}/{search_term}"
            for g in glob(path_to_img+"/*"):
                pr_name = row['prd_id']
                if(pr_name in g and glob(g+"/*")):
                    img_download = True                    


            if(img_download is None):
                for i in images:
                    print(i)
                    img_path = f'{retailer}/{path}/{search_term}/{prd_name}/{i.split("/")[-1]}.jpeg'
                    print(f"\n{img_path}")
                    os.system(f'wget -O "{img_path}" {i}')


    except Exception as e:
        print("error",e)

df = pd.read_csv('myntra_imgs.csv',sep=",",index_col=False)
df = df.astype(str)

for i in range(len(df)):
    row = df.iloc[i].to_dict()
    try:
        p_name = row['prd_id']
        df1 = df.head(i)    
        copy_folder(df1[df1['prd_id'] == p_name].iloc[0].to_dict(),row,i+1)

    except:
        _download((row,i+1))


