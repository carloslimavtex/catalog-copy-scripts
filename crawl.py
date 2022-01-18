#!/usr/bin/python3

import requests
import json

# SOURCE ACCOUNT FOR READING
# FOR SOURCE ACCOUNT sourceaccount.myvtex.com USE source_account = "sourceaccount"
source_account = "sourceaccount"

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

## INSERT YOUR VTEX ID CLIENT AUT COOKIE HERE
cookies = {
    "VtexIdclientAutCookie": ""
}

def read_api_as_JSON(url, params=""):
    if (params != ""):
        print('params='+json.dumps(params))
    response = requests.request("GET", url, headers=headers, cookies=cookies, params=params)
    if (response.status_code == 200):
        return json.loads(response.text)
    else:
        print("Status Code="+str(response.status_code))
        return False

def print_category(cat):
    print(
        "Cat ID="+str(cat['id']) + 
        "|"+cat['name']+
        "|"+str(cat['hasChildren'])+
        "|"+cat['url']+
        "|"+cat['Title'])

def print_brand(brand):
        print('Brand='+str(brand['id'])+
        "|"+brand['name'])

def print_product(dict_product):
    for key in dict_product:
        print(key+"|"+str(dict_product[key]))
        
# READ SOURCE BRANDS FROM API AND SAVE TO FILE
print("==SOURCE BRANDS===============================")
json_source_brands = read_api_as_JSON("https://"+source_account+".vtexcommercestable.com.br/api/catalog_system/pvt/brand/list")

if json_source_brands:
    with open('./'+source_account+'-source_brands.outputfile.json', 'w') as fout:
        json.dump(json_source_brands, fout)
    for source_brand in json_source_brands:
        print_brand(source_brand)

# READ SOURCE CATEGORIES FROM API AND SAVE TO FILE
print("==SOURCE CATEGORIES===============================")
json_source_categories = read_api_as_JSON("https://"+source_account+".vtexcommercestable.com.br/api/catalog_system/pub/category/tree/999")

if json_source_categories:
    working_copy_categories = json_source_categories.copy()
    needs_to_reprocess_tree = True

    while needs_to_reprocess_tree:
        needs_to_reprocess_tree = False
        next_iteration_categories = []
        for category_node in working_copy_categories:
            current_category_id = category_node['id']
            if 'children' in category_node:
                next_iteration_categories.append(category_node)
                for children_node in category_node['children']:
                    needs_to_reprocess_tree = True
                    children_node['FatherCategoryId'] = current_category_id
                    next_iteration_categories.append(children_node)
                    category_node.pop('children',None)
            else:
                next_iteration_categories.append(category_node)
        working_copy_categories = next_iteration_categories.copy()

    with open('./'+source_account+'-source_categories.outputfile.json', 'w') as fout1:
        json.dump(working_copy_categories, fout1)

print("==SOURCE PRODUCTS AND SKUS IDS BY CATEGORY=================")
# CHANGE categoryId TO READ FROM A SPECIFIC CATEGORY
json_source_product_x_sku_ids = read_api_as_JSON("https://"+source_account+".vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds?_from=1&_to=50&categoryId=1001348")

all_source_products = []
all_source_skus = []
all_source_sku_images = {}
all_source_sku_prices = {}

if json_source_product_x_sku_ids['data']:
    count_all_products = len(json_source_product_x_sku_ids['data'])
    pos_current_product = 0

    for product_with_sku_ids in json_source_product_x_sku_ids['data']:
        print("PRODUCT ID=" + str(product_with_sku_ids)+" WITH THE FOLLOWING SKU(S):" + " *** {:.0%}".format(pos_current_product/count_all_products))
        json_source_product = read_api_as_JSON("https://"+source_account+".vtexcommercestable.com.br/api/catalog/pvt/product/"+str(product_with_sku_ids))
        all_source_products.append(json_source_product)
        for sku_id in json_source_product_x_sku_ids['data'][product_with_sku_ids]:
            json_source_sku = read_api_as_JSON("https://"+source_account+".vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/"+str(sku_id))
            all_source_skus.append(json_source_sku)
            json_source_sku_images = read_api_as_JSON("https://"+source_account+".vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/"+str(sku_id)+"/file")
            if json_source_sku_images:
                for sku_image in json_source_sku_images:
                    sku_image['Url'] = 'https://'+source_account+'.vteximg.com.br/arquivos/ids/'+str(sku_image['ArchiveId'])+'/'+str(sku_image['Name'])+'.jpg'
                all_source_sku_images[sku_id] = json_source_sku_images
                print("SKU ID="+str(sku_id)+" WITH "+str(len(json_source_sku_images))+" IMAGE(S)")
            else:
                print("SKU ID="+str(sku_id)+" WITH NO IMAGE")
            json_source_sku_prices = read_api_as_JSON("https://"+source_account+".vtexcommercestable.com.br/api/pricing/prices/"+str(sku_id))
            all_source_sku_prices[sku_id] = json_source_sku_prices
        pos_current_product = pos_current_product + 1

with open('./'+source_account+'-source_products.outputfile.json', 'w') as fout1:
    json.dump(all_source_products, fout1)
with open('./'+source_account+'-source_skus.outputfile.json', 'w') as fout2:
    json.dump(all_source_skus, fout2)
with open('./'+source_account+'-source_sku_images.outputfile.json', 'w') as fout3:
    json.dump(all_source_sku_images, fout3)
with open('./'+source_account+'-source_sku_prices.outputfile.json', 'w') as fout4:
    json.dump(all_source_sku_prices, fout4)