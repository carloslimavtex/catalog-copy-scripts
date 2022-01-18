#!/usr/bin/python3

import requests
import json
import sys

#####################
# CONFIGURATION BLOCK
#####################
replicate_to_account = 'destinationaccount'
source_files_prefix = 'sourceaccount'
verbose_mode = True

do_brand_step = True
do_category_step = True
do_product_step = True
do_sku_step = True
do_clean_files = True
do_stock_and_price = True

## SAFETY MEASURE... WILL ONLY WRITE TO AN ACCOUNT IF IT IS IN THE WHITELIST BELOW... JUST IN CASE...
destination_whitelist = ["safetowriteaccount"]

## INSERT YOUR VTEX ID CLIENT AUT COOKIE HERE
cookies = {
    "VtexIdclientAutCookie": ""
}

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

def read_api_as_JSON(url, params=""):
    if verbose_mode:
        print('[INFO] READ URL='+url)
        if (params != ""):
            print('[INFO] params='+json.dumps(params))
    response = requests.request("GET", url, headers=headers, cookies=cookies, params=params)
    if (response.status_code == 200):
        return json.loads(response.text)
    else:
        print("[ERROR] Status Code="+str(response.status_code))
        print("[ERROR] RESPONSE="+json.dumps(response.text))
        return False

def send_DELETE_to_api(url, payload=""):
    if verbose_mode:
        print('[INFO] DELETE URL='+url)
        print('[INFO] payload='+json.dumps(payload))
    response = requests.request("DELETE", url, json=payload,headers=headers, cookies=cookies)
    if (response.status_code == 200):
        try:
            return json.loads(response.text)
        except:
            return False
    else:
        print('[ERROR] DELETE URL='+url)
        print("[ERROR] Status Code="+str(response.status_code))
        print("[ERROR] Result="+response.text)
        return False

def write_JSON_to_api(url, payload=""):
    if verbose_mode:
        print('[INFO] WRITE URL='+url)
        print('[INFO] payload='+json.dumps(payload))
    response = requests.request("POST", url, json=payload,headers=headers, cookies=cookies)
    if (response.status_code == 200):
        return json.loads(response.text)
    else:
        print('[ERROR] WRITE URL='+url)
        print("[ERROR] Status Code="+str(response.status_code))
        print("[ERROR] Result="+response.text)
        return False

def update_JSON_to_api(url, payload=""):
    if verbose_mode:
        print('[INFO] UPDATE URL='+url)
        print('[INFO] payload='+json.dumps(payload))
    response = requests.request("PUT", url, json=payload,headers=headers, cookies=cookies)
    if (response.status_code == 200):
        if response.text:
            return json.loads(response.text)
        else:
            return "(empty response)"
    else:
        print('[ERROR] UPDATE URL='+url)
        print("[ERROR] Status Code="+str(response.status_code))
        print("[ERROR] Result="+response.text)
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

####################
# 00 - PROCESS START
####################

if replicate_to_account not in destination_whitelist:
    print("Sorry, destination account '"+replicate_to_account+"' is not WHITELISTED. Please add it to the source code.")
    sys.exit()
else:
    print("SOURCE FILES PREFIX="+source_files_prefix)
    print("DESTINATION ACCOUNT="+replicate_to_account)

sample_data_from_destination = read_api_as_JSON("https://"+replicate_to_account+".vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds")
if sample_data_from_destination:
    if verbose_mode:
        print("[INFO] TOKEN VALIDATED ON DESTINATION ACCOUNT: "+replicate_to_account)
else:
    print("[ERROR] INVALID TOKEN (VtexIdclientAutCookie) ON DESTINATION ACCOUNT OR INVALID DESTINATION ACCOUNT, PLEASE CHECK.")
    sys.exit()

########################################
# 01 - READ JSON DATA FROM CRAWLER FILES
########################################
try:
    # READ BRANDS JSON FROM FILE
    with open('./'+source_files_prefix+'-source_brands.outputfile.json', 'r') as brands_JSON_file:
        json_source_brands=json.load(brands_JSON_file)
    if verbose_mode:
        print('[INFO] BRANDS READ FROM FILE='+str(len(json_source_brands)))
    # READ CATEGORIES JSON FROM FILE
    with open('./'+source_files_prefix+'-source_categories.outputfile.json', 'r') as categories_JSON_file:
        json_source_categories=json.load(categories_JSON_file)
    if verbose_mode:
        print('[INFO] CATEGORIES READ FROM FILE='+str(len(json_source_categories)))
    # READ PRODUCTS JSON FROM FILE
    with open('./'+source_files_prefix+'-source_products.outputfile.json', 'r') as products_JSON_file:
        json_source_products=json.load(products_JSON_file)
    if verbose_mode:
        print('[INFO] PRODUCTS READ FROM FILE='+str(len(json_source_products)))
    # READ SKUS JSON FROM FILE
    with open('./'+source_files_prefix+'-source_skus.outputfile.json', 'r') as skus_JSON_file:
        json_source_skus=json.load(skus_JSON_file)
    if verbose_mode:
        print('[INFO] SKUS READ FROM FILE='+str(len(json_source_skus)))
    # READ SKU IMAGES JSON FROM FILE
    with open('./'+source_files_prefix+'-source_sku_images.outputfile.json', 'r') as sku_images_JSON_file:
        json_source_sku_images=json.load(sku_images_JSON_file)
    if verbose_mode:
        print('[INFO] IMAGE FILES READ FROM FILE='+str(len(json_source_sku_images)))
    # READ SKU PRICES JSON FROM FILE
    with open('./'+source_files_prefix+'-source_sku_prices.outputfile.json', 'r') as sku_prices_JSON_file:
        json_source_sku_prices=json.load(sku_prices_JSON_file)
    if verbose_mode:
        print('[INFO] PRICES READ FROM FILE='+str(len(json_source_sku_prices)))
except Exception as e:
    print("ERROR DURING INITIAL CRAWLER FILES READ: ")
    print(str(e))
    sys.exit()

#########################
# 02 - CREATE BRANDS STEP
#########################
new_brand_payload = {
   "Id": 2000003,
   "Name": "New Brand",
   "Text": "New Brand",
   "Keywords": "new-brand",
   "SiteTitle": "New Brand",
   "Active": True,
   "MenuHome": True,
   "AdWordsRemarketingCode": "",
   "LomadeeCampaignCode": "",
   "LinkId": "Orma-Carbon"
}

if do_brand_step:
    brand_count = 0
    brand_total = len(json_source_brands)
    for brand_payload in json_source_brands:
        brand_payload['Active'] = True
        brand_payload['MenuHome'] = True
        new_brand = write_JSON_to_api("https://"+replicate_to_account+".vtexcommercestable.com.br/api/catalog/pvt/brand", brand_payload)
        if new_brand:
            if verbose_mode:
                print("[INFO] SUCCESSFULLY CREATED BRAND "+brand_payload['name']+" {:.0%}".format(brand_count/brand_total))
        else:
            print("[ERROR] CREATING BRAND "+brand_payload['name']+" {:.0%}".format(brand_count/brand_total))
        brand_count = brand_count + 1
else:
    print("[INFO] SKIPPING BRAND STEP...")

#############################
# 03 - CREATE CATEGORIES STEP
#############################
new_category_payload = {
    "Id": "12",
    "Name": "Home Appliances",
    "Keywords": "Kitchen, Laundry, Appliances",
    "Title": "Home Appliances",
    "Description": "Discover our range of home appliances. Find smart vacuums, kitchen and laundry appliances to suit your needs. Order online now.",
    "FatherCategoryId": 2,
    "GlobalCategoryId": 222,
    "ShowInStoreFront": True,
    "IsActive": True,
    "ActiveStoreFrontLink": True,
    "ShowBrandFilter": True,
    "Score": 3,
    "StockKeepingUnitSelectionMode": "SPECIFICATION"
}

if do_category_step:
    category_count = 0
    category_total = len(json_source_categories)
    for category_payload in json_source_categories:
        if 'hasChildren' in category_payload: category_payload.pop('hasChildren')
        if 'url' in category_payload: category_payload.pop('url')
        if 'children' in category_payload: category_payload.pop('children')
        category_payload['isActive'] = True
        category_payload['ShowInStoreFront'] = True
        category_payload['ActiveStoreFrontLink'] = True
        
        new_category = write_JSON_to_api("https://"+replicate_to_account+".vtexcommercestable.com.br/api/catalog/pvt/category", category_payload)

        if new_category:
            if verbose_mode:
                print("[INFO] SUCCESSFULLY CREATED CATEGORY " + category_payload['name']+" {:.0%}".format(category_count/category_total))
        else:
            print("[ERROR] CREATING CATEGORY "+category_payload['name']+" {:.0%}".format(category_count/category_total))
        category_count = category_count + 1
else:
    print("[INFO] SKIPPING CATEGORY STEP...")

###########################
# 04 - CREATE PRODUCTS STEP
###########################
new_prod_payload = {
    "Name": "insert product test",
    "DepartmentId": 12,
    "CategoryId": 12,
    "BrandId": 2000003,
    "LinkId": "insert-product-test",
    "RefId": "310117869",
    "IsVisible": True,
    "Description": "Description text",
    "DescriptionShort": "Digit something here",
    "ReleaseDate": "2019-01-01T00:00:00",
    "KeyWords": "test",
    "Title": "test product",
    "IsActive": True,
    "TaxCode": "12345",
    "MetaTagDescription": "tag test",
    "SupplierId": 1,
    "ShowWithoutStock": True,
    "Score": 1
}

if do_product_step:
    product_count = 0
    product_total = len(json_source_products)
    for product_payload in json_source_products:
        # product payload data fixes
        new_product = write_JSON_to_api("https://"+replicate_to_account+".vtexcommercestable.com.br/api/catalog/pvt/product", product_payload)

        if new_product:
            if verbose_mode:
                print("[INFO] SUCCESSFULLY CREATED PRODUCT " + product_payload['Name']+" {:.0%}".format(product_count/product_total))
        else:
            print("[ERROR] CREATING PRODUCT "+product_payload['Name']+" {:.0%}".format(product_count/product_total))
        product_count = product_count + 1
else:
    print("[INFO] SKIPPING PRODUCT STEP...")

#######################
# 05 - CREATE SKUS STEP
#######################
new_sku_payload = {
    "ProductId": 1,
    "IsActive": False,
    "Name": "sku test",
    "RefId": "125478",
    "PackagedHeight": 10,
    "PackagedLength": 10,
    "PackagedWidth": 10,
    "PackagedWeightKg": 10,
    "Height": 1,
    "Length": 1,
    "Width": 1,
    "WeightKg": 1,
    "CubicWeight": 1,
    "IsKit": False,
    "RewardValue": 1,
    "ManufacturerCode": "123",
    "CommercialConditionId": 1,
    "MeasurementUnit": "un",
    "UnitMultiplier": 1,
    "KitItensSellApart": False,
    "ActivateIfPossible": False
}

if do_sku_step:
    sku_count = 0
    sku_total = len(json_source_skus)
    for sku_payload in json_source_skus:
        # data fixes
        sku_payload.pop('IsActive',None)
        new_sku = write_JSON_to_api("https://"+replicate_to_account+".vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit", sku_payload)

        if new_sku:
            if verbose_mode:
                print("[INFO] SUCCESSFULLY CREATED SKU " + sku_payload['Name']+" {:.0%}".format(sku_count/sku_total))
            if do_clean_files:
                sku_id_to_delete_all_files = str(new_sku['Id'])
                clean_sku_files = send_DELETE_to_api("https://"+replicate_to_account+".vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/"+sku_id_to_delete_all_files+'/file')
            if sku_payload['Id'] in json_source_sku_images:
                for sku_image_payload in json_source_sku_images[sku_payload['Id']]:
                    # data fixes
                    sku_image_payload.pop("Id",None)
                    sku_image_payload.pop("ArchiveId",None)
                    sku_image_payload.pop("SkuId",None)
                    new_sku_file = write_JSON_to_api("https://"+replicate_to_account+".vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/"+str(sku_payload['Id'])+"/file", sku_image_payload)
                    if new_sku_file:
                        if verbose_mode:
                            print("[INFO] SUCCESSFULLY CREATED SKU IMAGE "+sku_image_payload["Url"]+" for SKU ID: "+str(sku_payload['Id']))
                    else:
                        print("[ERROR] CREATING IMAGE "+sku_image_payload["Url"]+" for SKU ID: "+str(sku_payload['Id']))
        else:
            print("[ERROR] CREATING SKU "+sku_payload['Name']+" {:.0%}".format(sku_count/sku_total))
            if do_clean_files:
                sku_id_to_delete_all_files = str(sku_payload['Id'])
                clean_sku_files = send_DELETE_to_api("https://"+replicate_to_account+".vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/"+sku_id_to_delete_all_files+'/file')

            if str(sku_payload['Id']) in json_source_sku_images:
                for sku_image_payload in json_source_sku_images[str(sku_payload['Id'])]:
                    # data fixes
                    sku_image_payload.pop("Id",None)
                    sku_image_payload.pop("ArchiveId",None)
                    sku_image_payload.pop("SkuId",None)
                    new_sku_file = write_JSON_to_api("https://"+replicate_to_account+".vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/"+str(sku_payload['Id'])+"/file", sku_image_payload)
                    if new_sku_file:
                        if verbose_mode:
                            print("[INFO] SUCCESSFULLY CREATED SKU IMAGE "+sku_image_payload["Url"]+" for SKU ID: "+str(sku_payload['Id']))
                    else:
                        print("[ERROR] CREATING IMAGE "+sku_image_payload["Url"]+" for SKU ID: "+str(sku_payload['Id']))
        
        sku_count = sku_count + 1
else:
    print("[INFO] SKIPPING SKU STEP...")

###################################
# 06 - CREATE STOCK AND PRICE STEPS
###################################

if do_stock_and_price:
    sku_stock_count = 0
    sku_stock_total = len(json_source_skus)
    
    new_sku_inventory_payload = {
        "unlimitedQuantity": False,
        "quantity": 666
        }

    for sku_payload in json_source_skus:
        warehouse_to_fill = '1_1'
        new_sku_inventory = update_JSON_to_api("https://"+replicate_to_account+".vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/"+str(sku_payload['Id'])+"/warehouses/"+warehouse_to_fill,new_sku_inventory_payload)
        if new_sku_inventory:
            if verbose_mode:
                print("[INFO] SUCCESSFULLY ADDED INVENTORY TO SKU ID "+str(sku_payload['Id'])+" {:.0%}".format(sku_stock_count/sku_stock_total))
        else:
            print("[ERROR] ADDING INVENTORY TO SKU ID "+str(sku_payload['Id'])+" {:.0%}".format(sku_stock_count/sku_stock_total))
        
        if json_source_sku_prices[str(sku_payload['Id'])]:
            json_source_sku_prices[str(sku_payload['Id'])].pop("itemId",None)
            json_source_sku_prices[str(sku_payload['Id'])].pop("basePrice",None)
            json_source_sku_prices[str(sku_payload['Id'])].pop("fixedPrices",None)
            
            new_sku_price = update_JSON_to_api("https://"+replicate_to_account+".vtexcommercestable.com.br/api/pricing/prices/"+str(sku_payload['Id']),json_source_sku_prices[str(sku_payload['Id'])])
            if new_sku_price:
                if verbose_mode:
                    print("[INFO] SUCCESSFULLY ADDED PRICE TO SKU ID "+str(sku_payload['Id'])+" {:.0%}".format(sku_stock_count/sku_stock_total))
            else:
                print("[ERROR] ADDING PRICE TO SKU ID "+str(sku_payload['Id'])+" {:.0%}".format(sku_stock_count/sku_stock_total))
        else:
            if verbose_mode:
                print("[INFO] SKIPPING PRICING FOR SKU ID "+str(sku_payload['Id'])+" {:.0%}".format(sku_stock_count/sku_stock_total))

        sku_stock_count = sku_stock_count + 1
else:
    print("[INFO] SKIPPING STOCK AND PRICE STEPS...")

