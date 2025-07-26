import pandas as pd
from curl_cffi import requests
from datetime import datetime
import json
import time
import random


def perform_blinkit_scraping_v2():
    # Load inputs
    try:
        loc_data = pd.read_csv("blinkit_locations.csv")
        cat_data = pd.read_csv("blinkit_categories.csv")
        schema_data = pd.read_csv("Scraping Task _ Schema - Schema.csv", skiprows=1)
        required_cols = schema_data['Field'].tolist()
    except FileNotFoundError as fnf_err:
        print(f"Input files missing or path incorrect: {fnf_err}")
        return

    collected_products = []
    today_str = datetime.now().strftime("%Y-%m-%d")
    endpoint_url = "https://blinkit.com/v1/layout/listing_widgets"

    sess = requests.Session(impersonate="chrome120", timeout=30)

    print("Starting scraping run (v2)...")

    for _, loc in loc_data.iterrows():

        latitude = loc['latitude']
        longitude = loc['longitude']

        print(f"\nProcessing location -> lat: {latitude}, lon: {longitude}")

        for _, cat in cat_data.iterrows():
            main_cat = cat['l1_category']
            main_cat_id = cat['l1_category_id']
            sub_cat = cat['l2_category']
            sub_cat_id = cat['l2_category_id']
            
            # build headers for this request
            req_headers = {
                'authority': 'blinkit.com',
                'origin': 'https://blinkit.com',
                'referer': f"https://blinkit.com/cn/{main_cat.lower().replace(' ', '-')}/cid/{main_cat_id}/{sub_cat_id}",
                'auth_key': 'c761ec3633c22afad934fb17a66385c1c06c5472b4898b866b7306186d0bb477',
                'lat': str(latitude),
                'lon': str(longitude),
                'content-type': 'application/json',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            }
            params_dict = {'l0_cat': main_cat_id, 'l1_cat': sub_cat_id}

            print(f"  Fetching products for category: {main_cat} > {sub_cat} ...")

            attempts_allowed = 3
            for attempt_idx in range(attempts_allowed):
                try:
                    res = sess.post(endpoint_url, headers=req_headers, params=params_dict, json={})
                    if res.status_code == 429:
                        res.raise_for_status()

                    res.raise_for_status()
                    content = res.json()
                    products_list = content.get('response', {}).get('snippets', [])

                    if not products_list:
                        print(f"  No products found, possibly empty category.")
                        break

                    print(f"  Successful retrieval: {len(products_list)} items.")

                    for prod_wrapper in products_list:
                        prod_info = prod_wrapper.get('data', {})
                        atc_info = prod_info.get('atc_action', {}).get('add_to_cart', {}).get('cart_item', {})
                        tracking_info = prod_info.get('tracking', {}).get('common_attributes', {})

                        collected_products.append({
                            'date': today_str,
                            'l1_category': main_cat,
                            'l1_category_id': main_cat_id,
                            'l2_category': sub_cat,
                            'l2_category_id': sub_cat_id,
                            'store_id': atc_info.get('merchant_id'),
                            'variant_id': atc_info.get('product_id'),
                            'variant_name': atc_info.get('display_name'),
                            'group_id': atc_info.get('group_id'),
                            'selling_price': atc_info.get('price'),
                            'mrp': atc_info.get('mrp'),
                            'in_stock': not prod_info.get('is_sold_out', True),
                            'inventory': atc_info.get('inventory'),
                            'is_sponsored': tracking_info.get('badge') == 'AD',
                            'image_url': atc_info.get('image_url'),
                            'brand': atc_info.get('brand'),
                            'brand_id': None
                        })

                    break  # break retry loop on success

                except requests.errors.RequestsError as err:
                    if err.response and err.response.status_code == 429:
                        cooldown = 60
                        print(f"  Rate limited (429). Waiting for {cooldown} sec... Attempt {attempt_idx+1}/{attempts_allowed}")
                        time.sleep(cooldown)
                    else:
                        print(f"  Fatal error during request: {err}")
                        break

            time.sleep(random.uniform(1.5, 4.0))

    df_out = pd.DataFrame(collected_products)

    if not df_out.empty:
        df_out = df_out.reindex(columns=required_cols)

    out_file = 'blinkit_scraped_data_final.csv'
    df_out.to_csv(out_file, index=False)
    print(f"\nScraping finished. Total products saved: {len(df_out)} in '{out_file}'.")


if __name__ == "__main__":
    perform_blinkit_scraping_v2()
