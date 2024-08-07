import uuid
import os
import json
import logging

from flask import Flask, redirect, request, render_template, jsonify
import shopify
import helpers
from shopify_client import ShopifyStoreClient

from dotenv import load_dotenv
from pprint import pprint

load_dotenv()

SHOPIFY_API_KEY = os.environ.get('SHOPIFY_API_KEY')
APP_NAME = os.environ.get('APP_NAME')

WEBHOOK_APP_UNINSTALL_URL = os.environ.get('WEBHOOK_APP_UNINSTALL_URL')
WEBHOOK_QUERY_FINISHED_URL = os.environ.get('WEBHOOK_QUERY_FINISHED_URL')
print('webhook 1', WEBHOOK_APP_UNINSTALL_URL)
print('webhook 2', WEBHOOK_QUERY_FINISHED_URL)


app = Flask(__name__)


ACCESS_TOKEN = None
NONCE = None
ACCESS_MODE = []  # Defaults to offline access mode if left blank or omitted. https://shopify.dev/apps/auth/oauth/access-modes
SCOPES = ['write_script_tags', "read_products", "read_orders", "read_all_orders", "read_customers", "read_inventory"] #]  # https://shopify.dev/docs/admin-api/access-scopes


class DataManager:
    data = {}

    @staticmethod
    def get_data(shop, name):
        return DataManager.data[shop][name]

    @staticmethod
    def set_data(shop, name, value):
        if not shop in DataManager.data:
            DataManager.data[shop] = {}
        DataManager.data[shop][name] = value




@app.route('/app_launched', methods=['GET'])
@helpers.verify_web_call
def app_launched():
    shop = request.args.get('shop')
    embedded = request.args.get('embedded', '0')
    global ACCESS_TOKEN, NONCE

    if ACCESS_TOKEN:

        ###
        shopify_client = ShopifyStoreClient(shop=shop, access_token=ACCESS_TOKEN)
        webhook_app_uninstall_url = f"{WEBHOOK_APP_UNINSTALL_URL}?shop={shop}"
        shopify_client.create_webook(address=webhook_app_uninstall_url, topic="app/uninstalled", overwrite=True)
        ###

        if embedded == '1':
            return render_template('welcome.html', shop=shop, api_key=SHOPIFY_API_KEY)
        else:
            embedded_url = helpers.generate_app_redirect_url(shop=shop)
            return redirect(embedded_url, code=302)
    
    # The NONCE is a single-use random value we send to Shopify so we know the next call from Shopify is valid (see #app_installed)
    #   https://en.wikipedia.org/wiki/Cryptographic_nonce
    NONCE = uuid.uuid4().hex
    redirect_url = helpers.generate_install_redirect_url(shop=shop, scopes=SCOPES, nonce=NONCE, access_mode=ACCESS_MODE)
    return redirect(redirect_url, code=302)


@app.route('/app_installed', methods=['GET'])
@helpers.verify_web_call
def app_installed():
    state = request.args.get('state')
    global NONCE, ACCESS_TOKEN

    # Shopify passes our NONCE, created in #app_launched, as the `state` parameter, we need to ensure it matches!
    if state != NONCE:
        return "Invalid `state` received", 400
    NONCE = None

    # Ok, NONCE matches, we can get rid of it now (a nonce, by definition, should only be used once)
    # Using the `code` received from Shopify we can now generate an access token that is specific to the specified `shop` with the
    #   ACCESS_MODE and SCOPES we asked for in #app_installed
    shop = request.args.get('shop')
    code = request.args.get('code')
    ACCESS_TOKEN = ShopifyStoreClient.authenticate(shop=shop, code=code)

    # We have an access token! Now let's register a webhook so Shopify will notify us if/when the app gets uninstalled
    # NOTE This webhook will call the #app_uninstalled function defined below
    client = ShopifyStoreClient(shop=shop, access_token=ACCESS_TOKEN)

    webhook_app_uninstall_url = f"{WEBHOOK_APP_UNINSTALL_URL}?shop={shop}"
    client.create_webook(address=webhook_app_uninstall_url, topic="app/uninstalled", overwrite=True)

    webhook_query_finished_url = f"{WEBHOOK_QUERY_FINISHED_URL}?shop={shop}"
    client.create_webook(address=webhook_query_finished_url, topic="bulk_operations/finish", overwrite=True)


    client.fetch_variants()
    #DataManager.set_data(shop, "variants", variants)

    client.fetch_orders()
    #DataManager.set_data(shop, "orders", orders)
    #DataManager.set_data(shop, "line_items", line_items)

    client.fetch_products()
    #DataManager.set_data(shop, "products", products)

    redirect_url = helpers.generate_app_redirect_url(shop=shop)
    return redirect(redirect_url, code=302)



@app.route('/app_uninstalled', methods=['POST'])
@helpers.verify_webhook_call
def app_uninstalled():
    # https://shopify.dev/docs/admin-api/rest/reference/events/webhook?api[version]=2020-04
    # Someone uninstalled your app, clean up anything you need to
    # NOTE the shop ACCESS_TOKEN is now void!
    global ACCESS_TOKEN
    ACCESS_TOKEN = None

    webhook_topic = request.headers.get('X-Shopify-Topic')
    webhook_payload = request.get_json()
    logging.error(f"webhook call received {webhook_topic}:\n{json.dumps(webhook_payload, indent=4)}")

    return "OK"


@app.route('/data_removal_request', methods=['POST'])
@helpers.verify_webhook_call
def data_removal_request():
    # https://shopify.dev/tutorials/add-gdpr-webhooks-to-your-app
    # Clear all personal information you may have stored about the specified shop
    return "OK"

@app.route('/query_finished', methods=['POST'])
def query_finished():
    print("--- query_finished ---")
    shop = request.args.get('shop')
    client = ShopifyStoreClient(shop=shop, access_token=ACCESS_TOKEN)
    client.data_notification_push()
    return "Webhook received", 200

@app.route('/home', methods=['GET'])
def home():
    shop = request.args.get('shop')
    return render_template('home.html', products=[], shop=shop, api_key=SHOPIFY_API_KEY)

@app.route('/products', methods=['GET'])
def products():
    shop = request.args.get('shop')
    client = ShopifyStoreClient(shop=shop, access_token=ACCESS_TOKEN)
    products = DataManager.get_data(shop, "products")
    pprint(products[0])
    return render_template('products.html', products=products, shop=shop, api_key=SHOPIFY_API_KEY)

@app.route('/products2', methods=['GET'])
def products2():
    shop = request.args.get('shop')
    client = ShopifyStoreClient(shop=shop, access_token=ACCESS_TOKEN)

    products = DataManager.get_data(shop, "products")
    variants = DataManager.get_data(shop, "variants")

    grouped_variants = {}
    for variant in variants:
        grouped_variants[ variant['product_id'] ] = variant

    return render_template('products2.html', products=products, grouped_variants=grouped_variants, shop=shop, api_key=SHOPIFY_API_KEY)

@app.route('/variants', methods=['GET'])
def variants():
    shop = request.args.get('shop')
    client = ShopifyStoreClient(shop=shop, access_token=ACCESS_TOKEN)
    variants = DataManager.get_data(shop, "variants")
    pprint(variants[0])
    return render_template('variants.html', variants=variants, shop=shop, api_key=SHOPIFY_API_KEY)

@app.route('/orders', methods=['GET'])
def orders():
    shop = request.args.get('shop')
    client = ShopifyStoreClient(shop=shop, access_token=ACCESS_TOKEN)
    orders = DataManager.get_data(shop, "orders")
    return render_template('orders.html', orders=orders, shop=shop, api_key=SHOPIFY_API_KEY)

@app.route('/line_items', methods=['GET'])
def line_items():
    shop = request.args.get('shop')
    client = ShopifyStoreClient(shop=shop, access_token=ACCESS_TOKEN)
    line_items = DataManager.get_data(shop, "line_items")
    return render_template('line_items.html', line_items=line_items, shop=shop, api_key=SHOPIFY_API_KEY)


if __name__ == '__main__':
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('APP_PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
