import os
import json
from typing import List
import logging
import time

import requests
from requests.exceptions import HTTPError

from dotenv import load_dotenv
import shopify
import threading
import queue

from toolz.dicttoolz import get_in
from pprint import pprint
from pathlib import Path

from prefect.deployments import run_deployment

query_dir = os.path.dirname(os.path.realpath(__file__))
read_query = lambda fname_query: Path( os.path.join(query_dir, fname_query) ).read_text()

query_fetch_products = read_query("queries/products.graphql")
query_fetch_variants = read_query("queries/variants.graphql")
query_fetch_orders = read_query("queries/orders.graphql")
query_bulkop_status = read_query("queries/bulkop_status.graphql")



load_dotenv()

SHOPIFY_SECRET = os.environ.get('SHOPIFY_SECRET')
SHOPIFY_API_KEY = os.environ.get('SHOPIFY_API_KEY')
SHOPIFY_API_VERSION = os.environ.get('SHOPIFY_API_VERSION')

BACKEND_HOSTNAME = os.environ.get('BACKEND_HOSTNAME')
BACKEND_PORT = os.environ.get('BACKEND_PORT')

REQUEST_METHODS = {
    "GET": requests.get,
    "POST": requests.post,
    "PUT": requests.put,
    "DEL": requests.delete
}


def run_orders_ingestion(shop_name, **kwargs):
    flow_run_name = f"<{shop_name}><{kwargs['json_url']}>"
    run_deployment( name = "orders_ingestion", flow_run_name = flow_run_name, **kwargs )

def run_product_ingestion(shop_name, **kwargs):
    flow_run_name = f"<{shop_name}><{kwargs['json_url']}>"
    run_deployment( name = "products_ingestion", flow_run_name = flow_run_name, **kwargs )

def run_variants_ingestion(shop_name, **kwargs):
    flow_run_name = f"<{shop_name}><{kwargs['json_url']}>"
    run_deployment( name = "variants_ingestion", flow_run_name = flow_run_name, **kwargs )

def run_optimization(shop_name, **kwargs):
    flow_run_name = f"<{shop_name}><{kwargs['product_id']}><{kwargs['variant_id']}>"
    run_deployment( name = "forecast", flow_run_name = flow_run_name, **kwargs )

def run_optimization(shop_name, **kwargs):
    flow_run_name = f"<{shop_name}><{kwargs['product_id']}><{kwargs['variant_id']}>"
    run_deployment( name = "optimization", flow_run_name = flow_run_name, **kwargs )

def run_generate_synthetic_sales(shop_name, **kwargs):
    flow_run_name = f"<{shop_name}><{kwargs['product_id']}><{kwargs['variant_id']}>"
    run_deployment( name = "synthetic_sales", flow_run_name = flow_run_name, **kwargs )


class EventManager:
    events = {}

    @staticmethod
    def _exists_event(name):
        if name not in EventManager.events:
            EventManager.events[name] = threading.Event()

    @staticmethod
    def signal(name):
        EventManager._exists_event(name)
        EventManager.events[name].set()

    @staticmethod
    def wait(name):
        EventManager._exists_event(name)
        EventManager.events[name].wait()
        EventManager.events[name].clear()


class Events:
    def __init__(self, shop):
        self.shop = shop

    def signal(self, id):
        EventManager.wait( (self.shop, id) )

    def wait(self, id):
        EventManager.signal( (self.shop, id) )



class ShopifyGraphQLClient:
    def __init__(self, shop, access_token):
        self.shop = shop
        self.access_token = access_token
        self.events = Events(shop)

        self.session = shopify.Session(
            shop_url=f"{shop}.myshopify.com", 
            version=SHOPIFY_API_VERSION, 
            token=access_token
        )
        shopify.ShopifyResource.activate_session( self.session )

    def execute_graphql_query(self, query):
        response = shopify.GraphQL().execute(query)
        return json.loads(response)

    def check_bulk_operation_status(self):
        return self.execute_graphql_query( query_bulkop_status )

    def _read_bulk_operation_data(self, url):
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an error for bad status codes

        # Read the response line by line and parse each line as JSON
        data = []
        for line in response.iter_lines():
            if line:
                data.append(json.loads(line))

        return data


    def fetch_bulk_operation_data(self, query, fn_read_bulk_operation_data):
        query = f"""
                mutation {{
                    bulkOperationRunQuery(
                        query: \"\"\"
                        {{
                            {query}
                        }}
                        \"\"\"
                    ) {{
                        bulkOperation {{
                            id
                            status
                        }}
                        userErrors {{
                            field
                            message
                        }}
                    }}
                }}
        """

        try:
            logging.info("Executing GraphQL query for bulk operation.")
            initiate_response = self.execute_graphql_query(query)
            
            query_status = get_in(['data', 'bulkOperationRunQuery', 'bulkOperation', 'status'], initiate_response)
            if (not query_status) and (not query_status == 'CREATED'):
                logging.error(str(initiate_response))
                raise f"*** GraphQL query failed. ***"

            while True:
                logging.info("Waiting for data notification.")
                data_notification = self.data_notification_pop()

                query_status = get_in(['data', 'currentBulkOperation', 'status'], data_notification)
                if query_status == "FAILED":
                    logging.error(f"GraphQL query failure. Error code: {data_notification['errorCode']}")
                    raise Exception(f"GraphQL query failure. Error code: {data_notification['errorCode']}")
                
                elif query_status == "RUNNING":
                    continue

                download_url = data_notification['data']['currentBulkOperation']['url']
                logging.info(f"Fetching data from bulk operation download URL: {download_url}")
                if download_url:
                    break

            logging.info("Starting data retrieval.")
            #data = self._read_bulk_operation_data(download_url)
            result = fn_read_bulk_operation_data( self.shop, json_url = download_url )
            logging.info("Bulk operation data fetched successfully.")

            #return data
        
        except Exception as ex:
            logging.exception("An error occurred during the bulk operation process.")
            raise ex

    def data_notification_pop(self):
        self.events.wait( "webhook" )
        return self.check_bulk_operation_status()

    def data_notification_push(self):
        self.events.signal( "webhook" )

if False:
    class BackendClient:
        def __init__(self, shop):
            self.shop = shop
            self.url_base = f"https://{BACKEND_HOSTNAME}:{BACKEND_PORT}"


        def cue_orders_retrieval(self, json_url, callback_url_success , callback_url_failure):
            url_endpoint = f"{self.url_base}/data/cue_orders_retrieval"
            params = {'shop': self.shop, 'json_url': json_url, 'callback_url_success': callback_url_success, 'callback_url_failure': callback_url_failure }
            response = requests.get(url_endpoint, params)

        def cue_products_retrieval(self, json_url, callback_url_success , callback_url_failure):
            url_endpoint = f"{self.url_base}/data/cue_products_retrieval"
            params = {'shop': self.shop, 'json_url': json_url, 'callback_url_success': callback_url_success, 'callback_url_failure': callback_url_failure}
            response = requests.get(url_endpoint, params)

        def cue_variants_retrieval(self, json_url, callback_url_success , callback_url_failure):
            url_endpoint = f"{self.url_base}/data/cue_variants_retrieval"
            params = {'shop': self.shop, 'json_url': json_url, 'callback_url_success': callback_url_success, 'callback_url_failure': callback_url_failure}
            response = requests.get(url_endpoint, params)


class WebhookClient:
    def __init__(self, shop):
        self.shop = shop

    #def create_webook(self, address: str, topic: str, overwrite = False) -> dict:
    #
    #    # remove webhook first if it already exists and is different, otherwise (if it exists and is the same), quit
    #    if overwrite:
    #        existing_webhooks = self.get_existing_webhooks(topic=topic)
    #        if len(existing_webhooks) > 0:
    #            if existing_webhooks[0]['address'] != address:
    #                self.remove_webhooks(topic=topic)
    #            else:
    #                return
    #
    #    payload = {
    #        "webhook": {
    #            "topic": topic,
    #            "address": address,
    #            "format": "json"
    #        }
    #    }
    #    webhook_response = self.authenticated_shopify_call('webhooks.json', method='POST', payload=payload)
    #    if not webhook_response:
    #        return None
    #    return webhook_response['webhook']

    def create_webook(self, address: str, topic: str, overwrite = False) -> dict:

        # remove webhook first if it already exists and is different, otherwise (if it exists and is the same), quit
        if overwrite:
            existing_webhooks = self.get_existing_webhooks(topic=topic)
            if len(existing_webhooks) > 0:
                if existing_webhooks[0]['address'] != address:
                    self.remove_webhooks(topic=topic)
                else:
                    return

        webhook = shopify.Webhook()
        webhook.topic = topic
        webhook.address = address
        webhook.format = "json"
        
        try:
            if webhook.save():
                return webhook.attributes
            else:
                logging.error(f"Failed to create webhook: {webhook.errors.full_messages()}")
                return None
        except Exception as ex:
            logging.exception(ex)
            return None

    def get_webhooks_count(self, topic: str):
        try:
            webhooks_count = shopify.Webhook.count(topic=topic)
            return webhooks_count
        except Exception as ex:
            logging.exception(ex)
            return None

    def get_existing_webhooks(self, topic = None):
        try:
            webhooks_it = shopify.Webhook.find()
            webhooks = [webhook.attributes for webhook in webhooks_it]
            if not topic:
                return webhooks

            relevant_webhooks = [webhook for webhook in webhooks if webhook['topic'] == topic]
            return relevant_webhooks
        
        except Exception as ex:
            logging.exception(ex)
            return None
    
    def remove_webhooks(self, topic: str):
        try:
            existing_webhooks = self.get_existing_webhooks(topic)
            if existing_webhooks:
                for webhook in existing_webhooks:
                    webhook_id = webhook["id"]
                    webhook_obj = shopify.Webhook.find(webhook_id)
                    if webhook_obj.destroy():
                        logging.info(f"Removed webhook with ID: {webhook_id} and topic: {topic}")
                    else:
                        logging.error(f"Failed to remove webhook with ID: {webhook_id} and topic: {topic}")

        except Exception as ex:
            logging.exception(ex)



class ShopifyStoreClient(ShopifyGraphQLClient, WebhookClient):

    def __init__(self, shop, access_token):
        ShopifyGraphQLClient.__init__(self, shop, access_token)
        WebhookClient.__init__(self, shop)
        self.base_url = f"https://{shop}/admin/api/{SHOPIFY_API_VERSION}/"

    @staticmethod
    def authenticate(shop: str, code: str) -> str:
        url = f"https://{shop}/admin/oauth/access_token"
        payload = {
            "client_id": SHOPIFY_API_KEY,
            "client_secret": SHOPIFY_SECRET,
            "code": code
        }
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            return response.json()['access_token']

        except HTTPError as ex:
            logging.exception(ex)
            return None

    def authenticated_shopify_call(self, call_path: str, method: str, params: dict = None, payload: dict = None, headers: dict = {}) -> dict:
        url = f"{self.base_url}{call_path}"
        request_func = REQUEST_METHODS[method]
        headers['X-Shopify-Access-Token'] = self.access_token
        #logging.basicConfig(level=logging.DEBUG)

        try:
            logging.debug(f"request_func('{url}', params={params}, json={payload}, headers={headers})")
            response = request_func(url, params=params, json=payload, headers=headers)
            response.raise_for_status()
            logging.debug(f"authenticated_shopify_call response:\n{json.dumps(response.json(), indent=4)}")
            return response.json()

        except HTTPError as ex:
            logging.exception(ex)
            return None


    def fetch_products(self):
        self.fetch_bulk_operation_data(query_fetch_products, run_orders_ingestion)
        # products = ...
        # for i in range(len(products)):
        #     products[i]['id'] = products[i]['id'][22:] 
        # return products

    def fetch_variants(self):
        self.fetch_bulk_operation_data(query_fetch_variants, run_variant_ingestion)
        # variants = 
        # variants_clean = []
        # for variant in variants:
        #     variants_clean.append( {
        #             'variant_id': variant['id'][29:],
        #             'variant_title': variant['title'],
        #             'product_id': variant['product']['id'][22:],
        #             'product_title': variant['product']['title']
        #     })
        # 
        # return variants_clean

    def fetch_orders(self):
        #orders = shopify.Order.find()
        #order_list = []
        #for order in orders:
        #    order_list.append( order.to_dict() )
        #
        self.fetch_bulk_operation_data(query_fetch_orders, run_product_ingestion)
        # orders = ...
        # orders_meta = []
        # orders_line_items = []
        # for item in orders:
        #     if "__parentId" in item:
        #         item['id'] = item['id'][23:]
        #         item['product']['id'] = item['product']['id'][22:]
        #         item['variant']['id'] = item['variant']['id'][29:]
        #         item['__parentId'] = item['__parentId'][20:]
        #         orders_line_items.append( item )
        #     else:
        #         item['id'] = item['id'][20:]
        #         orders_meta.append( item )
        #     #orders_clean.append({
        #     #    'id': order['id'],
        #     #    'customer_id': order['customer']['id'] if order['customer'] else None,
        #     #    'total_price': order['totalPriceSet']['presentmentMoney']['amount'],
        #     #    'order_date': order['createdAt'],
        #     #    'line_items': [{'id': item['node']['id'], 'title': item['node']['title']} for item in order['lineItems']['edges']]
        #     #})
        # 
        # return orders_meta, orders_line_items
