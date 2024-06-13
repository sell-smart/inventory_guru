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

load_dotenv()

SHOPIFY_SECRET = os.environ.get('SHOPIFY_SECRET')
SHOPIFY_API_KEY = os.environ.get('SHOPIFY_API_KEY')
SHOPIFY_API_VERSION = os.environ.get('SHOPIFY_API_VERSION')

REQUEST_METHODS = {
    "GET": requests.get,
    "POST": requests.post,
    "PUT": requests.put,
    "DEL": requests.delete
}

#class NamedQueues:
#    queues = {}
#
#    @staticmethod
#    def get_queue(name):
#        if name not in NamedQueues.queues:
#            NamedQueues.queues[name] = queue.Queue()
#        return NamedQueues.queues[name]

    #@staticmethod
    #def put_item(name, item):
    #    if name in NamedQueues.queues:
    #        NamedQueues.queues[name].put(item)

    #@staticmethod
    #def get_item(name):
    #    if name in NamedQueues.queues:
    #        return NamedQueues.queues[name].get()

class EventManager:
    events = {}

    @staticmethod
    def get_event(name):
        if name not in EventManager.events:
            EventManager.events[name] = threading.Event()
        return EventManager.events[name]

    #@staticmethod
    #def set_event(name):
    #    if name in NamedEvents.events:
    #        NamedEvents.events[name].set()

    #@staticmethod
    #def clear_event(name):
    #    if name in NamedEvents.events:
    #        NamedEvents.events[name].clear()


class ShopifyGraphQLClient:
    def __init__(self, shop, access_token):
        self.shop = shop
        self.access_token = access_token
        self.data_ready_event = EventManager.get_event(shop)

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
        query = """
        {
          currentBulkOperation {
            id
            status
            errorCode
            createdAt
            completedAt
            objectCount
            fileSize
            url
            partialDataUrl
          }
        }
        """
        return self.execute_graphql_query(query)

    def fetch_bulk_operation_data(self, url):
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an error for bad status codes

        # Read the response line by line and parse each line as JSON
        data = [json.loads(line) for line in response.iter_lines() if line]
        #data = []
        #for line in response.iter_lines():
        #    if line:
        #        data.append(json.loads(line))

        return data

    def data_notification_pop(self):
        self.data_ready_event.wait()
        self.data_ready_event.clear()
        return self.check_bulk_operation_status()

    def data_notification_push(self):
        self.data_ready_event.set()


class ShopifyStoreClient(ShopifyGraphQLClient):

    def __init__(self, shop, access_token):
        super().__init__(shop, access_token)
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

    def get_shop(self) -> dict:
        call_path = 'shop.json'
        method = 'GET'
        shop_response = self.authenticated_shopify_call(call_path=call_path, method=method)
        if not shop_response:
            return None
        # The myshopify_domain value is the one we'll need to listen to via webhooks to determine an uninstall
        return shop_response['shop']

    def get_script_tags(self) -> List:
        script_tags_response = self.authenticated_shopify_call('script_tags.json', 'GET')
        if not script_tags_response:
            return None
        return script_tags_response['script_tags']

    def get_script_tag(self, id: int) -> dict:
        script_tag_response = self.authenticated_shopify_call(f'script_tags/{id}.json', 'GET')
        if not script_tag_response:
            return None
        return script_tag_response['script_tag']

    def update_script_tag(self, id: int, src: str, display_scope: str = None) -> bool:
        payload = {"script_tag": {"id": id, "src": src}}
        if display_scope:
            payload['script_tag']['display_scope'] = display_scope
        script_tags_response = self.authenticated_shopify_call(f'script_tags/{id}.json', 'PUT', payload)
        if not script_tags_response:
            return None
        return script_tags_response['script_tag']

    def create_script_tag(self, src: str, event: str = 'onload', display_scope: str = None) -> int:
        payload = {'script_tag': {'event': event, 'src': src}}
        if display_scope:
            payload['script_tag']['display_scope'] = display_scope
        script_tag_response = self.authenticated_shopify_call('script_tags.json', 'POST', payload)
        if not script_tag_response:
            return None
        return script_tag_response['script_tag']

    def delete_script_tag(self, script_tag_id: int) -> int:
        script_tag_response = self.authenticated_shopify_call(f'script_tags/{script_tag_id}.json', 'DEL')
        if script_tag_response is None:
            return False
        return True

    def create_usage_charge(self, recurring_application_charge_id: int, description: str, price: float) -> dict:
        payload = {'usage_charge': {'description': description, 'price': price}}
        usage_charge_response = self.authenticated_shopify_call(f'recurring_application_charges/{recurring_application_charge_id}/usage_charges.json', 'POST', payload)
        if not usage_charge_response:
            return None
        return usage_charge_response['usage_charge']

    def get_recurring_application_charges(self) -> List:
        recurring_application_charges_response = self.authenticated_shopify_call('recurring_application_charges.json', 'GET')
        if not recurring_application_charges_response:
            return None
        return recurring_application_charges_response['recurring_application_charges']

    def delete_recurring_application_charges(self, recurring_application_charge_id: int) -> bool:
        # Broken currently,authenticated_shopify_call expects JSON but this returns nothing
        delete_recurring_application_charge_response = self.authenticated_shopify_call(f'recurring_application_charges/{recurring_application_charge_id}.json', 'DEL')
        if delete_recurring_application_charge_response is None:
            return False
        return True

    def activate_recurring_application_charge(self, recurring_application_charge_id: int) -> dict:
        recurring_application_charge_activation_response = self.authenticated_shopify_call(f'recurring_application_charges/{recurring_application_charge_id}/activate.json', 'POST', {})
        if not recurring_application_charge_activation_response:
            return None
        return recurring_application_charge_activation_response['recurring_application_charge']

    def create_webook(self, address: str, topic: str, overwrite = False) -> dict:

        # remove webhook first if it already exists and is different, otherwise (if it exists and is the same), quit
        if overwrite:
            existing_webhooks = self.get_existing_webhooks(topic=topic)
            if len(existing_webhooks) > 0:
                if existing_webhooks[0]['address'] != address:
                    self.remove_webhooks(topic=topic)
                else:
                    return

        payload = {
            "webhook": {
                "topic": topic,
                "address": address,
                "format": "json"
            }
        }
        webhook_response = self.authenticated_shopify_call('webhooks.json', method='POST', payload=payload)
        if not webhook_response:
            return None
        return webhook_response['webhook']

    def get_webhooks_count(self, topic: str):
        webhook_count_response = self.authenticated_shopify_call(f'webhooks/count.json?topic={topic}', 'GET')
        if not webhook_count_response:
            return None
        return webhook_count_response['count']

    def get_existing_webhooks(self, topic = None):
        webhooks = self.authenticated_shopify_call('webhooks.json', 'GET')
        if not topic:
            return webhooks['webhooks'] 
        
        relevant_webhooks = []
        for webhook in webhooks['webhooks']:
            if webhook['topic'] == topic:
                relevant_webhooks.append( webhook )
        return relevant_webhooks
    
    def remove_webhooks(self, topic: str):
        existing_webhooks = self.get_existing_webhooks(topic)
        if existing_webhooks:
            for webhook in existing_webhooks:
                self.authenticated_shopify_call(f'webhooks/{webhook["id"]}.json', 'DEL')
                logging.info(f"Removed webhook with ID: {webhook['id']} and topic: {topic}")

    def fetch_products(self):
        query = """
        mutation {
          bulkOperationRunQuery(
            query: \"\"\"
            {
              products {
                edges {
                  node {
                    id
                    title
                  }
                }
              }
            }
            \"\"\"
          ) {
            bulkOperation {
              id
              status
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        initiate_response = self.execute_graphql_query(query)
    
        if 'errors' in initiate_response:
            # return jsonify(initiate_response['errors'])
            raise initiate_response['errors']

        incoming_data = self.data_notification_pop()
        print( incoming_data )

        # to-do: add error-handling
        download_url = incoming_data['data']['currentBulkOperation']['url']
        products = self.fetch_bulk_operation_data(download_url)

        return products
    

        raise "XXX"
    
        # self.fetch_bulk_operation_data(self.bulk_data_url)

        # Polling mechanism for demonstration (replace with webhook handling in production)
        while True:
            status_response = self.check_bulk_operation_status()
            status = status_response['data']['currentBulkOperation']['status']
            if status == 'COMPLETED':
                download_url = status_response['data']['currentBulkOperation']['url']
                products = self.fetch_bulk_operation_data(download_url)
                # return jsonify(products_data)
                break
            elif status == 'FAILED':
                # return jsonify({'error': 'Bulk operation failed'})
                raise 'Bulk operation failed'
            time.sleep(2)  # Wait before checking the status again

        return products

