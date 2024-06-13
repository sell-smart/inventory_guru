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

    def _read_bulk_operation_data(self, url):
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an error for bad status codes

        # Read the response line by line and parse each line as JSON
        data = []
        for line in response.iter_lines():
            if line:
                data.append(json.loads(line))

        return data


    def fetch_products(self, query):
        try:
            logging.info("Executing GraphQL query for bulk operation.")
            initiate_response = self.execute_graphql_query(query)
            
            if 'errors' in initiate_response:
                logging.error(f"GraphQL query errors: {initiate_response['errors']}")
                raise Exception(initiate_response['errors'])

            logging.info("Waiting for data notification.")
            data_notification = self.data_notification_pop()

            # to-do: add error-handling
            download_url = data_notification['data']['currentBulkOperation']['url']
            logging.info(f"Fetching data from bulk operation download URL: {download_url}")

            data = self._read_bulk_operation_data(download_url)
            logging.info("Bulk operation data fetched successfully.")

            return data
        
        except Exception as ex:
            logging.exception("An error occurred during the bulk operation process.")
            raise ex



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
        self.activate_session()  # Ensure the session is activated

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
        return self.fetch_products(query)