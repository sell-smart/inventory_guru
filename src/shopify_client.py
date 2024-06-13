import os
import json
from typing import List
import logging

import requests
from requests.exceptions import HTTPError

from dotenv import load_dotenv
import shopify

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


class ShopifyGraphQLClient:
    def __init__(self, shop, access_token):
        self.shop = shop
        self.access_token = access_token
        self.graphql_url = f"https://{self.shop}/admin/api/2021-04/graphql.json"

        self.session = shopify.Session(
            shop_url=f"{shop}.myshopify.com", 
            version=SHOPIFY_API_VERSION, 
            token=access_token
        )
        shopify.ShopifyResource.activate_session( self.session )

        # requests.Session
        # session.headers.update({
        #     'X-Shopify-Access-Token': self.access_token,
        #     'Content-Type': 'application/json'
        # })

    def execute_query(self, query):
        response = shopify.GraphQL().execute(query)
        return json.loads(response)

    def initiate_bulk_product_download(self):
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
        return self.execute_query(query)

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
        return self.execute_query(query)

    def fetch_bulk_operation_data(self, url):
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an error for bad status codes

        # Read the response line by line and parse each line as JSON
        data = []
        for line in response.iter_lines():
            if line:
                data.append(json.loads(line))

        return data


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
        call_path = 'script_tags.json'
        method = 'GET'
        script_tags_response = self.authenticated_shopify_call(call_path=call_path, method=method)
        if not script_tags_response:
            return None
        return script_tags_response['script_tags']

    def get_script_tag(self, id: int) -> dict:
        call_path = f'script_tags/{id}.json'
        method = 'GET'
        script_tag_response = self.authenticated_shopify_call(call_path=call_path, method=method)
        if not script_tag_response:
            return None
        return script_tag_response['script_tag']

    def update_script_tag(self, id: int, src: str, display_scope: str = None) -> bool:
        call_path = f'script_tags/{id}.json'
        method = 'PUT'
        payload = {"script_tag": {"id": id, "src": src}}
        if display_scope:
            payload['script_tag']['display_scope'] = display_scope
        script_tags_response = self.authenticated_shopify_call(call_path=call_path, method=method, payload=payload)
        if not script_tags_response:
            return None
        return script_tags_response['script_tag']

    def create_script_tag(self, src: str, event: str = 'onload', display_scope: str = None) -> int:
        call_path = f'script_tags.json'
        method = 'POST'
        payload = {'script_tag': {'event': event, 'src': src}}
        if display_scope:
            payload['script_tag']['display_scope'] = display_scope
        script_tag_response = self.authenticated_shopify_call(call_path=call_path, method=method, payload=payload)
        if not script_tag_response:
            return None
        return script_tag_response['script_tag']

    def delete_script_tag(self, script_tag_id: int) -> int:
        call_path = f'script_tags/{script_tag_id}.json'
        method = 'DEL'
        script_tag_response = self.authenticated_shopify_call(call_path=call_path, method=method)
        if script_tag_response is None:
            return False
        return True

    def create_usage_charge(self, recurring_application_charge_id: int, description: str, price: float) -> dict:
        call_path = f'recurring_application_charges/{recurring_application_charge_id}/usage_charges.json'
        method = 'POST'
        payload = {'usage_charge': {'description': description, 'price': price}}
        usage_charge_response = self.authenticated_shopify_call(call_path=call_path, method=method, payload=payload)
        if not usage_charge_response:
            return None
        return usage_charge_response['usage_charge']

    def get_recurring_application_charges(self) -> List:
        call_path = 'recurring_application_charges.json'
        method = 'GET'
        recurring_application_charges_response = self.authenticated_shopify_call(call_path=call_path, method=method)
        if not recurring_application_charges_response:
            return None
        return recurring_application_charges_response['recurring_application_charges']

    def delete_recurring_application_charges(self, recurring_application_charge_id: int) -> bool:
        # Broken currently,authenticated_shopify_call expects JSON but this returns nothing
        call_path = f'recurring_application_charges/{recurring_application_charge_id}.json'
        method = 'DEL'
        delete_recurring_application_charge_response = self.authenticated_shopify_call(call_path=call_path, method=method)
        if delete_recurring_application_charge_response is None:
            return False
        return True

    def activate_recurring_application_charge(self, recurring_application_charge_id: int) -> dict:
        call_path = f'recurring_application_charges/{recurring_application_charge_id}/activate.json'
        method = 'POST'
        payload = {}
        recurring_application_charge_activation_response = self.authenticated_shopify_call(call_path=call_path, method=method, payload=payload)
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

        call_path = f'webhooks.json'
        method = 'POST'
        payload = {
            "webhook": {
                "topic": topic,
                "address": address,
                "format": "json"
            }
        }
        webhook_response = self.authenticated_shopify_call(call_path=call_path, method=method, payload=payload)
        if not webhook_response:
            return None
        return webhook_response['webhook']

    def get_webhooks_count(self, topic: str):
        call_path = f'webhooks/count.json?topic={topic}'
        method = 'GET'
        webhook_count_response = self.authenticated_shopify_call(call_path=call_path, method=method)
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


