def save_advert(event, context):
    """Background Cloud Function to be triggered by Pub/Sub API Results

        function will take an advert from event["data"]  and save it into datastore using Kind of ebay-adverts

    Args:
         event (dict):  The dictionary with data specific to this type of
                        event. The `@type` field maps to
                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                        The `data` field maps to the PubsubMessage data
                        in a base64-encoded string. The `attributes` field maps
                        to the PubsubMessage attributes if any is present.
         context (google.cloud.functions.Context): Metadata of triggering event
                        including `event_id` which maps to the PubsubMessage
                        messageId, `timestamp` which maps to the PubsubMessage
                        publishTime, `event_type` which maps to
                        `google.pubsub.topic.publish`, and `resource` which is
                        a dictionary that describes the service API endpoint
                        pubsub.googleapis.com, the triggering topic's name, and
                        the triggering event type
                        `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
    Returns:
        None. The output is written to Cloud Logging.
    """
    import base64
    import json

    from datetime import datetime, timezone
    from google.cloud import datastore
    datastore_client = datastore.Client()

    # The kind(table) to save data in datastore
    kind = "ebay-adverts"

    # bucket to save images
    bucket = 'hades-bucket'

    # The Cloud Datastore key for the new entity
    advert_key = datastore_client.key(kind)

    # Prepares the new entity
    advert = datastore.Entity(key=advert_key, exclude_from_indexes=(
        'condition', 'listingInfo', 'primaryCategory', 'sellingStatus', 'shippingInfo'))

    # setup the list to capture the gallery urls
    gallery_images = []

    key_names = {
        "price_key": "sellingStatus",
        "current_price_key": "currentPrice",
        "value_key": "__value__",
        "currency_key": "@currencyId",
        "seller_key": "sellerInfo"

    }

    # get the string data from the event and convert into a dictionary
    if 'data' in event:
        data = base64.b64decode(event['data']).decode('utf-8')
        print(f'the data is {data}')
        adverts = json.loads(data)

    else:
        print(f'Error: There was no data for {context.event_id} !')
        adverts = {}

    find_items_by_keyword_list = adverts['findItemsByKeywordsResponse']
    # print(find_items_by_keyword_list)

    for search_result in find_items_by_keyword_list:

        search_dictionary = (search_result['searchResult'][0])

        # list of all the adverts and batch update datastore
        advert_list = (search_dictionary['item'])

        with datastore_client.batch() as batch:
            # get every advert and save to the datastore as a batch
            for advert_dictionary in advert_list:
                # print(advert_dictionary)
                dictionary_advert = unpack_dictionary(advert_dictionary, key_names)
                advert = datastore.Entity(key=advert_key, exclude_from_indexes=(
                    'condition', 'listingInfo', 'primaryCategory', 'sellingStatus', 'shippingInfo'))

                # append gallery url so can collect them and save to bucket

                if 'galleryURL' in dictionary_advert:
                    gallery_images.append(advert_dictionary['galleryURL'][0])

                # unpack and append seller info

                if 'sellerInfo' in dictionary_advert:
                    seller = advert_dictionary['sellerInfo'][0]
                    advert['sellerUserName'] = ''.join(seller['sellerUserName'])
                    advert['feedbackScore'] = ''.join(seller['feedbackScore'])
                    advert['positiveFeedbackPercent'] = ''.join(seller['positiveFeedbackPercent'])

                for key, value in dictionary_advert.items():
                    # print(f'{key}:{value}')
                    advert[key] = value

                advert['found_date'] = datetime.now(timezone.utc)
                batch.put(advert)

    get_images(gallery_images, bucket, '-')


def get_images(gallery_images, bucket, folder_safe_name_replace):
    from google.cloud import storage
    import requests
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)

    for image in gallery_images[0:10]:
        response = requests.get(image)

        blob = bucket.blob(response.url.replace('/', folder_safe_name_replace))
        blob.upload_from_string(response.content)


def unpack_dictionary(dictionary, key_names):
    """ Function that unpacks an dictionary returned by Ebay


    Args:
         dictionary (dict):  The dictionary with data specific the the ebay search returned by ebat API.
         key_names (dict) : Specific dictionary values that are nested in lists and dictionaries (e.g price)

    Returns:
        key_values(dict) the unpacked values (strings)

"""

    key_values = {}

    if key_names['seller_key'] in dictionary:
        nested_dictionary = dictionary.get(key_names['seller_key'])[0]
        for seller_key in nested_dictionary:
            key_values[seller_key] = ''.join(str(element) for element in nested_dictionary.get(seller_key))

    if key_names['price_key'] in dictionary:
        nested_dict = dictionary.get(key_names['price_key'])[0]

        if key_names['current_price_key'] in nested_dict:
            # print(nested_dict)
            next_nested_dictionary = nested_dict.get(key_names['current_price_key'])[0]
            key_values["price"] = next_nested_dictionary[key_names['value_key']]
            key_values["currency"] = next_nested_dictionary[key_names['currency_key']]

    for key in dictionary:
        key_values[key] = ''.join(str(element) for element in dictionary.get(key))

    return key_values


def request_api(event, context):
    """Background Cloud Function to be triggered by Pub/Sub - search keywords TOPIC

        Function will call the ebay API using the supplied keywords in event["data"]
        Results passed back are sent to pub-sub TOPIC - search results

        Args:
             event (dict):  The dictionary with data specific to this type of
                            event.
             context (google.cloud.functions.Context)
        Returns:
            None. The output is written to pub sub
        """
    import requests
    import json
    import base64
    import urllib.parse
    from google.cloud import pubsub_v1
    import os

    publisher = pubsub_v1.PublisherClient()
    topic = 'projects/hades-cloud-330810/topics/api-results'

    app_name = os.environ.get("APPNAME")

    keywords = []

    # extract keywords from event["data"]
    if 'data' in event:
        data = base64.b64decode(event['data']).decode('utf-8')
        print(f'the data is {data}')
        sent_keywords = json.loads(data)

    else:
        print(f'Error: There was no data for {context.event_id} !')
        sent_keywords = {}

    for key in sent_keywords:
        value = sent_keywords.get(key)
        keywords.append(value)
        print(f'keywords are {keywords}')

        # call api
        if len(keywords) > 0:
            encoded_keywords = urllib.parse.quote(keywords[0])

            url = f'https://svcs.ebay.com/services/search/FindingService/v1?OPERATION-NAME=findItemsByKeywords' \
                  f'&SERVICE-VERSION=1.0.0&SECURITY-APPNAME={app_name}&RESPONSE-DATA' \
                  f'-FORMAT=JSON&REST-PAYLOAD&outputSelector=SellerInfo&keywords={encoded_keywords}'

            response = requests.get(url)

            data = response.json()
            data = json.dumps(data)
            data = data.encode('utf-8')

            # result = publisher.publish(topic, data, **attributes)
            result = publisher.publish(topic, data)
            print(f'published message {result.result()}')
