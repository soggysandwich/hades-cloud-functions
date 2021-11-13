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
    import uuid
    import re
    import json
    from datetime import datetime, timezone
    from google.cloud import datastore
    datastore_client = datastore.Client()

    # The kind(table) to save data in datastore and unique key as id
    kind = "ebay-adverts"
    unique_id = str(uuid.uuid4())

    # bucket to save images
    bucket = 'hades-bucket'

    pattern = re.compile('[~\-a-zA-Z0-9]{16}')

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
        advert_dict = json.loads(data)

    else:
        print(f'Error: There was no data for {context.event_id} !')
        advert_dict = {}

    dictionary_advert = unpack_dictionary(advert_dict, key_names)
    # The Cloud Datastore key for the new entity
    advert_key = datastore_client.key(kind, unique_id)
    advert = datastore.Entity(key=advert_key, exclude_from_indexes=(
        'condition', 'listingInfo', 'primaryCategory', 'sellingStatus', 'shippingInfo', 'sellerInfo'))

    if 'galleryURL' in dictionary_advert:
        gallery_url = dictionary_advert['galleryURL']
        image_id = pattern.search(gallery_url)

        if image_id and ('itemId' in dictionary_advert):
            image_url = f'https://i.ebayimg.com/images/g/{image_id.group()}/s-l1600.jpg'
            get_images(image_url, unique_id, bucket)  # unique_id is the same as the datastore key for name of image

        for key, value in dictionary_advert.items():
            advert[key] = value
            advert['found_date'] = datetime.now(timezone.utc)

        datastore_client.put(advert)


def get_images(image_url, blob_name, bucket):
    from google.cloud import storage
    import requests
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)

    response = requests.get(image_url)
    # print(f' response code : {response.status_code}')
    if response.status_code == 200:
        blob = bucket.blob(blob_name)
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
            find_items_by_keyword_dict = data['findItemsByKeywordsResponse'][0]

            search_results = find_items_by_keyword_dict['searchResult'][0]['item']

            for advert in search_results:
                advert = json.dumps(advert)

                advert = advert.encode('utf-8')

                # print(f'advert is : {advert}')

                # result = publisher.publish(topic, data, **attributes)
                result = publisher.publish(topic, advert)
                # print(f'published message {result.result()}')


def analyse_image(event, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This function takes an image from google-storage and sends to vision API and saves results in datastore
    Args:
        event (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:

    """
    from google.cloud import vision
    from google.cloud import datastore
    labels = {}
    kind = 'ebay-adverts'
    print(f"Name : {event['name']}")

    # print(f"gs://{event['bucket']}/{event['name']}")

    vision_client = vision.ImageAnnotatorClient()
    image = vision.Image()
    image.source.image_uri = f"gs://{event['bucket']}/{event['name']}"

    response = vision_client.label_detection(image)
    logo_response = vision_client.logo_detection(image)

    for label in response.label_annotations:
        if label.score >= 0.80:
            labels[label.description] = label.score

    for logo in logo_response.logo_annotations:
        if logo.score >= 0.80:
            labels[logo.description] = logo.score
            # print(f'logo description: {logo.description}    confidence level: {logo.score}  ')

    if len(labels) > 0:
        client = datastore.Client()
        key = client.key(kind, str(event['name']))
        advert = client.get(key)
        print(f' entity {advert}')
        print(f' labels {labels}')
        advert['MachineLearning'] = labels
        client.put(advert)
