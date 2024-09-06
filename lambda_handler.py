import boto3
from collections import defaultdict
from urllib.parse import unquote_plus
import json
import os

def get_kv_map(bucket, key, queries):
    # process using image bytes
    client = boto3.client('textract')
    query_list = [{'Text': query['Text']} for query in queries]
    response = client.analyze_document(Document={'S3Object': {'Bucket': bucket, "Name": key}}, FeatureTypes=['QUERIES'], QueriesConfig={'Queries': query_list})
    blocks = response['Blocks']

    # get key and value maps
    key_map = {}
    value_map = {}
    block_map = {}
    for block in blocks:
        block_id = block['Id']
        block_map[block_id] = block
        if block['BlockType'] == "QUERY":
            key_map[block_id] = block
        elif block['BlockType'] == "QUERY_RESULT":
            value_map[block_id] = block

    return key_map, value_map, block_map


def get_kv_relationship(key_map, value_map, block_map):
    kvs = defaultdict(list)
    for block_id, key_block in key_map.items():
        value_block = find_value_block(key_block, value_map)
        key = get_text(key_block)
        val = get_text(value_block)
        kvs[key].append(val)
    return kvs


def find_value_block(key_block, value_map):
    for relationship in key_block['Relationships']:
        if relationship['Type'] == 'ANSWER':
            for value_id in relationship['Ids']:
                value_block = value_map[value_id]
    return value_block


def get_text(result):
    text = ''
    if result['BlockType'] == "QUERY":
        text = result['Query']['Text']
    elif result['BlockType'] == "QUERY_RESULT":
        text = result['Text']

    return text

def lambda_handler(event, context):
    bucket = os.getenv('BUCKET_NAME')
    file_name = event['queryStringParameters']['filename']
    print(bucket)
    print(file_name)
    print(event)
    queries = [
        {"Text": "What is numero guia no prestador?", "Key": "guideId"},
        {"Text": "What is data da autorizacao?", "Key": "authorizationDate"},
        {"Text": "What is valor total?", "Key": "value"},
        {"Text": "What is nome?", "Key": "name"},
        {"Text": "What is data de validade?", "Key": "dueDate"},
        {"Text": "What is the name of the company that issued the document?", "Key": "covenantName"}
    ]
    key_map, value_map, block_map = get_kv_map( bucket, file_name, queries)

    # Get Key Value relationship
    kvs = get_kv_relationship(key_map, value_map, block_map)
    response = {}
    for key, value in kvs.items():
        index = [q['Key'] for q in queries if q['Text'] == key][0]
        response[index] = value[0]
    print(response)
    return {
        "statusCode": 200,
        "body": json.dumps(response),
        "headers": {
            "Access-Control-Allow-Origin": os.getenv('ACCESS_CONTROL_ALLOW_ORIGIN'),
            "Access-Control-Allow-Methods": os.getenv('ACCESS_CONTROL_ALLOW_METHODS'),
            "Access-Control-Allow-Headers": os.getenv('ACCESS_CONTROL_ALLOW_HEADERS')
        }
    }
