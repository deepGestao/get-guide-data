import boto3
import os
import json
from trp.trp2_expense import TAnalyzeExpenseDocumentSchema

client = boto3.client('textract')

def call_textract_expense(bucket, key):
  response = client.analyze_expense(
    Document={'S3Object': {'Bucket': bucket, 'Name': key}}
  )
  return response

def call_textract(bucket, key):
  j = call_textract_expense(bucket, key)
  t_doc = TAnalyzeExpenseDocumentSchema().load(j)
  return t_doc

def parse_fields(table_fields, summary_fields, normalized_fields):
  return {
    "guideId": [f[key] for f in normalized_fields for key in f if key == 'INVOICE_RECEIPT_ID'][0],
    "name": [f[key] for f in normalized_fields for key in f if key == 'RECEIVER_NAME'][0],
    "value": [f[key] for f in normalized_fields for key in f if key == 'SUBTOTAL'][0],
    "covenantName": [f[key] for f in normalized_fields for key in f if key == 'VENDOR_NAME'][0],
    "authorizationDate": [f[key] for f in summary_fields for key in f if key.startswith('4. Data de Autorização')][0],
    "dueDate": [f[key] for f in summary_fields for key in f if key.startswith("6 Data de validade da Senha")][0],
    "clientId": [f[key] for f in summary_fields for key in f if key.startswith('8 Número da carteira')][0],
    "treatment": list(map(lambda x: {"code": x['PRODUCT_CODE'], "label": x['ITEM'], "value": x['PRICE'], "quantity": x['QUANTITY']}, table_fields))
  }

def get_fields(document):

  table_fields = []
  normalized_fields_parsed = []
  summary_fields = []

  for expense in document.expenses_documents:
    for field in expense.summaryfields:
      key = field.labeldetection.text if field.labeldetection else 'Unknown'
      value = field.valuedetection.text if field.valuedetection else 'No value detected'
      summary_fields.append({key: value})

    for line_item_group in expense.lineitemgroups:
      for line_item in line_item_group.lineitems:
        newLine = {}
        for expense_field in line_item.lineitem_expensefields:
          key = expense_field.ftype.text if expense_field.ftype else 'Unknown'
          value = expense_field.valuedetection.text if expense_field.valuedetection else 'No value detected'
          newLine[key] = value
        table_fields.append(newLine)

  normalized_fields = document.get_normalized_summaryfields_by_expense_id(expense.expense_idx)
  if normalized_fields:
    for field in normalized_fields:
      if field.valuedetection:
        key = field.ftype.text if field.ftype else "UNKNOWN"
        value = field.valuedetection.text
        normalized_fields_parsed.append({key: value})

  return parse_fields(table_fields, summary_fields, normalized_fields_parsed)

def lambda_handler(event, context):
  bucket = os.getenv('BUCKET_NAME')
  file_name = event['queryStringParameters']['filename']
  #file_name = '0bbc35b1-198c-4538-ad56-ba3ffe265330.pdf'
  document = call_textract(bucket, file_name)
  fields = get_fields(document)
  return {
    "statusCode": 200,
    "body": json.dumps(fields),
    "headers": {
      "Access-Control-Allow-Origin": os.getenv('ACCESS_CONTROL_ALLOW_ORIGIN'),
      "Access-Control-Allow-Methods": os.getenv('ACCESS_CONTROL_ALLOW_METHODS'),
      "Access-Control-Allow-Headers": os.getenv('ACCESS_CONTROL_ALLOW_HEADERS')
    }
  }