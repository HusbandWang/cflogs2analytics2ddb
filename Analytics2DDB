import base64
import boto3
import json

ddb_client = boto3.client('dynamodb', region_name='us-east-1')

def lambda_handler(event, context):
    i = 1
    data = []
    output = []
    for record in event['records']:
        record_decode = base64.b64decode(record['data'])
        record_json = json.loads(record_decode)
        data.append(record_json)
        output.append({'recordId': record['recordId'], 'result': 'Ok'})
    for item in data:
        print item
        response = ddb_client.put_item(
            TableName='TopN_User',
            Item={
                'TopN': {'S': 'Top'+str(i)},
                'Host': {'S': item['REQUESTIP']},
                'Count': {'N': str(item['REQUESTCOUNT'])}
                }
        )
        i +=1
    return {'records': output}
        
        
   
