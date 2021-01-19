import logging
import boto3
from retrying import retry

import config as cf

logger = logging.getLogger(__name__)

@retry(stop_max_attempt_number=5, wait_fixed=2)
def update_dynamodb(**kwg):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(cf.BREADTH_TABLE_NAME)

    resp = table.update_item(
        Key={
            'symbol_country': 'US_{}'.format(kwg['symbol']),
            'date': kwg['date']
        },
        UpdateExpression='SET o = :o ,c = :c,name = :name, i18n = :i18n, country = :country, updatedAt = :updatedAt',
        ExpressionAttributeValues={
            ':o': kwg['o'],
            ':c': kwg['c'],
            ':name': kwg['name'],
            ':i18n': kwg['i18n'],
            ':country': kwg['country'],
            ':updatedAt': kwg['updatedAt'],
        },
        ReturnValues="UPDATED_NEW"
    )
    logging.info(resp)
    if 'Attributes' not in resp:
        raise IOError

if __name__ == '__main__':
    update_dynamodb(**dict(
        symbol="test",
        date="test",
        o="test",
        c="test",
        i18n="test",
        country="test",
        updatedAt="test",
    ))