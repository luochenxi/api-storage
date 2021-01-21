import logging
import boto3
from retrying import retry
import awswrangler as wr


import config as cf

logger = logging.getLogger(__name__)

@retry(stop_max_attempt_number=5, wait_fixed=2)
def update_dynamodb(**kwg):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(cf.BREADTH_TABLE_NAME)
    # 添加数据/如果数据存在就更新数据
    resp = table.update_item(
        Key={
            'hash_key': 'BREADTH_{}_{}'.format("US",kwg['symbol']),
            'date': kwg['date']
        },
        UpdateExpression='SET o = :o ,c = :c,n = :n, symbol = :symbol, i18n = :i18n, data_type = :data_type, updatedAt = :updatedAt',
        ExpressionAttributeValues={
            ':o': kwg['o'],
            ':c': kwg['c'],
            ':n': kwg['name'],
            ':symbol': kwg['symbol'],
            ':i18n': kwg['i18n'],
            ':data_type': kwg['data_type'],
            ':updatedAt': kwg['updatedAt'],
        },
        ReturnValues="UPDATED_NEW"
    )
    logging.info(resp)
    if 'Attributes' not in resp:
        raise IOError


def import_data():
    # filepath = Path("export.json")
    # df.to_json(filepath, orient="records")
    # wr.dynamodb.
    wr.dynamodb.put_json(path="../export.json", table_name="Breadth-pro")
    # filepath.unlink()

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