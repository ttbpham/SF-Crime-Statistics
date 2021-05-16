"""Defines core consumer functionality"""
import logging

from kafka import KafkaConsumer
from json import loads

logger = logging.getLogger(__name__)


consumer = KafkaConsumer(
    'gov.department.police.sf.crime',
     bootstrap_servers=['localhost:9093'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

logger.info("start consuming data")

for message in consumer:
    msg = message.value
    print(msg)

def close(self):
    """Cleans up any open kafka consumers"""
    logger.info("close consumer")
    self.consumer.close()
