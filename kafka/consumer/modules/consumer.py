import logging

import json
import time
from datetime import datetime
from kafka import KafkaConsumer

from . import database


class Consumer(KafkaConsumer):
    """
    Consumer with DB Loader
    Kafka Consumer wrapped with Postgres Data Loader functionality.
    Inherited from KafkaConsumer.
    params:
    - bootstrap_servers: str. Kafka instance hostname (bootstrap).
    - topic: str. Topic for consumer to pull (subscribe) data.
    - db_config: dict. Database connection configuration.
    - tablename: str. Target table for final data.
    """
    def __init__(self, bootstrap_servers: str, topic: str, db_config: dict, tablename: str):
        super().__init__(
            topic,
            bootstrap_servers = [bootstrap_servers],
            value_deserializer = self._deserializer
        )
        self.active = False
        self.database = database.get_engine(**db_config)
        self.tablename = tablename
    

    # Public Methods
    def start(self):
        """
        Start Consumer
        Start consumer activity.
        """
        self.active = True
        self._consume()
    
    def stop(self):
        """
        Stop Consumer
        Stop consumer activity.
        """
        self.active = False
    

    # Private Methods
    def _consume(self):
        while(self.active):
            data = self.poll(timeout_ms=500)
            for _, messages in data.items():
                for message in messages:
                    fmt_messages = self._format_data(message.value)
                    for fmt_data in fmt_messages:
                        logging.info(f"[FORMATED_DATA]':{fmt_data}")
                        database.insert_data(self.database, fmt_data, self.tablename)

            logging.info("Fetching another batch...")
            time.sleep(10)

    def _deserializer(self, data: bytes) -> dict:
        return json.loads(data.decode("utf-8"))
    
    def _format_data(self, data: dict) -> list:
        dt = json.loads(data)
        dt = data['rates']
        currencies = {
            'EURUSD':'US Dollar',
            'EURGBP':'Pound Sterling',
            'USDEUR':'Euro'}
        all_data = []
        for id,cur_name, in currencies.item():
            fmt_data={
                'currency_id':id,
                'currency_name':cur_name,
                'rate': data[id]['rate'],
                'timestamp':datetime.fromtimestamp(data[id]['timestamp'])
            }
            all_data.append(fmt_data)
        return all_data