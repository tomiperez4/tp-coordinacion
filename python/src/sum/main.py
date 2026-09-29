import os
import logging
import threading
import zlib
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.control_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )

        self.control_consume = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )
        
        self.amount_by_client = {} # Amount of fruits received from each client
        self.processed = {} # Amount of MESSAGES processed from each client
        self.closing = {} # Indicates if EOF from certain client has been received
        self.count_by_client = {} # Takes count of all sum's processed messages
        self.lock = threading.Lock() # Serialize access to shared data

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")

        with self.lock:
            fruits = self.amount_by_client.setdefault(client_id, {})
            self.processed[client_id] = self.processed.get(client_id, 0) + 1

            fruits[fruit] = fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

            processed = self.processed[client_id]
            total = self.closing.get(client_id)

        if total is not None:
            message = message_protocol.internal.serialize(
                [client_id, ID, processed, total]
            )
            self.control_publisher.send(message)

    def _process_eof(self, client_id, total):
        with self.lock:
            processed = self.processed.get(client_id, 0)
            self.closing[client_id] = total

        self.control_publisher.send(
            message_protocol.internal.serialize(
                [client_id, ID, processed, total]
            )
        )

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def _send_eof_to_aggregator(self, client_id):
        with self.lock:
            fruits = self.amount_by_client.pop(client_id, {})
            self.processed.pop(client_id, None)
            self.closing.pop(client_id, None)
        self.count_by_client.pop(client_id, None)

        for final_fruit_item in fruits.values():
            idx = zlib.crc32(final_fruit_item.fruit.encode()) % AGGREGATION_AMOUNT

            self.data_output_exchanges[idx].send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def process_control_message(self, message, ack, nack):
        logging.info(f"Control message")

        client_id, sum_id, count, total = message_protocol.internal.deserialize(message)

        sum_count = self.count_by_client.setdefault(client_id, {})
        sum_count[sum_id] = max(sum_count.get(sum_id, 0), count)

        with self.lock:
            first_time = client_id not in self.closing
            if first_time:
                self.closing[client_id] = total
            own_count = self.processed.get(client_id, 0)

        if first_time and own_count > 0:
            self.control_consume.send(
                message_protocol.internal.serialize(
                    [client_id, ID, own_count, total]
                )
            )

        if total == sum(sum_count.values()):
            self._send_eof_to_aggregator(client_id)

        ack()

    def _handle_sigterm(self):
        self.control_consume.stop_consuming()
        self.input_queue.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, lambda signum, frame: self._handle_sigterm())
        
        t_control = threading.Thread(target=self.start_control_thread)
        t_control.start()

        try:
            self.input_queue.start_consuming(self.process_data_messsage)

        finally:
            self.control_consume.stop_consuming()
            self.input_queue.close()
            self.control_publisher.close()
            t_control.join()

    def start_control_thread(self):
        try:
            self.control_consume.start_consuming(self.process_control_message)

        finally:
            self.input_queue.stop_consuming()
            self.control_consume.close()
            for exchange in self.data_output_exchanges:
                exchange.close()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
