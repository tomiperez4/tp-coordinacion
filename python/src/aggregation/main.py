import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

EOF_OPCODE = message_protocol.internal.Opcode.EOF
FRUITS_OPCODE = message_protocol.internal.Opcode.Fruits
PARTIAL_TOP_OPCODE = message_protocol.internal.Opcode.PartialTop

class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top = {}
        self.eof = {} # Sum IDs that sent EOF for each client

    def _process_data(self, client_id, sum_id, fruit, amount):
        logging.info(f"Processing data message from client {client_id} (sum {sum_id})")
        fruits = self.fruit_top.setdefault(client_id, [])
        for i in range(len(fruits)):
            if fruits[i].fruit == fruit:
                updated = fruits.pop(i) + fruit_item.FruitItem(fruit, amount)
                bisect.insort(fruits, updated)
                return

        bisect.insort(fruits, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id, sum_id):
        senders = self.eof.setdefault(client_id, set())
        senders.add(sum_id)
        logging.info(
            f"Received EOF from sum {sum_id} for client {client_id} ({len(senders)}/{SUM_AMOUNT})"
        )

        if len(senders) == SUM_AMOUNT:
            del self.eof[client_id]
            fruits = self.fruit_top.pop(client_id, [])
            fruit_chunk = list(fruits[-TOP_SIZE:])
            fruit_chunk.reverse()
            fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                    fruit_chunk,
                )
            )
            self.output_queue.send(message_protocol.internal.serialize(
                [PARTIAL_TOP_OPCODE, client_id, ID, fruit_top]
            ))

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        opcode, fields = message_protocol.internal.deserialize(message)
        if opcode == FRUITS_OPCODE:
            self._process_data(*fields)
        elif opcode == EOF_OPCODE:
            self._process_eof(*fields)
        ack()

    def _handle_sigterm(self):
        self.input_exchange.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, lambda signun, frame: self._handle_sigterm())

        try:
            self.input_exchange.start_consuming(self.process_messsage)

        finally:
            self.input_exchange.close()
            self.output_queue.close()

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
