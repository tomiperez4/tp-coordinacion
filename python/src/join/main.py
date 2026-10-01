import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

PARTIAL_TOP_OPCODE = message_protocol.internal.Opcode.PartialTop
FRUIT_TOP_OPCODE = message_protocol.internal.Opcode.FruitTop

class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.client_fruit_top = {} # Maintaints global top for each client
        self.eof = {} # Aggregation IDs that sent their partial top for each client

    def process_messsage(self, message, ack, nack):
        opcode, fields = message_protocol.internal.deserialize(message)

        if opcode == PARTIAL_TOP_OPCODE:
            self._process_partial_top(*fields)
        
        ack()

    def _process_partial_top(self, client_id, aggregation_id, partial_top):
        senders = self.eof.setdefault(client_id, set())
        if aggregation_id in senders:
            logging.info(f"Duplicated partial top from aggregation {aggregation_id} for client {client_id}")
            return
        senders.add(aggregation_id)
        logging.info(
            f"Received partial top from aggregation {aggregation_id} for client {client_id} ({len(senders)}/{AGGREGATION_AMOUNT})"
        )

        self.client_fruit_top.setdefault(client_id, [])
        for fruit, amount in partial_top:
            bisect.insort(self.client_fruit_top[client_id], fruit_item.FruitItem(fruit, amount))

        if len(senders) == AGGREGATION_AMOUNT:
            self.eof.pop(client_id)
            fruits = self.client_fruit_top.pop(client_id, [])
            fruit_chunk = list(fruits[-TOP_SIZE:])
            fruit_chunk.reverse()
            fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                    fruit_chunk,
                )
            )

            self.output_queue.send(message_protocol.internal.serialize(
                [FRUIT_TOP_OPCODE, client_id, fruit_top]
            ))
        

    def _handle_sigterm(self):
        self.input_queue.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, lambda signum, frame: self._handle_sigterm())

        try:
            self.input_queue.start_consuming(self.process_messsage)

        finally:
            self.input_queue.close()
            self.output_queue.close()

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
