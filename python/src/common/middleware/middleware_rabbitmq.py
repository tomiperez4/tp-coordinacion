from abc import ABC, abstractmethod

import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, \
    MessageMiddlewareMessageError, MessageMiddlewareCloseError, MessageMiddleware

# Clase que encapsula comportamiento comun
class MessageMiddlewareRabbitMQ(MessageMiddleware, ABC):
    def __init__(self, host):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e

        except pika.exceptions.AMQPError as e:
            if self.connection.is_open:
                self.connection.close()
            raise MessageMiddlewareMessageError from e

        self.consumer_tag = None

    @abstractmethod
    def __queue_to_consume__(self) -> str:
        pass

    def __publish__(self, exchange, routing_key, body) -> None:
        try:
            self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e

        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError from e

    def start_consuming(self, on_message_callback) -> None:
        def callback(ch, method, _properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try:
            queue_name = self.__queue_to_consume__()
            self.consumer_tag = self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
            self.channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e

        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError from e

        finally:
            self.consumer_tag = None

    def stop_consuming(self) -> None:
        if not self.consumer_tag:
            return
        try:
            self.channel.stop_consuming(self.consumer_tag)
            self.consumer_tag = None
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e

    def close(self) -> None:
        try:
            self.stop_consuming()
            if self.connection.is_open:
                # Cerrar la conexion cierra todos los canales abiertos
                self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError from e

# Implementaciones de MessageMiddlewares (Queue - Exchange)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareRabbitMQ, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        super().__init__(host)
        try:
            self.channel.queue_declare(queue=queue_name)
            self.channel.basic_qos(prefetch_count=1)
        except pika.exceptions.AMQPError as e:
            if self.connection.is_open:
                self.connection.close()
            raise MessageMiddlewareMessageError from e

        self.queue_name = queue_name

    def __queue_to_consume__(self) -> str:
        return self.queue_name

    def send(self, message) -> None:
        self.__publish__(exchange='', routing_key=self.queue_name, body=message)

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareRabbitMQ, MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        try:
            self.channel.exchange_declare(exchange=exchange_name, exchange_type="topic")
        except pika.exceptions.AMQPError as e:
            if self.connection.is_open:
                self.connection.close()
            raise MessageMiddlewareMessageError from e

        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.queue_name = None

    def __queue_to_consume__(self) -> str:
        if self.queue_name:
            return self.queue_name

        result = self.channel.queue_declare(queue='', exclusive=True)
        for key in self.routing_keys:
            self.channel.queue_bind(exchange=self.exchange_name, queue=result.method.queue, routing_key=key)

        self.queue_name = result.method.queue
        return self.queue_name

    def send(self, message) -> None:
        for key in self.routing_keys:
            self.__publish__(exchange=self.exchange_name, routing_key=key, body=message)