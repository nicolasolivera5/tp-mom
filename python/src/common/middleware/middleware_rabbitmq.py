import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareCloseError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def start_consuming(self, on_message_callback):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)

            def callback(ch, method, properties, body):
                ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
                nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
                on_message_callback(body, ack, nack)

            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self.channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error connecting to message broker: {str(e)}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error consuming message: {str(e)}")

    def stop_consuming(self):
        try:
            if self.channel is not None and self.channel.is_open:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error disconnecting from message broker: {str(e)}")

    def send(self, message):
        try:
            if (self.connection is None or self.connection.is_closed or 
                self.channel is None or self.channel.is_closed):
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue_name, durable=True)

            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error connecting to message broker: {str(e)}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error sending message: {str(e)}")

    def close(self):
        try:
            if self.channel is not None and self.channel.is_open:
                self.channel.close()
            if self.connection is not None and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error closing connection: {str(e)}")


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = None
        self.channel = None

    def start_consuming(self, on_message_callback):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()

            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic', durable=True)

            result = self.channel.queue_declare(queue='', durable=True, exclusive=True)
            queue_name = result.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=routing_key)

            def callback(ch, method, properties, body):
                ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
                nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
                on_message_callback(body, ack, nack)

            self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
            self.channel.start_consuming()
        
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error connecting to message broker: {str(e)}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error consuming message: {str(e)}")

    def stop_consuming(self):
        try:
            if self.channel is not None and self.channel.is_open:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error disconnecting from message broker: {str(e)}")

    def send(self, message):
        try:
            if (self.connection is None or self.connection.is_closed or 
                self.channel is None or self.channel.is_closed):
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
                self.channel = self.connection.channel()
                self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic', durable=True)

            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error connecting to message broker: {str(e)}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error sending message: {str(e)}")

    def close(self):
        try:
            if self.channel is not None and self.channel.is_open:
                self.channel.close()
            if self.connection is not None and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error closing connection: {str(e)}")
