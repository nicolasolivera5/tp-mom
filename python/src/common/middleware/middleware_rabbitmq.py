import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        pass

    def start_consuming(self, on_message_callback):

		try:
			connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
			channel = connection.channel()

			channel.queue_declare(queue=queue_name, durable=True)

			on_message_callback(message, ack, nack)

			channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback)
			channel.start_consuming()

		except pika.exceptions.AMQPConnectionError as e:
			raise MessageMiddlewareDisconnectedError(f"Error connecting to message broker: {str(e)}")
		except Exception as e:
			raise MessageMiddlewareMessageError(f"Error consuming message: {str(e)}")

    def stop_consuming(self):

        try:
            if channel.is_open:
                channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error disconnecting from message broker: {str(e)}")
            

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass

    def start_consuming(self, on_message_callback):

		try:
			connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
			channel = connection.channel()

			channel = exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)

			result = channel.queue_declare(queue='', durable=True)
			queue_name = result.method.queue

			for routing_key in routing_keys:
				channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

			on_message_callback(message, ack, nack)

			channel.basic_consume(queue=queue_name, on_message_callback=on_message_callback)
			channel.start_consuming()
		
		except pika.exceptions.AMQPConnectionError as e:
			raise MessageMiddlewareDisconnectedError(f"Error connecting to message broker: {str(e)}")
		except Exception as e:
			raise MessageMiddlewareMessageError(f"Error consuming message: {str(e)}")

    def stop_consuming(self):

        try:
            if channel.is_open:
                channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error disconnecting from message broker: {str(e)}")


    

    
