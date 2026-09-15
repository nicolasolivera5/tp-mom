from abc import ABC, abstractmethod

class MessageMiddlewareMessageError(Exception):
    pass

class MessageMiddlewareDisconnectedError(Exception):
    pass

class MessageMiddlewareCloseError(Exception):
    pass

class MessageMiddlewareDeleteError(Exception):
    pass

class MessageMiddleware(ABC):

	#Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
	#cada mensaje de datos o de control con el cuerpo del mensaje.
	# on_message_callback tiene como parámetros:
	# message - El valor tal y como lo recibe el método send de esta clase.
	# ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
	# nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	@abstractmethod
	def start_consuming(self, on_message_callback):
		pass
	
	#Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	#no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	#Si se pierde la conexión con el midleware eleva MessageMiddlewareDisconnectedError.
	@abstractmethod
	def stop_consuming(self):
		pass
	
	#Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	@abstractmethod
	def send(self, message):
		pass

	#Se desconecta de la cola o exchange al que estaba conectado.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
	@abstractmethod
	def close(self):
		pass


class MessageMiddlewareExchange(MessageMiddleware):
	@abstractmethod
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


class MessageMiddlewareQueue(MessageMiddleware):
	@abstractmethod
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
			
