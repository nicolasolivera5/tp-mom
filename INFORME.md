## Decisiones de diseño

### Manejo de excepciones en `middleware_rabbitmq.py`

Se utilizó `contextlib.contextmanager` para abstraer el código repetido de manejo de excepciones en las clases `MessageMiddlewareQueueRabbitMQ` y `MessageMiddlewareExchangeRabbitMQ` (uso validado en el foro de la materia).

Se exploraron también implementaciones con una clase auxiliar `RabbitMQConnection` para reducir la duplicación de código entre ambas clases. Se probó tanto con composición (instanciando la clase directamente en el constructor, como sugirió el docente en el foro) como combinándola con los context managers. Sin embargo, se optó por utilizar únicamente los contextmanagers ya que la introducción de una nueva clase hacía el código más difícil de seguir sin brindar una reducción significativa en el volumen del código.