import json

from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


server = 'localhost:9092'


def get_producer():
    """
    Devuelve una conexión de productor a un servidor Kafka.

    Retorna:
        Producer: Objeto de productor conectado al servidor Kafka.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=[server],
            value_serializer=json_serializer
        )
        return producer
    except Exception as e:
        print(f"Error al establecer la conexión del productor: {e}")
        return None
