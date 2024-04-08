import time


def enviar_mensajes(df_mensajes, producer):
    """
    Envia cada registro del df en formato diccionario al producer de Kafka
    :param df_mensajes: df con los mensajes a enviar
    :param producer: producer del servidor Karka dónde queremos enviar el mensaje.
    :return:
    """
    t0 = time.time()

    topic_name = 'green-trips'
    print(f"Sending messages to topic: {topic_name} ...")
    for row in df_mensajes.itertuples(index=False):
        message = {col: getattr(row, col) for col in row._fields}
        producer.send(topic_name, value=message)
        # print(f"Sent: {message}")
        # break

    t1 = time.time()
    producer.flush()
    t2 = time.time()
    print(f'Total messages sent: {(df_mensajes.shape[0]):.2f} rows')
    print(f'Sending the messages time took: {(t1 - t0):.2f} seconds')
    print(f'Flushing time took: {(t2 - t1):.2f} seconds')
