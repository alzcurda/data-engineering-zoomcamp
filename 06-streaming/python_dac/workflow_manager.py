import kafka_mod
import producer
from data_mod import leer_csv_comprimido, seleccionar_columnas

if __name__ == "__main__":
    # Ruta del archivo CSV comprimido
    ruta_archivo = '../data/green_tripdata_2019-10.csv.gz'

    # Leer el archivo CSV comprimido y guardar solo las columnas deseadas en un DataFrame
    df_green = leer_csv_comprimido(ruta_archivo)

    if df_green is not None:
        # Seleccionar solo las columnas deseadas
        df_green_seleccionado = seleccionar_columnas(df_green)

        # Mostrar las primeras filas del DataFrame resultante
        print("Contenido del DataFrame df_green_seleccionado:")
        print(df_green_seleccionado.head())

        producer.enviar_mensajes(df_green_seleccionado, kafka_mod.get_producer())

    else:
        print("No se pudo leer el archivo CSV comprimido.")
