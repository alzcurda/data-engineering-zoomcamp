import pandas as pd


def leer_csv_comprimido(ruta_archivo):
    """
    Lee un archivo CSV comprimido en formato .gz y lo guarda en un DataFrame de pandas.

    Parámetros:
        ruta_archivo (str): La ruta del archivo CSV comprimido.

    Retorna:
        DataFrame: El contenido del archivo CSV en un DataFrame.
    """
    try:
        df = pd.read_csv(ruta_archivo, compression='gzip')
        return df
    except FileNotFoundError:
        print(f"El archivo {ruta_archivo} no fue encontrado.")
        return None
    except Exception as e:
        print(f"Ocurrió un error al leer el archivo {ruta_archivo}: {str(e)}")
        return None


def seleccionar_columnas(df):
    """
    Selecciona las columnas especificadas de un DataFrame de pandas.

    Parámetros:
        df (DataFrame): El DataFrame original.

    Retorna:
        DataFrame: El DataFrame con solo las columnas seleccionadas.
    """
    columnas_deseadas = [
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'tip_amount'
    ]
    return df[columnas_deseadas]