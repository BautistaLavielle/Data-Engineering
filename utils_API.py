import requests
import pandas as pd
from datetime import datetime

def get_data(base_url, endpoint, data_field=None, params=None, headers=None):
    """
    Realiza una solicitud GET a una API para obtener datos.

    Parámetros:
    base_url (str): La URL base de la API.
    endpoint (str): El endpoint de la API al que se realizará la solicitud.
    data_field (str): Atribudo del json de respuesta donde estará la lista
    de objetos con los datos que requerimos
    params (dict): Parámetros de consulta para enviar con la solicitud.
    headers (dict): Encabezados para enviar con la solicitud.

    Retorna:
    dict: Los datos obtenidos de la API en formato JSON.
    """
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.

        # Verificar si los datos están en formato JSON.
        try:
            data = response.json()
            if data_field:
                data = data[data_field]
        except:
            print("El formato de respuesta no es el esperado")
            return None
        return data

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        print(f"La petición ha fallado. Código de error : {e}")
        return None

def build_table(json_data, record_path=None):
    """
    Construye un DataFrame de pandas a partir de datos en formato JSON.

    Parámetros:
    json_data (dict): Los datos en formato JSON obtenidos de una API.

    Retorna:
    DataFrame: Un DataFrame de pandas que contiene los datos.
    """
    try:
        df = pd.json_normalize(
            json_data,
            record_path)
        return df
    except:
        print("Los datos no están en el formato esperado")
        return None

def get_max_incremental_value(timestamp_serie):
    """
    Obtiene el mayor valor incremental del JSON devuelto por la API en la consulta actual (que es el mayor timestamp).

    Parámetros:
        timestamp_serie (pandas.core.series.Series): Serie con los Timestamp del JSON devuelto por la API en la consulta actual.

    Retorna:
        Mayor valor incremental del JSON devuelto por la API en la 
        consulta actual (que es el mayor timestamp) en formato de fecha.
    """
    try:
        #Convierto la serie de int64 a tipo datetime
        timestamp_serie = pd.to_datetime(timestamp_serie, unit='ms')

        #Obtengo el timestamp más reciente de la serie
        max_timestamp = timestamp_serie.max()

        #Convierto el timestamp más reciente al formato de fecha deseado (YYYY-mm-dd HH:MM:SS)
        last_value = max_timestamp.strftime('%Y-%m-%d %H:%M:%S')

        #Convierto la fecha mas reciente de tipo string a tipo datetime
        last_value = datetime.strptime(last_value, '%Y-%m-%d %H:%M:%S')
        return last_value
    except Exception as e:
        print(f"Error al cargar el mayor valor incremental del archivo JSON: {e}")
        return
