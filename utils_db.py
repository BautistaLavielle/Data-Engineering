from configparser import ConfigParser
import pandas as pd
from sqlalchemy import create_engine


def connect_to_db(config_file, section):
    """
    Crea una conexión a la base de datos especificada en el archivo de configuración.

    Parámetros:
    config_file (str): La ruta del archivo de configuración.
    section (str): La sección del archivo de configuración que contiene los datos de la base de datos.
    driverdb (str): El driver de la base de datos a la que se conectará.

    Retorna:
    Un objeto de conexión a la base de datos.
    """
    try:
        # Lectura del archivo de configuración
        parser = ConfigParser()
        parser.read(config_file)

        # Creación de un diccionario
        # donde cargaremos los parámetros de la base de datos
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            db = {param[0]: param[1] for param in params}

            # Creación de la conexión a la base de datos
            engine = create_engine(
                f"{db['driverdb']}://{db['user']}:{db['pwd']}@{db['host']}:{db['port']}/{db['dbname']}"
            )
            return engine

        else:
            print(
                f"Sección {section} no encontrada en el archivo de configuración.")
            return None
    except Exception as e:
        print(f"Error al conectarse a la base de datos: {e}")
        return None

def load_data(df, table, schema, engine, mode='append'):
    """
    Carga los datos de un DataFrame en una tabla de la base de datos.

    Parametros:
    df (Dataframe): DataFrame conteniendo los datos a cargar.
    table (str): Nombre de la tabla en la base de datos.
    schema (str): Esquema de la base de datos donde se encuentra la tabla.
    engine (objeto de conexión a la base de datos de sqlalchemy): Motor de base de datos SQLAlchemy.
    mode (str): Modo para cargar el dataframe en la tabla de la base de datos (puede ser 'append' o 'replace')

    Retorna:
    None
    """
    try:
        df.to_sql(table, con=engine, schema=schema, if_exists=mode, index=False)
        print("Datos cargados exitosamente en la tabla", table)
    except Exception as e:
        print("Error al cargar los datos en la tabla", table)
        print(e)