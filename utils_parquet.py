import pandas as pd
import os
import glob

def save_to_parquet(df, output_path, partition_cols=None):
    """
    Recibe un dataframe, se recomienda que haya sido convertido a un formato tabular,
    y lo guarda en formato parquet.

    Parametros:
    df (pd.DataFrame). Dataframe a guardar.
    output_path (str). Ruta donde se guardará el archivo. Si no existe, se creará.
    partition_cols (list o str). Columna/s por las cuales particionar los datos.
    """

    # Crear el directorio si no existe
    directory = os.path.dirname(output_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

    df.to_parquet(
        output_path,
        engine="fastparquet",
        partition_cols=partition_cols
        )

def read_parquet_with_partition(file_path, partition_column):
    """
    Lee archivos Parquet particionados por una columna específica y agrega la columna de partición al DataFrame.

    Parametros:
    file_path (str): Ruta al directorio que contiene los archivos Parquet particionados.
    partition_column (str): Nombre de la columna por la que se particionaron los archivos.

    Retorna:
    DataFrame: DataFrame con la información de los archivos Parquet particionados.
    """
    # Obtener una lista de todos los archivos Parquet dentro de la carpeta y sus subcarpetas
    parquet_files = glob.glob(os.path.join(file_path, '**', '*.parquet'), recursive=True)
    print(parquet_files)

    # Inicializar una lista para almacenar los DataFrames de cada archivo
    dataframes = []
    
    # Leer cada archivo Parquet y agregar su DataFrame a la lista
    for parquet_file in parquet_files:
        try:
            df = pd.read_parquet(parquet_file)

            # Extraer la partición y agregarla como una columna en el DataFrame
            file_components = parquet_file.split('\\')
            partition_value = file_components[1].replace(f'{partition_column}=', '')
            df[partition_column] = partition_value
            dataframes.append(df)
        except Exception as e:
            print(f"Error al leer el archivo '{parquet_file}': {str(e)}")
    
    # Concatenar todos los DataFrames en uno solo
    if dataframes:
        df_concatenated = pd.concat(dataframes, ignore_index=True)
        return df_concatenated
    else:
        return None
    
def read_parquet(directory_path):
    """
    Lee archivos Parquet y construye DataFrames con su información.

    Parámetros:
        directory_path (str): Ruta al directorio que contiene archivos Parquet.

    Retorna:
        DataFrame o lista de DataFrames, dependiendo de si hay un solo archivo o varios.
    """
    # Verificar si la ruta es un archivo Parquet
    if directory_path.endswith('.parquet'):
        try:
            return pd.read_parquet(directory_path)
        except Exception as e:
            print(f"Error al leer el archivo Parquet '{directory_path}': {str(e)}")
            return None

    # Obtener una lista de todos los archivos Parquet dentro del directorio y sus subdirectorios
    parquet_files = glob.glob(os.path.join(directory_path, '**/*.parquet'), recursive=True)

    # Inicializar una lista para almacenar los DataFrames de cada archivo
    dataframes = []

    # Leer cada archivo Parquet y agregar su DataFrame a la lista
    for parquet_file in parquet_files:
        try:
            df = pd.read_parquet(parquet_file)
            dataframes.append(df)
        except Exception as e:
            print(f"Error al leer el archivo Parquet '{parquet_file}': {str(e)}")

    # Si hay solo un DataFrame en la lista, retornarlo
    if len(dataframes) == 1:
        return dataframes[0]
    # Si hay varios DataFrames en la lista, retornar la lista
    elif len(dataframes) > 1:
        return dataframes
    # Si no se encuentra ningún archivo Parquet
    else:
        print(f"No se encontraron archivos Parquet en '{directory_path}'.")
        return None