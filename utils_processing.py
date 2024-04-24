import pandas as pd

def fill_null_values(df, column_name, fill_value):
    """
    Rellena los valores nulos en una columna con un valor específico.

    Args:
        df (pd.DataFrame): El DataFrame que contiene los datos.
        column_name (str): El nombre de la columna en la que se deben rellenar los valores nulos.
        fill_value: El valor con el que se deben rellenar los valores nulos en la columna especificada.

    Returns:
        pd.DataFrame: El DataFrame con los valores nulos rellenados en la columna especificada.
    """
    df[column_name] = df[column_name].fillna(fill_value)
    return df

def delete_null_records(df, column):
    """
    Elimina los registros que tienen valores nulos en una columna específica de un DataFrame.
    
    Parámetros:
        - df: DataFrame de pandas.
        - column: Nombre de la columna en la que se buscarán los valores nulos.
    
    Retorna:
        Un nuevo DataFrame sin los registros que contienen valores nulos en la columna especificada.
    """
    try:
        df_filtrado = df.dropna(subset=[column])
        return df_filtrado
    except KeyError:
        print("La columna especificada no existe en el DataFrame.")
        return None
    except Exception as e:
        print("Ocurrió un error:", e)
        return None