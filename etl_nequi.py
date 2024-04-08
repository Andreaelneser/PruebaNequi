import boto3
import os
import pandas as pd
import io 

# Configurar las credenciales de AWS
aws_access_key_id = '**********'
aws_secret_access_key = '**********'
aws_region = 'us-east-1'  

def extract_data(file_name):
    """
    Función para extraer los datos del archivo CSV.
    """
    try:
        # Leer el archivo CSV en un DataFrame de Pandas
        data = pd.read_csv(file_name)
        print("Datos extraídos exitosamente.")
        return data
    except Exception as e:
        print(f"Error al extraer datos del archivo CSV: {str(e)}")
        return None

def transform_data(data):
    """
    Función para transformar los datos.
    """
    try:
        # Validar que la clave primaria sea única y no tenga nulos
        if data['channel_id'].is_unique and not data['channel_id'].isnull().values.any():
            # Renombrar columnas
            data = data.rename(columns={'avatar': 'avatar_link', 'join_date': 'join_date_formatted'})

            # Eliminar filas con valores nulos en columnas específicas
            data = data.dropna(subset=['total_views', 'total_videos', 'description'])

            # Formatear columnas de fecha y convertir tipos de datos
            data['join_date_formatted'] = pd.to_datetime(data['join_date_formatted']) # Convertir join_date_formatted a tipo fecha
            data['channel_id'] = data['channel_id'].astype(str)   # Convertir channel_id a tipo string
            data['channel_link'] = data['channel_link'].astype(str)  # Convertir channel_link a tipo string
            data['total_views'] = data['total_views'].astype(int)  # Convertir total_views a tipo entero
            data['total_videos'] = data['total_videos'].astype(int)  # Convertir total_videos a tipo entero
            print("Datos transformados exitosamente.")

            # Dividir el DataFrame en dos: uno para los datos del canal y otro para las estadísticas
            canal_data = data[['channel_id', 'channel_link', 'channel_name', 'subscriber_count', 'banner_link', 'description', 'keywords', 'avatar_link', 'country', 'join_date_formatted']]
            estadisticas_data = data[['channel_id', 'total_views', 'total_videos', 'mean_views_last_30_videos', 'median_views_last_30_videos', 'std_views_last_30_videos', 'videos_per_week']]
            return canal_data, estadisticas_data
        else:
            print("Error: La clave primaria no es única o contiene valores nulos.")
            return None, None
    except Exception as e:
        print(f"Error al transformar los datos: {str(e)}")
        return None, None

def load_data_to_s3(data, file_name, bucket_name):
    """
    Función para cargar los datos en un bucket de S3.
    """
    try:
        # Guardar los datos en un archivo CSV temporal
        temp_file = f'temp_data_yt.csv'
        data.to_csv(temp_file, index=False)
        
        # Crear un cliente de S3 con las credenciales configuradas
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)
        s3.upload_file(temp_file, bucket_name, file_name)
        
        # Eliminar el archivo CSV temporal
        os.remove(temp_file)
        print(f"Datos cargados exitosamente en S3 en el bucket {bucket_name}.")
    except Exception as e:
        print(f"Error al cargar los datos en S3: {str(e)}")

def data_quality_check(data):
    """
    Realiza controles de calidad en los datos.
    """
    # Verificar si hay duplicados en la clave primaria
    if data['channel_id'].duplicated().any():
        print("Advertencia: Se encontraron registros duplicados en la clave primaria 'channel_id'.")

    # Verificar si hay valores nulos en la clave primaria
    if data['channel_id'].isnull().any():
        print("Advertencia: Se encontraron valores nulos en la clave primaria 'channel_id'.")
    
    print("Control de calidad de datos completado.")

def main():
    # Nombre del archivo CSV con los datos
    file_name = 'c:/Users/Andrea bb/Downloads/Spotify/youtube_channels_1M_clean.csv'
    print(f"Archivo de entrada: {file_name}")
    
    # Nombre del bucket de S3 donde se cargarán los datos
    bucket_name = 'nequiprueba'
    print(f"Bucket de destino: {bucket_name}")
    
    # Extraer los datos del archivo CSV
    data = extract_data(file_name)
    if data is not None:
        # Realizar control de calidad en los datos
        data_quality_check(data)
        
        # Transformar los datos
        canal_data, estadisticas_data = transform_data(data)
        if canal_data is not None and estadisticas_data is not None:
            # Cargar los datos transformados en S3
            load_data_to_s3(canal_data, 'canal_data.csv', bucket_name)
            load_data_to_s3(estadisticas_data, 'estadisticas_data.csv', bucket_name)
            
            # Verificar las columnas en S3
            print("Verificación de columnas en canal_data.csv:")
            verify_columns_in_s3('canal_data.csv', bucket_name, canal_data.columns)
            print("Verificación de columnas en estadisticas_data.csv:")
            verify_columns_in_s3('estadisticas_data.csv', bucket_name, estadisticas_data.columns)
            
    print("Proceso de carga de datos completado.")

def verify_columns_in_s3(file_name, bucket_name, expected_columns):
    """
    Verifica las columnas de un archivo CSV en S3.
    """
    try:
        # Crear un cliente de S3 con las credenciales configuradas
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)
        
        # Descargar el archivo CSV desde S3
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        data = pd.read_csv(io.BytesIO(response['Body'].read()))
        
        # Verificar si las columnas esperadas están presentes
        if all(col in data.columns for col in expected_columns):
            print(f"Columnas en {file_name} encontradas:")
            print(expected_columns)
        else:
            print(f"Error: No se encontraron todas las columnas requeridas en {file_name}")
            print(data.columns)
            
    except Exception as e:
        print(f"Error al verificar columnas en S3: {str(e)}")

if __name__ == "__main__":
    main()
