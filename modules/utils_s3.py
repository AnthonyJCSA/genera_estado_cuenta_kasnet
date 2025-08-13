import os
import logging

def download_pdf_from_s3_if_exists(s3_client, bucket: str, key: str, subfolder: str = "") -> str:
    """
    Descarga un archivo PDF desde S3 si existe y lo guarda en una subcarpeta temporal específica.
    
    Args:
        s3_client: Cliente de boto3 para S3.
        bucket (str): Nombre del bucket S3.
        key (str): Clave completa del objeto PDF en S3.
        subfolder (str): Subcarpeta dentro de 'temp_emails' donde guardar el archivo temporal.

    Returns:
        str: Ruta local al archivo descargado si existe, o None si no se encontró.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)  # Verifica existencia

        # Crear carpeta temporal específica por tipo
        temp_dir = os.path.join("temp_emails", subfolder)
        os.makedirs(temp_dir, exist_ok=True)

        filename = os.path.basename(key)
        ruta_local = os.path.join(temp_dir, filename)

        s3_client.download_file(bucket, key, ruta_local)
        logging.info(f"PDF descargado de S3: {ruta_local}")
        return ruta_local

    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logging.warning(f"[S3] No encontrado: {key}")
            return None
        else:
            raise



