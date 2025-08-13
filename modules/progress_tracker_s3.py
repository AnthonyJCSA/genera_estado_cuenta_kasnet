import boto3
import pandas as pd
import io
from datetime import datetime
import logging

class ProgressTrackerS3:
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key
        self.s3_client = boto3.client("s3")
        self.df = self._load_or_init()

    def _load_or_init(self):
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=self.key)
            content = response["Body"].read()
            return pd.read_csv(io.BytesIO(content))
        except self.s3_client.exceptions.NoSuchKey:
            logging.warning(f"[ProgressTracker] Archivo de progreso no encontrado en S3, inicializando nuevo archivo...")
            return pd.DataFrame(columns=["store_id", "email", "tipo", "estado", "fecha_envio"])

    def ya_enviado(self, store_id, tipo):
        return not self.df[(self.df.store_id == store_id) & (self.df.tipo == tipo)].empty
    
    

    def registrar_envio(self, store_id, email, tipo, estado):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        nuevo = pd.DataFrame([
            {
                "store_id": store_id,
                "email": email or "NA",
                "tipo": tipo,
                "estado": estado,
                "fecha_envio": timestamp,
            }
        ])
        self.df = pd.concat([self.df, nuevo], ignore_index=True)


    def _guardar(self):
        csv_buffer = io.StringIO()
        self.df.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(Bucket=self.bucket, Key=self.key, Body=csv_buffer.getvalue().encode("utf-8"))
        logging.info(f"[ProgressTracker] Progreso actualizado en S3: {self.key}")
