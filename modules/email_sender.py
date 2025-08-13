import smtplib
import ssl
import os
import logging
import tempfile
import boto3
from urllib.parse import urlparse
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from modules.aws_credentials import aws_output


class EmailSender:
    def __init__(self, smtp_user: str, smtp_password: str, remitente, destinatario_pruebas: str, pruebas: bool = True):
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.remitente = remitente
        self.destinatario_pruebas = destinatario_pruebas
        self.pruebas = pruebas
        #self.s3_client = boto3.client("s3")  # Modificado por aws_output
        self.s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_output["aws_access_key_id"],
        aws_secret_access_key=aws_output["aws_secret_access_key"],
        aws_session_token=aws_output["aws_session_token"]
        )
        
        logging.info(f"[EmailSender] Inicializado en modo {'PRUEBA' if pruebas else 'PRODUCCIÓN'}.")



    def enviar_correo(
        self,
        remitente: str,
        destinatario: str,
        asunto: str,
        mensaje: str,
        ruta_pdf_contra: str = None,
        ruta_pdf_reembolso: str = None,
        ruta_pdf_adquirencia: str = None
    ):
        # Construimos solo las rutas válidas
        archivos = []

        if ruta_pdf_contra:
            archivos.append(ruta_pdf_contra)
        if ruta_pdf_reembolso:
            archivos.append(ruta_pdf_reembolso)
        if ruta_pdf_adquirencia:
            archivos.append(ruta_pdf_adquirencia)

        archivos_locales = []

        try:
            if self.pruebas:
                logging.info(f"Modo pruebas: redirigiendo de {destinatario} → {self.destinatario_pruebas}")
                destinatario = self.destinatario_pruebas

            servidor_smtp = "email-smtp.us-east-1.amazonaws.com"
            puerto_smtp = 465

            msg = MIMEMultipart()
            msg["From"] = self.remitente
            msg["To"] = destinatario
            msg["Subject"] = asunto
            msg.attach(MIMEText(mensaje, "plain"))

            for ruta in archivos:
                local_path = None
                if not ruta:
                    continue
                if ruta.startswith("s3://"):
                    try:
                        local_path = self._descargar_s3_temp(ruta)
                        archivos_locales.append(local_path)
                        logging.info(f"Archivo descargado de S3: {ruta}")
                    except Exception as e:
                        logging.error(f"Error al descargar archivo S3 {ruta}: {e}")
                        continue
                elif os.path.exists(ruta):
                    local_path = ruta
                    archivos_locales.append(local_path)
                else:
                    logging.warning(f"Ruta no válida u omitida: {ruta}")
                    continue

                if local_path:
                    try:
                        with open(local_path, "rb") as file:
                            part = MIMEBase("application", "octet-stream")
                            part.set_payload(file.read())
                            encoders.encode_base64(part)
                            part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(local_path)}"')
                            msg.attach(part)
                            logging.info(f"PDF adjuntado: {local_path}")
                    except Exception as e:
                        logging.error(f"No se pudo adjuntar {local_path}: {e}")


            contexto = ssl.create_default_context()
            with smtplib.SMTP_SSL(servidor_smtp, puerto_smtp, context=contexto) as server:
                server.login(self.smtp_user, self.smtp_password)
                server.sendmail(remitente, destinatario, msg.as_string())

            
            logging.info(f"[EmailSender] Enviando {len(archivos)} PDF(s) adjunto(s) a {destinatario}")
            logging.info(f"Correo enviado correctamente a {destinatario}")

        except Exception as e:
            logging.exception(f"Error al enviar correo a {destinatario}")
            raise

        finally:
            # Borrar archivos temporales
            for tmp_file in archivos_locales:
                if tmp_file and tmp_file.startswith(tempfile.gettempdir()):
                    try:
                        os.remove(tmp_file)
                        logging.info(f"Archivo temporal eliminado: {tmp_file}")
                    except Exception as e:
                        logging.warning(f"No se pudo eliminar archivo temporal {tmp_file}: {e}")
