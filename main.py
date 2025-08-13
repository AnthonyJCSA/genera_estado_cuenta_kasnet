import logging
from modules.logger_config import LoggerConfig
from modules.config_loader import load_config
from modules.data_processor import procesar_datos
import time
from modules.email_sender import EmailSender
import os
import calendar
from modules.utils_s3 import download_pdf_from_s3_if_exists
import boto3
from modules.pdf_generator import (PDFGeneratorContraprestaciones, PDFGeneratorReembolso, PDFGeneratorAdquirencia,)
from modules.progress_tracker_s3 import ProgressTrackerS3
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import base64
import signal
import sys
from modules.aws_credentials import aws_input, aws_output

def signal_handler(sig, frame):
    print("\n Interrupción detectada con Ctrl + C. Cerrando procesos...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


def main():
    inicio = time.time()
    try:
        # === Inicializar logging ===
        LoggerConfig.setup_logger("logs_ejecucion_general")

        # === Cargar configuración ===
        config = load_config()
        periodo = config["periodo"]
        periodo_texto = f"{periodo['mes']:02}/{periodo['anio']}"
        config["periodo_texto"] = periodo_texto

        # === Cargar flags de ejecución desde config ===
        # ejecutar_contra = config.get("generar_contra", True)
        # ejecutar_reembolso = config.get("generar_reembolso", True)
        # ejecutar_adquirencia = config.get("generar_adquirencia", True)


        logging.info(f"Configuración cargada correctamente para {periodo}")

        # === CONVERTIR LOGO A BASE64 ===
        with open(config["rutas"]["logo"], "rb") as logo_file:
            logo_base64 = base64.b64encode(logo_file.read()).decode("utf-8")
        config["logo_base64"] = logo_base64

        # === Procesar datos ===
        logging.info("Iniciando procesamiento de datos...")
        resultado = procesar_datos(config)
        logging.info(f"Datos procesados para {resultado['agentes'].shape[0]} agentes")
        print(f"Datos procesados para {resultado['agentes'].shape[0]} agentes")

        # === Inicializar generadores de PDFs ===
        logging.info("Inicializando generadores de PDFs...")
        pdf_contra = PDFGeneratorContraprestaciones(config)
        pdf_reembolso = PDFGeneratorReembolso(config)
        pdf_adquirencia = PDFGeneratorAdquirencia(config)


        # === Generar PDFs ===
        logging.info("Iniciando generación de PDFs para todos los agentes...")

        # === Cargar log de PDFs ya generados (si existe)
        path_log = "log_pdfs_existentes.csv"
        df_log = None

        if os.path.exists(path_log):
            df_log = pd.read_csv(path_log, usecols=["store_id", "estado", "tipo"], dtype={"store_id": str})

            df_log = df_log[df_log["estado"] == 1]  # Solo los generados correctamente
            logging.info(f"[main.py] Log de PDFs existentes cargado correctamente.")
        else:
            logging.info("[main.py] No se encontró log previo, se generarán todos los PDFs.")

        # === Función auxiliar para obtener store_ids faltantes por tipo
        def obtener_faltantes(tipo: str, df_datos_tipo: pd.DataFrame):
            store_ids_generados = set()
            if df_log is not None:
                store_ids_generados = set(
                    df_log[(df_log["tipo"] == tipo) & (df_log["estado"] == 1)]["store_id"]
                )
            store_ids_origen = df_datos_tipo["store_id"].unique()
            return [sid for sid in store_ids_origen if sid not in store_ids_generados]


        # === Filtrar store_ids por tipo
        store_ids_contra_faltantes = obtener_faltantes("contraprestacion", resultado["detalle_contra"])
        store_ids_reembolso_faltantes = obtener_faltantes("reembolso", resultado["detalle_reembolso"])
        store_ids_adquirencia_faltantes = obtener_faltantes("adquirencia", resultado["detalle_adquirencia"])

        # === DataFrames de agentes por tipo
        df_agentes = resultado["agentes"]
        df_agentes_contra = df_agentes[df_agentes["store_id"].isin(store_ids_contra_faltantes)]
        df_agentes_reembolso = df_agentes[df_agentes["store_id"].isin(store_ids_reembolso_faltantes)]
        df_agentes_adquirencia = df_agentes[df_agentes["store_id"].isin(store_ids_adquirencia_faltantes)]

        # === Logs
        logging.info(f"[main.py] {len(store_ids_contra_faltantes)} agentes pendientes para Contraprestaciones.")
        logging.info(f"[main.py] {len(store_ids_reembolso_faltantes)} agentes pendientes para Reembolso.")
        logging.info(f"[main.py] {len(store_ids_adquirencia_faltantes)} agentes pendientes para Adquirencia.")

        flags = config.get("ejecucion", {})


        if flags.get("generar_contraprestacion", False):
            logging.info("[main.py] Generando PDFs de contraprestaciones en paralelo...")

            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.map(
                    lambda sid: pdf_contra.generar_individual(sid, df_agentes_contra, resultado, config),
                    store_ids_contra_faltantes
                )

            logging.info("[main.py] Generación de contraprestaciones finalizada.")
        else:
            logging.info("[main.py] generar_contraprestacion=False → Contraprestaciones omitidas.")

        if flags.get("generar_reembolso", False):
            logging.info("[main.py] Generando PDFs de reembolso en paralelo...")

            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.map(
                    lambda sid: pdf_reembolso.generar_individual(sid, df_agentes_reembolso, resultado, config),
                    store_ids_reembolso_faltantes
                )

            logging.info("[main.py] Generación de reembolso finalizada.")
        else:
            logging.info("[main.py] generar_reembolso=False → Reembolsos omitidos.")

        if flags.get("generar_adquirencia", False):
            logging.info("[main.py] Generando PDFs de adquirencia en paralelo...")

            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.map(
                    lambda sid: pdf_adquirencia.generar_individual(sid, df_agentes_adquirencia, resultado, config),
                    store_ids_adquirencia_faltantes
                )

            logging.info("[main.py] Generación de adquirencia finalizada.")
        else:
            logging.info("[main.py] generar_adquirencia=False → Adquirencia omitida.")


        agentes = resultado["agentes"].to_dict(orient="records")


        log_path = "log_correo_existentes.csv"
        df_log = None

        
        ### INICIO ENVIO DE CORREO ### 
        # === Cargar emails ya enviados ===
        def cargar_log_envios(log_path):
            if not os.path.exists(log_path):
                return set()
            df = pd.read_csv(log_path)
            return set(df["store_id"].astype(str))


        # === Lógica principal de envío de correos ===
        def enviar_correos_main(config, agentes):
            if not config["ejecucion"].get("enviar_correos", False):
                logging.info("[main.py] envío_correos = false → Envío de correos omitido.")
                print("El envío de correos está desactivado (config.json → ejecucion.enviar_correos = false).")
                return

            logging.info("Iniciando envío de correos...")


        email_sender = EmailSender(
            smtp_user=config["correo"]["smtp_user"],
            smtp_password=config["correo"]["smtp_password"],
            remitente=config["correo"]["remitente"],
            destinatario_pruebas=config["correo"]["destinatario_pruebas"],
            pruebas=not config["correo"]["enviar_a_todos"]
        )

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_output["aws_access_key_id"],
            aws_secret_access_key=aws_output["aws_secret_access_key"],
            aws_session_token=aws_output["aws_session_token"]
        )

        enviados = cargar_log_envios(log_path)
        anio, mes = config["periodo"]["anio"], config["periodo"]["mes"]
        periodo_folder = f"{anio}{mes:02}"

        def procesar_agente(agente):
            try:
                store_id = str(agente["store_id"])
                email = agente.get("email")

                if not email:
                    logging.warning(f"Agente {store_id} sin correo. Correo omitido.")
                    return

                if store_id in enviados:
                    logging.info(f"Agente {store_id} ya tiene correo enviado. Omitido.")
                    return

                from urllib.parse import urlparse
                def construir_s3_key(s3_uri_base, tipo):
                    parsed = urlparse(s3_uri_base)
                    bucket = parsed.netloc
                    base_key = parsed.path.lstrip("/")
                    key = f"{base_key}{periodo_folder}/{store_id}.pdf"
                    return bucket, key

                rutas = config["rutas"]["salida_s3"]
                keys = {tipo: construir_s3_key(rutas[tipo], tipo) for tipo in ["contraprestacion", "reembolso", "adquirencia"]}

                archivos = {}
                for tipo, (bucket, key) in keys.items():
                    archivos[tipo] = download_pdf_from_s3_if_exists(s3_client, bucket, key, subfolder=tipo)

                pdfs_validos = [ruta for ruta in archivos.values() if ruta and os.path.exists(ruta)]
                if not pdfs_validos:
                    logging.warning(f"[main.py] - Archivos PDF no encontrados o no descargados correctamente para {store_id}. Correo omitido.")
                    return

                meses_es = ["", "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO", "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"]
                nombre_mes = meses_es[mes]

                email_sender.enviar_correo(
                    remitente=config["correo"]["remitente"],
                    destinatario=email,
                    asunto=config["correo"]["asunto"].format(NOMBRE_MES=nombre_mes, ANIO=anio),
                    mensaje=config["correo"]["mensaje"].format(NOMBRE_MES=nombre_mes, ANIO=anio),
                    ruta_pdf_contra=archivos["contraprestacion"],
                    ruta_pdf_reembolso=archivos["reembolso"],
                    ruta_pdf_adquirencia=archivos["adquirencia"]
                )

                with open("log_pdfs_existentes.csv", "a", encoding="utf-8") as f:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    f.write(f"{now},{store_id},correo,enviado,{email},enviado,{now}\n")

            except Exception as e:
                logging.error(f"Error enviando correo a {store_id}: {str(e)}")

        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(procesar_agente, agentes)

        logging.info("Envío de correos finalizado.")


        # === Guardar archivo resumen en S3 ===

        #tracker.registrar_envio(store_id, email, "omitido",  datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


        ### tracker._guardar()

        fin = time.time()
        duracion = fin - inicio

        # Convertir segundos a formato hh:mm:ss
        horas, resto = divmod(duracion, 3600)
        minutos, segundos = divmod(resto, 60)

        tiempo_formateado = f"{int(horas):02d}:{int(minutos):02d}:{int(segundos):02d}"

        logging.info(f"Proceso completado en {tiempo_formateado} (hh:mm:ss)")

    except Exception as e:
        LoggerConfig.log_exception(e, "main.py")
        print("\nPROCESO ABORTADO. Revisar logs para más detalles.")


if __name__ == "__main__":
    main()
