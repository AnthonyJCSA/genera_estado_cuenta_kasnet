# import boto3
# import os
# from datetime import datetime
# import pandas as pd

# # Generación agentes procesados en local ###
# # Configura tus credenciales temporales
# aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
# aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
# aws_session_token = boto3.Session()

# # Parámetros
# bucket_name = 'staccprodestadosdecuenta'
# periodo = '202507'
# tipos = {
#     'contraprestacion': f'estados-cuenta/contraprestacion/{periodo}/',
#     'reembolso': f'estados-cuenta/reembolso/{periodo}/',
#     'adquirencia': f'estados-cuenta/adquirencia/{periodo}/',
# }

# # Cliente S3
# s3 = aws_session_token.client('s3')

# print("📂 Escaneando carpetas en el bucket...\n")

# # Obtener timestamp actual
# timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# log_registros = []

# # Escaneo por tipo
# for tipo, prefix in tipos.items():
#     print(f"🔍 Procesando tipo: {tipo.upper()} - Ruta: {prefix}")
#     paginator = s3.get_paginator('list_objects_v2')
#     page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

#     total = 0
#     for i, page in enumerate(page_iterator, 1):
#         objetos = page.get('Contents', [])
#         count_pagina = 0
#         for obj in objetos:
#             if obj['Key'].endswith('.pdf'):
#                 filename = os.path.basename(obj['Key'])
#                 store_id = filename.replace('.pdf', '')
#                 log_registros.append({
#                     'fecha_hora': timestamp,
#                     'store_id': store_id,
#                     'tipo': tipo,
#                     'estado': 1
#                 })
#                 count_pagina += 1
#         print(f"  📦 Página {i}: {count_pagina} PDFs encontrados")
#         total += count_pagina

#     print(f"✅ Total en {tipo.upper()}: {total} PDFs\n")

# # Exportar CSV
# df_log = pd.DataFrame(log_registros)
# df_log.to_csv("log_pdfs_existentes.csv", index=False)
# print("📝 Log guardado como 'log_pdfs_existentes.csv'")


# # ### eliminar registro de S3:

# import boto3

# s3 = boto3.client('s3')

# bucket_name = 'staccprodestadosdecuenta'
# prefix = 'estados-cuenta/contraprestacion/202507/'

# # Agentes que se deben conservar (NO eliminar)
# agentes_a_conservar = [
#     284111, 407761, 239681, 417311, 425551,
#     313415, 312889, 332622, 318641, 332981,
#     312887, 422961, 431111, 415871, 428671,
#     408771, 408391, 917001, 746701, 123001,
#     351371,
# ]

# # Convertimos a string para comparar con nombres de archivo
# agentes_permitidos = set(str(a) for a in agentes_a_conservar)

# print(f"Conservando {len(agentes_permitidos)} agentes. Eliminando todos los demás...")

# paginator = s3.get_paginator('list_objects_v2')
# pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

# for page in pages:
#     for obj in page.get("Contents", []):
#         key = obj["Key"]
#         filename = key.split("/")[-1]

#         # Nos aseguramos que sea un PDF válido y extraemos el store_id sin extensión
#         if filename.endswith(".pdf"):
#             store_id = filename.replace(".pdf", "")
#             if store_id not in agentes_permitidos:
#                 try:
#                     s3.delete_object(Bucket=bucket_name, Key=key)
#                     print(f"🗑️ Eliminado: s3://{bucket_name}/{key}")
#                 except Exception as e:
#                     print(f"❌ Error al eliminar {key}: {e}")
#             else:
#                 print(f"✅ Conservado: {store_id}")

# ### lectura de parquet input S3 ###
# import pandas as pd
# import s3fs
# from modules.aws_credentials import aws_input

# # Crear S3FileSystem pasando las credenciales manualmente
# fs_input = s3fs.S3FileSystem(
#     key=aws_input["aws_access_key_id"],
#     secret=aws_input["aws_secret_access_key"],
#     token=aws_input["aws_session_token"],
# )

# # Ruta del archivo parquet en S3
# path_s3 = "s3://kn-internal-delivery-dtldevs3/tecnologia/estados_cuenta/agente/agentes/2025/07/agentes_202507.parquet"

# # Leer el parquet desde S3 usando el sistema de archivos personalizado
# df = pd.read_parquet(path_s3, filesystem=fs_input, engine="pyarrow")

# print(df.head())

### lectura de parquet input S3 ###

# import json
# from modules import data_processor
# from modules.aws_credentials import aws_input  # S3FileSystem ya autenticado

# # === Cargar configuración desde config.json ===
# with open("config.json", "r", encoding="utf-8") as f:
#     config = json.load(f)

# # === Ejecutar procesamiento de datos ===
# datos = data_processor.procesar_datos(config)

# # === Mostrar tamaños de los dataframes cargados ===
# print("\n[TEST] Carga de Parquets desde S3:")
# for nombre_df, df in datos.items():
#     print(f"{nombre_df}: {df.count()} registros")



# import boto3
# import s3fs
# import pandas as pd
# import os
# import json
# import importlib.util
# from datetime import datetime
# from pathlib import Path

# # === CARGAR CREDENCIALES INPUT/OUTPUT ===
# def load_aws_credentials():
#     spec = importlib.util.spec_from_file_location("aws_credentials", "modules/aws_credentials.py")
#     aws_credentials = importlib.util.module_from_spec(spec)
#     spec.loader.exec_module(aws_credentials)
#     return aws_credentials.aws_input, aws_credentials.aws_output

# # === CARGAR CONFIGURACIÓN ===
# def load_config():
#     with open("config.json", "r") as f:
#         return json.load(f)

# # === ESCANEAR PDFs EN S3 (OUTPUT) ===
# def escanear_pdfs(bucket, periodo, tipos, s3_client):
#     timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     registros = []

#     for tipo, prefix_base in tipos.items():
#         anio, mes = periodo[:4], periodo[4:]
#         prefix = f"{prefix_base}{anio}/{mes}/"
#         print(f"🔍 Procesando tipo: {tipo.upper()} - Ruta: {prefix}")

#         paginator = s3_client.get_paginator('list_objects_v2')
#         page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

#         total = 0
#         for i, page in enumerate(page_iterator, 1):
#             objetos = page.get('Contents', [])
#             count_pagina = 0
#             for obj in objetos:
#                 if obj['Key'].endswith('.pdf'):
#                     filename = os.path.basename(obj['Key'])
#                     store_id = filename.replace('.pdf', '')
#                     registros.append({
#                         'fecha_hora': timestamp,
#                         'store_id': store_id,
#                         'tipo': tipo,
#                         'estado': 1
#                     })
#                     count_pagina += 1
#             print(f"  📦 Página {i}: {count_pagina} PDFs encontrados")
#             total += count_pagina
#         print(f"✅ Total en {tipo.upper()}: {total} PDFs\n")
    
#     return pd.DataFrame(registros)

# # === CARGAR PARQUET DE AGENTES DESDE INPUT S3 ===
# def cargar_agentes(bucket, base_path, periodo, creds):
#     anio, mes = periodo[:4], periodo[4:]
#     path = f"s3://{bucket}/{base_path}/agentes/{anio}/{mes}/agentes_{periodo}.parquet"

#     fs = s3fs.S3FileSystem(
#         key=creds["aws_access_key_id"],
#         secret=creds["aws_secret_access_key"],
#         token=creds["aws_session_token"]
#     )

#     print(f"📥 Cargando agentes desde: {path}")
#     with fs.open(path, "rb") as f:
#         df = pd.read_parquet(f, engine="pyarrow")
#         df["store_id"] = df["store_id"].astype(str)
#         df["email"] = df["email"].astype(str)
#         return df[["store_id", "email"]].drop_duplicates()

# # === FUNCIÓN PRINCIPAL ===
# def generar_log_estado_envio():
#     aws_input, aws_output = load_aws_credentials()
#     config = load_config()

#     anio = str(config["periodo"]["anio"])
#     mes = str(config["periodo"]["mes"]).zfill(2)
#     periodo = f"{anio}{mes}"  # "202507"

#     bucket_output = config["s3"]["output"]["bucket"]
#     tipos = {
#         'contraprestacion': config["s3"]["output"]["path_contraprestacion"],
#         'reembolso': config["s3"]["output"]["path_reembolso"],
#         'adquirencia': config["s3"]["output"]["path_adquirencia"]
#     }

#     # Cliente S3 OUTPUT
#     s3_client = boto3.client('s3')

#     # Escaneo de PDFs existentes
#     df_pdfs = escanear_pdfs(bucket_output, periodo, tipos, s3_client)

#     # Cargar agentes desde parquet (input)
#     bucket_input = config["s3"]["input"]["bucket"]
#     base_path_input = config["s3"]["input"]["base_path"]
#     df_agentes = cargar_agentes(bucket_input, base_path_input, periodo, aws_input)

#     # Unión por store_id
#     df_final = pd.merge(df_pdfs, df_agentes, on="store_id", how="left")

#     # Agregar columnas de envío
#     df_final["estado_envio"] = "no enviado"
#     df_final["fecha_hora_envio"] = ""

#     # Orden final
#     columnas = [
#         "fecha_hora", "store_id", "tipo", "estado", "email", "estado_envio", "fecha_hora_envio"
#     ]
#     df_final = df_final[columnas]

#     # Guardar CSV final
#     output_filename = f"log_estado_envio_{periodo}.csv"
#     df_final.to_csv(output_filename, index=False)
#     print(f"\n📝 Log completo guardado como '{output_filename}'")

# # === EJECUCIÓN ===
# if __name__ == "__main__":
#     generar_log_estado_envio()



#### Log para envios de email ###

import os
import pandas as pd

# Ruta a tus PDFs generados
pdf_folder = "C:/Proyectos/eecc_kasnet/temp_emails/contraprestacion"

# Leer todos los PDFs existentes
pdf_store_ids = set(
    filename.replace(".pdf", "")
    for filename in os.listdir(pdf_folder)
    if filename.endswith(".pdf")
)

# Leer log de correos
log_file = "log_correo_existentes.csv"
df_log = pd.read_csv(log_file, dtype={"store_id": str})

# Obtener store_ids únicos con correo enviado
correo_store_ids = set(df_log["store_id"].unique())

# Comparar
pendientes = pdf_store_ids - correo_store_ids

print(f"📦 PDFs generados: {len(pdf_store_ids)}")
print(f"📧 Correos enviados: {len(correo_store_ids)}")
print(f"❗ Pendientes por enviar: {len(pendientes)}")

# Guardar CSV con pendientes
df_pendientes = pd.DataFrame(sorted(pendientes), columns=["store_id"])
df_pendientes.to_csv("store_ids_pendientes_envio.csv", index=False)
print("📝 Archivo 'store_ids_pendientes_envio.csv' generado.")


