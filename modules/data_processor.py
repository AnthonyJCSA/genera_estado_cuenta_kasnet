import pandas as pd
import logging
from modules.aws_credentials import aws_input
import s3fs

def construir_path_parquet(tipo: str, config: dict) -> str:
    bucket = config["s3"]["input"]["bucket"]
    base_path = config["s3"]["input"]["base_path"]
    subcarpeta = config["s3"]["input"]["archivos"][tipo]
    anio = config["periodo"]["anio"]
    mes = str(config["periodo"]["mes"]).zfill(2)
    nombre_archivo = f"{subcarpeta}_{anio}{mes}.parquet"

    return f"s3://{bucket}/{base_path}/{subcarpeta}/{anio}/{mes}/{nombre_archivo}"

def procesar_datos(config: dict) -> dict:
    try:
        logging.info("Cargando datos de los Parquet desde S3...")

        # Crear S3FileSystem con credenciales temporales
        fs = s3fs.S3FileSystem(
            key=aws_input["aws_access_key_id"],
            secret=aws_input["aws_secret_access_key"],
            token=aws_input["aws_session_token"]
        )

        # # === CARGA DE PARQUET ===
        # df_agentes = pd.read_parquet(config["rutas"]["parquet_agentes"])
        # df_contra = pd.read_parquet(config["rutas"]["parquet_contraprestaciones"])

        # df_bonos = pd.read_parquet(config["rutas"]["parquet_bonos"])
        # df_desc = pd.read_parquet(config["rutas"]["parquet_descuentos"])
        # df_reembolso = pd.read_parquet(config["rutas"]["parquet_reembolso"])
        # df_adquirencia = pd.read_parquet(config["rutas"]["parquet_adquirencia"])

        df_agentes = pd.read_parquet(construir_path_parquet("agentes", config), filesystem=fs, engine="pyarrow")
        df_contra = pd.read_parquet(construir_path_parquet("contraprestacion", config), filesystem=fs, engine="pyarrow")
        df_bonos = pd.read_parquet(construir_path_parquet("bonos", config), filesystem=fs, engine="pyarrow")
        df_desc = pd.read_parquet(construir_path_parquet("descuentos", config), filesystem=fs, engine="pyarrow")
        df_reembolso = pd.read_parquet(construir_path_parquet("reembolso", config), filesystem=fs, engine="pyarrow")
        df_adquirencia = pd.read_parquet(construir_path_parquet("adquirencia", config), filesystem=fs, engine="pyarrow")

        logging.info(
            f"Parquet cargados correctamente: "
            f"Agentes: {df_agentes.shape}, "
            f"Contraprestaciones: {df_contra.shape}, "
            f"Bonos: {df_bonos.shape}, "
            f"Descuentos: {df_desc.shape}, "
            f"Reembolso: {df_reembolso.shape}, "
            f"Adquirencia: {df_adquirencia.shape}"
        )

        # === NORMALIZACIÓN TIPOS (store_id, pos, transaction_id siempre str) ===
        for df in [df_agentes, df_contra, df_bonos, df_desc, df_reembolso, df_adquirencia]:

            if "store_id" in df.columns:
                df["store_id"] = df["store_id"].astype(str)
            if "pos" in df.columns:
                df["pos"] = df["pos"].astype(str)
            if "transaction_id" in df.columns:
                df["transaction_id"] = df["transaction_id"].astype(str)
            if "entity_transaction_id" in df.columns:
                df["entity_transaction_id"] = df["entity_transaction_id"].astype(str)

        # === AGREGAR CAMPO IGV TOTAL SI NO EXISTE (TEMPORAL) ===
        if "igv" not in df_contra.columns:
            df_contra["igv"] = 0.0

        if "igv" not in df_bonos.columns:
            df_bonos["igv"] = 0.0

        if "igv" not in df_desc.columns:
            df_desc["igv"] = 0.0

        if "igv" not in df_reembolso.columns:
            df_reembolso["igv"] = 0.0


        # === RENOMBRAR CAMPOS PARA HOMOGENEIDAD (solo donde aplique) ===
        if not df_reembolso.empty:
            df_reembolso = df_reembolso.rename(columns={
                "entity_descripcion": "entity_description",
                "comission": "comission_amount"
            }
            )

        # === RENOMBRAR CAMPOS PARA HOMOGENEIDAD (df_adquirencia) ===
        if not df_adquirencia.empty:
            df_adquirencia = df_adquirencia.rename(columns={
                "credited_amount": "importe_abonado"
            }
            )

        # === RENOMBRAR CAMPOS PARA COMPATIBILIDAD CON PDF (BONOS Y DESCUENTOS) ===
        if not df_bonos.empty:
            df_bonos = df_bonos.rename(columns={
                "description": "bonus_type",
                "amount": "monto",
                "amount_igv": "monto_igv"
            })

        if not df_desc.empty:
            df_desc = df_desc.rename(columns={
                "discount_item": "discount_type",
                "total_discount_amount": "monto",
                "total_discount_amount_igv": "monto_igv"
            })

        # === CRUCE CONTRA TABLA PRINCIPAL (AGENTES) ===
        df_contra = df_contra[df_contra["store_id"].isin(df_agentes["store_id"])]
        df_bonos = df_bonos[df_bonos["store_id"].isin(df_agentes["store_id"])]
        df_desc = df_desc[df_desc["store_id"].isin(df_agentes["store_id"])]
        df_reembolso = df_reembolso[df_reembolso["store_id"].isin(df_agentes["store_id"])]
        df_adquirencia = df_adquirencia[df_adquirencia["store_id"].isin(df_agentes["store_id"])]
        

        # === DEBUG: LOGS DE CRUCE ===
        logging.info(f"Agentes únicos (tabla principal): {df_agentes['store_id'].nunique()}")
        logging.info(f"Agentes con contraprestaciones: {df_contra['store_id'].nunique()}")
        logging.info(f"Agentes con bonos: {df_bonos['store_id'].nunique()}")
        logging.info(f"Agentes con descuentos: {df_desc['store_id'].nunique()}")
        logging.info(f"Agentes con reembolso: {df_reembolso['store_id'].nunique()}")
        logging.info(f"Agentes con adquirencia: {df_adquirencia['store_id'].nunique()}")  

        # === RESÚMENES ===
        resumen_contra = (
            df_contra.groupby("store_id")
            .agg(
                total_comision=("comission_amount", "sum"),
                total_operaciones=("transaction_amount", "sum"),
                total_igv=("comission_amount_igv", "sum"),  # NUEVO CAMPO IGV TOTAL
                igv=("igv", "sum")   # NUEVO CAMPO IGV
            )
            .reset_index()
        )


        resumen_reembolso = (
            df_reembolso.groupby("store_id")
            .agg(total_comision=("comission_amount", "sum"),
                 total_operaciones=("transaction_amount", "sum"),
                 total_igv=("comission_amount_igv", "sum"),  # NUEVO CAMPO IGV TOTAL
                 igv=("igv", "sum")   # NUEVO CAMPO IGV
                 )
            .reset_index()
        )

        resumen_adquirencia = (
            df_adquirencia.groupby("store_id")
            .agg(total_operaciones=("transaction_amount", "sum"),
                 total_comision=("comission_amount_igv", "sum"),
                 importe_abonado=("importe_abonado", "sum")
                 )
            .reset_index()
        )

        logging.info("Procesamiento de datos finalizado correctamente.")
        logging.info(
            f"Resumen: Agentes procesados (contraprestaciones): {df_contra['store_id'].nunique()}, "
            f"Contraprestaciones: {df_contra.shape[0]}, "
            f"Bonos: {df_bonos['store_id'].nunique()}, "
            f"Descuentos: {df_desc['store_id'].nunique()}, "
            f"Reembolso: {df_reembolso['store_id'].nunique()}, "
            f"Adquirencia: {df_adquirencia['store_id'].nunique()}"
        )

        return {
            "agentes": df_agentes,
            "detalle_contra": df_contra,
            "detalle_bonos": df_bonos,
            "detalle_desc": df_desc,
            "detalle_reembolso": df_reembolso,
            "detalle_adquirencia": df_adquirencia,
            "resumen_contra": resumen_contra,
            "resumen_reembolso": resumen_reembolso,
            "resumen_adquirencia": resumen_adquirencia
            
        }

    except Exception as e:
        logging.error("Error procesando los datos desde los Parquet.", exc_info=True)
        raise
