import json
import os
from datetime import datetime
import calendar
import logging

def load_config(config_path: str = "config.json") -> dict:
    """
    Carga y valida la configuración desde el archivo JSON.
    - Ajusta automáticamente las rutas de entrada y salida según el periodo.
    - Valida existencia de archivos parquet antes de procesar.
    - Crea carpetas de salida si no existen.
    """

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"No se encontró el archivo de configuración: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # =======================
    # 1. Calcular PERIODO
    # =======================
    hoy = datetime.today()
    mes_config = config["periodo"].get("mes")
    anio_config = config["periodo"].get("anio")

    if mes_config is None or anio_config is None:
        mes = hoy.month - 1 if hoy.month > 1 else 12
        anio = hoy.year if hoy.month > 1 else hoy.year - 1
    else:
        mes = mes_config
        anio = anio_config

    config["periodo"]["mes"] = mes
    config["periodo"]["anio"] = anio
    config["periodo"]["nombre_mes"] = calendar.month_name[mes]
    periodo_str = f"{anio}{mes:02}"

    # =======================
    # 2. Ajustar rutas automáticamente
    # =======================
    base_parquet = "parquet"
    base_output = "output"

    config["rutas"]["parquet_contraprestaciones"] = f"{base_parquet}/contraprestacion_{periodo_str}.parquet"
    config["rutas"]["parquet_bonos"] = f"{base_parquet}/bonos_{periodo_str}.parquet"
    config["rutas"]["parquet_descuentos"] = f"{base_parquet}/descuentos_{periodo_str}.parquet"

    config["rutas"]["salida_pdfs"] = f"{base_output}/pdfs"
    config["rutas"]["salida_logs"] = f"{base_output}/logs_{periodo_str}.txt"
    config["rutas"]["salida_resumen"] = f"{base_output}/resumen_ejecucion_{periodo_str}.json"

    # =======================
    # 3. Validar existencia de parquet
    # =======================
    parquet_paths = [
        config["rutas"]["parquet_contraprestaciones"],
        config["rutas"]["parquet_bonos"],
        config["rutas"]["parquet_descuentos"],
    ]

    for parquet_file in parquet_paths:
        if not os.path.exists(parquet_file):
            raise FileNotFoundError(
                f"No se encontró el archivo necesario para el periodo {calendar.month_name[mes]} {anio}: {parquet_file}"
            )

    # =======================
    # 4. Validar carpetas de salida
    # =======================
    for ruta in [config["rutas"]["salida_pdfs"], os.path.dirname(config["rutas"]["salida_logs"]),
                 os.path.dirname(config["rutas"]["salida_resumen"])]:
        if ruta and not os.path.exists(ruta):
            os.makedirs(ruta, exist_ok=True)

    # =======================
    # 5. Log de confirmación
    # =======================
    logging.info(f"Configuración cargada correctamente para {mes}/{anio}")
    print(f"Configuración cargada correctamente para {calendar.month_name[mes]} {anio}")

    return config
