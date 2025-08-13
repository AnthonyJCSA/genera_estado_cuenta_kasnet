import json
import os
import logging
from datetime import datetime
import calendar

def guardar_resumen(config, total_agentes, generados, fallidos, correos_enviados, tiempo_ejecucion, detalles):
    """
    Guarda un resumen de la ejecución en un archivo JSON.
    El nombre del archivo se genera automáticamente según el periodo (mes y año).
    """

    try:
        # ============================
        # 1. DETERMINAR PERIODO Y NOMBRE DE ARCHIVO
        # ============================
        mes = config["periodo"]["mes"]
        anio = config["periodo"]["anio"]
        nombre_mes = calendar.month_name[mes]

        # Si el archivo de salida no está definido, se genera automáticamente
        ruta_resumen = config["rutas"].get(
            "salida_resumen",
            f"output/resumen_ejecucion_{anio}{mes:02d}.json"
        )

        # Asegurar que la carpeta existe
        os.makedirs(os.path.dirname(ruta_resumen), exist_ok=True)

        # ============================
        # 2. CREAR ESTRUCTURA DEL RESUMEN
        # ============================
        resumen = {
            "periodo": f"{nombre_mes} {anio}",
            "fecha_ejecucion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_agentes": total_agentes,
            "pdfs_generados": generados,
            "pdfs_fallidos": fallidos,
            "correos_enviados": correos_enviados,
            "tiempo_total_segundos": round(tiempo_ejecucion, 2),
            "detalles": detalles
        }

        # ============================
        # 3. GUARDAR ARCHIVO
        # ============================
        with open(ruta_resumen, "w", encoding="utf-8") as f:
            json.dump(resumen, f, indent=4, ensure_ascii=False)

        logging.info(f"Resumen guardado en {ruta_resumen}")

    except Exception as e:
        logging.error(f"Error guardando resumen: {e}")



