from pathlib import Path
from datetime import datetime
import pandas as pd
import re

# === Config ===
ROOT = Path("temp_emails")   # si quieres ruta absoluta, cámbiala aquí
OUTPUT = Path("log_correo_existentes.csv")

# regex robusto: toma la cola de dígitos del nombre del archivo (sin extensión)
ID_RE = re.compile(r"(\d+)$")

def extraer_id(nombre_sin_ext: str) -> str | None:
    m = ID_RE.search(nombre_sin_ext)
    if not m:
        return None
    # mantenemos el formato tal cual (incluye ceros a la izquierda)
    return m.group(1)

def main():
    if not ROOT.exists():
        raise SystemExit(f"❌ No existe la carpeta {ROOT.resolve()}")

    # 1) Recorremos TODAS las subcarpetas dentro de temp_emails
    sets_por_carpeta: dict[str, set[str]] = {}
    for sub in sorted([p for p in ROOT.iterdir() if p.is_dir()]):
        sids = set()
        for pdf in sub.glob("*.pdf"):
            sid = extraer_id(pdf.stem)
            if sid:
                sids.add(sid)
        sets_por_carpeta[sub.name] = sids

    # 2) Resumen por carpeta
    print("Resumen PDFs locales:")
    total_union: set[str] = set()
    for nombre, sids in sets_por_carpeta.items():
        print(f"  - {nombre}: {len(sids)}")
        total_union |= sids

    print(f"  -> Unique store_id total (union): {len(total_union)}\n")

    # 3) Generar CSV con la estructura pedida
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    filas = [{
        "fecha_hora": ahora,
        "store_id": sid,
        "tipo": "correo",
        "estado": "enviado",
        "email": "",                 # usamos solo carpeta local → sin email
        "estado_envio": "enviado",
        "fecha_envio": ahora,
    } for sid in sorted(total_union, key=lambda x: int(x))]

    df = pd.DataFrame(filas, columns=[
        "fecha_hora","store_id","tipo","estado","email","estado_envio","fecha_envio"
    ])
    df.to_csv(OUTPUT, index=False, encoding="utf-8")

    print(f"✅ Log generado: {OUTPUT.resolve()}")
    print(f"   Registros en log: {len(df)}")

if __name__ == "__main__":
    main()


