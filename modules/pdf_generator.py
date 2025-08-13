import os
import logging
import pdfkit
import boto3
import tempfile
from jinja2 import Environment, FileSystemLoader

class PDFGeneratorBase:
    def __init__(self, config):
        self.config = config
        self.s3_client = boto3.client("s3")
        self.anio = config["periodo"]["anio"]
        self.mes = config["periodo"]["mes"]
        self.periodo_str = f"{self.anio}{self.mes:02d}"
        logging.info(f"[{self.__class__.__name__}] - Inicializado para periodo {self.periodo_str}")

    def _render_pdf(self, template_path: str, context: dict, tipo_actual: str, store_id: str):
        """
        Renderiza PDF desde template Jinja2 y lo sube a S3 directamente.
        """
        env = Environment(loader=FileSystemLoader(os.path.dirname(template_path)))
        template = env.get_template(os.path.basename(template_path))
        html_content = template.render(context)

        config_wkhtml = pdfkit.configuration(
            wkhtmltopdf=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
        )

        options = {
            "page-size": "A4",
            "margin-top": "5mm",
            "margin-right": "5mm",
            "margin-bottom": "25mm",
            "margin-left": "5mm",
            "encoding": "UTF-8",
            "enable-local-file-access": None,
            "print-media-type": ''
        }

        # Crear PDF en archivo temporal
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
            pdfkit.from_string(html_content, tmp_pdf.name, configuration=config_wkhtml, options=options)

            # Extraer info de S3
            ruta_s3 = self.config["rutas"]["salida_s3"][tipo_actual]
            if not ruta_s3.startswith("s3://"):
                raise ValueError(f"Ruta inválida S3: {ruta_s3}")

            bucket = ruta_s3.split("/")[2]
            prefix = "/".join(ruta_s3.split("/")[3:])
            key = f"{prefix}{self.periodo_str}/{store_id}.pdf"

            # Subir a S3
            self.s3_client.upload_file(tmp_pdf.name, bucket, key)
            logging.info(f"[{self.__class__.__name__}] - PDF subido a S3: s3://{bucket}/{key}")



class PDFGeneratorContraprestaciones(PDFGeneratorBase):
    """
    Genera PDFs de Contraprestaciones por agente.
    """

    def generar_individual(self, store_id, df_agentes, datos: dict, config: dict):
        """
        Genera el PDF para un solo agente, dado su store_id.
        """
        agente_row = df_agentes[df_agentes["store_id"] == store_id]
        if agente_row.empty:
            logging.warning(f"[PDFGeneratorContraprestaciones] - Agente {store_id} no encontrado en df_agentes.")
            return
        
        self.generar(agente_row, datos, config)


    def generar(self, df_agentes, datos: dict, config: dict):
        logging.info("[PDFGeneratorContraprestaciones] - Iniciando generación de PDFs")
        
        df_agentes = df_agentes.copy()  # evita el warning
        df_agentes["store_id"] = df_agentes["store_id"].astype(str)

        generados, fallidos = 0, 0

        # Asegurar tipo consistente de store_id
        datos["detalle_contra"]["store_id"] = datos["detalle_contra"]["store_id"].astype(str)

        for _, row in df_agentes.iterrows():
            store_id = row["store_id"]
            agente_ops = datos["detalle_contra"][datos["detalle_contra"]["store_id"] == store_id]

           
            if agente_ops.empty:
                logging.warning(f"[PDFGeneratorContraprestaciones] - Agente {store_id} sin datos para Procesar.")
                fallidos += 1
                continue

            # === Resumen operaciones ===
            resumen = (
                agente_ops.groupby("entity_description")
                .agg(cantidad=("transaction_amount", "count"),
                     total_comision=("comission_amount", "sum"),
                     total_igv=("comission_amount_igv", "sum"),
                     igv=("igv", "sum")
                     
                     )
                .reset_index()
            )
            subtotal_operaciones = resumen["total_comision"].sum()
            subtotal_operaciones_cantidad = resumen["cantidad"].sum()      
            igv_total_operaciones = resumen["igv"].sum() if "igv" in resumen.columns else 0   

            # Ordenar operaciones por fecha y número de operación
            agente_ops = agente_ops.sort_values(by=["transaction_date", "transaction_id"])

            # === Bonos ===
            bonos_agente = datos["detalle_bonos"][datos["detalle_bonos"]["store_id"] == store_id]
            bonos_list = [
                {
                  "descripcion": row["bonus_type"],
                  "monto": row["monto"],
                  "igv": row["igv"]
                }
                for _, row in bonos_agente.iterrows()
            ]
            subtotal_bonos = bonos_agente["monto"].sum() if not bonos_agente.empty else 0
            igv_total_bonos = bonos_agente["igv"].sum() if "igv" in bonos_agente.columns else 0  

            # === Descuentos ===
            desc_agente = datos["detalle_desc"][datos["detalle_desc"]["store_id"] == store_id]
            descuentos_list = [
                {
                  "descripcion": row["discount_type"],
                  "monto": row["monto"],
                  "igv": row["igv"]
                }
                for _, row in desc_agente.iterrows()
            ]
            subtotal_descuentos = desc_agente["monto"].sum() if not desc_agente.empty else 0
            igv_total_descuentos = desc_agente["igv"].sum() if "igv" in desc_agente.columns else 0 

            # === Actualizar TOTAL depósito (Método simplificado) ===
            total_sin_igv = subtotal_operaciones + subtotal_bonos + subtotal_descuentos
            igv_total = round(igv_total_operaciones + igv_total_bonos + igv_total_descuentos, 2)
            total_deposito = round(total_sin_igv + igv_total, 2)

            # === Contexto para el template ===

            anio = config["periodo"]["anio"]
            mes_num = config["periodo"]["mes"]

            # Traducción manual del mes
            meses_es = [
                    "", "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO",
                    "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"
                ]
            nombre_mes = meses_es[mes_num]            

            context = {
                "nombre_mes": nombre_mes,
                "anio": anio,
                "cod_agente": store_id,
                "nombre_agente": row["merchant"],
                "titular": row["store_owner"],
                "direccion": row["address"],
                "provincia": row["province"],
                "departamento": row["region"],
                "periodo": f"{config['periodo']['mes']}/{config['periodo']['anio']}",
                "logo_base64": config["logo_base64"],
                "informacion_adicional": config["textos_fijos"]["informacion_adicional"],
                "nota_importante": config["textos_fijos"]["nota_importante"],
                "campanas_activas": config["textos_fijos"]["campanas_activas"],
                "resumen_entidades": [
                    {
                        "descripcion_entidad": r.entity_description,
                        "cantidad": r.cantidad,
                        "total_comision": r.total_comision
                    }
                    for _, r in resumen.iterrows()
                ],
                "detalle_entidades": [
                    {
                        "nombre": entidad,
                        "operaciones": [
                            {
                                "entidad_financiera": op["entity_description"],  #NUEVO pedido Iris
                                "fecha": op["transaction_date"],
                                "descripcion": op["operation_description"],
                                "pos": op["pos"],
                                "numero_operacion": op["transaction_id"], ### validar para cambiar
                                "importe": float(op["transaction_amount"] or 0.0),
                                "comision": float(op["comission_amount"] or 0.0)
                            }
                            for _, op in agente_ops[agente_ops["entity_description"] == entidad].iterrows()
                        ]
                    }
                    for entidad in resumen["entity_description"]
                ],
                "subtotal_operaciones": subtotal_operaciones,
                "subtotal_operaciones_cantidad": subtotal_operaciones_cantidad,
                "bonos": bonos_list,
                "subtotal_bonos": subtotal_bonos,
                "descuentos": descuentos_list,
                "subtotal_descuentos": subtotal_descuentos,
                "total_sin_igv": total_sin_igv,
                "igv_total": igv_total,
                "total_deposito": total_deposito
            }

            
            output_file = f"{store_id}.pdf" 

            self._render_pdf(config["rutas"]["template_contraprestaciones"], context, tipo_actual="contraprestacion", store_id=store_id)

            logging.info(f"[PDFGeneratorContraprestaciones] - PDF generado: {output_file}")
            generados += 1

        logging.info(f"[PDFGeneratorContraprestaciones] - Completado: {generados} generados, {fallidos} fallidos.")


class PDFGeneratorReembolso(PDFGeneratorBase):
    """
    Genera PDFs de Reembolso por agente.
    """

    def generar_individual(self, store_id, df_agentes, datos: dict, config: dict):
        """
        Genera el PDF para un solo agente, dado su store_id.
        """
        agente_row = df_agentes[df_agentes["store_id"] == store_id]
        if agente_row.empty:
            logging.warning(f"[PDFGeneratorReembolso] - Agente {store_id} no encontrado en df_agentes.")
            return
        
        self.generar(agente_row, datos, config)


    def generar(self, df_agentes, datos: dict, config: dict):
        logging.info("[PDFGeneratorReembolso] - Iniciando generación de PDFs")

        df_agentes = df_agentes.copy()  # evita el warning
        df_agentes["store_id"] = df_agentes["store_id"].astype(str)

        generados, fallidos = 0, 0

        datos["detalle_reembolso"]["store_id"] = datos["detalle_reembolso"]["store_id"].astype(str)
        #df_agentes["store_id"] = df_agentes["store_id"].astype(str)

        for _, row in df_agentes.iterrows():
            store_id = row["store_id"]
            agente_ops = datos["detalle_reembolso"][datos["detalle_reembolso"]["store_id"] == store_id]
            
            agente_ops = agente_ops.sort_values(by=["transaction_date", "transaction_id"])

            if agente_ops.empty:
                logging.warning(f"[PDFGeneratorReembolso] - Agente {store_id} sin datos para Procesar.")
                fallidos += 1
                continue

            # === Resumen agrupado por tipo de reembolso (company_description) ===
            resumen = (
                agente_ops.groupby("company_description")
                .agg(
                    cantidad=("transaction_amount", "count"),
                    total_comision=("comission_amount", "sum")
                )
                .reset_index()
            )

            subtotal_operaciones = resumen["total_comision"].sum()
            subtotal_operaciones_cantidad = resumen["cantidad"].sum()

            # === IGV (cuando esté en el parquet lo tomará directamente) ===
            igv_total_operaciones = agente_ops["igv"].sum() if "igv" in agente_ops.columns else 0

            # === Total Depósito ===
            total_sin_igv = subtotal_operaciones
            igv_total = round(igv_total_operaciones, 2)
            total_deposito = round(total_sin_igv + igv_total, 2)

            # === Contexto para el template ===

            anio = config["periodo"]["anio"]
            mes_num = config["periodo"]["mes"]

            # Traducción manual del mes
            meses_es = [
                    "", "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO",
                    "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"
                ]
            nombre_mes = meses_es[mes_num] 


            context = {
                "nombre_mes": nombre_mes,
                "anio": anio,
                "cod_agente": store_id,
                "nombre_agente": row["merchant"],
                "titular": row["store_owner"],
                "direccion": row["address"],
                "provincia": row["province"],
                "departamento": row["region"],
                "periodo": f"{config['periodo']['mes']}/{config['periodo']['anio']}",
                "logo_base64": config["logo_base64"],
                "informacion_adicional": config["textos_fijos"]["informacion_adicional"],
                "nota_importante": config["textos_fijos"]["nota_importante"],
                "campanas_activas": config["textos_fijos"]["campanas_activas"],
                "resumen_entidades": [
                    {
                        "descripcion_entidad": r.company_description,
                        "cantidad": r.cantidad,
                        "total_comision": r.total_comision
                    }
                    for _, r in resumen.iterrows()
                ],

                "detalle_entidades": [
                    {
                        "nombre": entidad,  # company_description
                        "operaciones": [
                            {
                                "entidad": entidad, 
                                "fecha": op["transaction_date"],
                                "descripcion": op["operation_description"],
                                "pos": op["pos"],
                                "numero_operacion": op["transaction_id"],  # si es necesario renombrar luego
                                "importe": float(op["transaction_amount"] or 0.0),
                                "comision": float(op["comission_amount"] or 0.0)
                            }
                            for _, op in agente_ops[agente_ops["company_description"] == entidad].iterrows()
                        ]
                    }
                    for entidad in resumen["company_description"]
                ],

                "subtotal_operaciones": subtotal_operaciones,
                "subtotal_operaciones_cantidad": subtotal_operaciones_cantidad,
                "total_sin_igv": total_sin_igv,  # NUEVO
                "igv_total": igv_total,          # NUEVO
                "total_deposito": total_deposito # NUEVO

            }

            output_file = f"{store_id}.pdf"
            self._render_pdf(config["rutas"]["template_reembolso"], context, tipo_actual="reembolso", store_id=store_id)

            logging.info(f"[PDFGeneratorReembolso] - PDF generado: {output_file}")
            generados += 1

        logging.info(f"[PDFGeneratorReembolso] - Completado: {generados} generados, {fallidos} fallidos.")


class PDFGeneratorAdquirencia(PDFGeneratorBase):
    """
    Genera PDFs de Adquirencia por agente.
    """

    def generar_individual(self, store_id, df_agentes, datos: dict, config: dict):
        """
        Genera el PDF para un solo agente, dado su store_id.
        """
        agente_row = df_agentes[df_agentes["store_id"] == store_id]
        if agente_row.empty:
            logging.warning(f"[PDFGeneratorAdquirencia] - Agente {store_id} no encontrado en df_agentes.")
            return
        
        self.generar(agente_row, datos, config)

    def generar(self, df_agentes, datos: dict, config: dict):
        logging.info("[PDFGeneratorAdquirencia] - Iniciando generación de PDFs")
        
        df_agentes = df_agentes.copy()
        df_agentes["store_id"] = df_agentes["store_id"].astype(str)

        generados, fallidos = 0, 0

        datos["detalle_adquirencia"]["store_id"] = datos["detalle_adquirencia"]["store_id"].astype(str)

        for _, row in df_agentes.iterrows():
            store_id = row["store_id"]
            agente_ops = datos["detalle_adquirencia"][datos["detalle_adquirencia"]["store_id"] == store_id]

            agente_ops = agente_ops.sort_values(by=["transaction_date", "entity_transaction_id"])

            if agente_ops.empty:
                logging.warning(f"[PDFGeneratorAdquirencia] - Agente {store_id} sin datos para Procesar.")
                fallidos += 1
                continue

            ### === Resumen agrupado por fecha ===
            resumen = (
                agente_ops.groupby("transaction_date")
                .agg(
                    cantidad=("transaction_amount", "count"),
                    importe_venta=("transaction_amount", "sum"),
                    comision_venta=("comission_amount_igv", "sum"),
                    importe_abonado=("importe_abonado", "sum")
                )
                .reset_index()
            )

            total_cantidad = resumen["cantidad"].sum()
            total_importe_venta = resumen["importe_venta"].sum()
            total_comision_venta = resumen["comision_venta"].sum()
            total_importe_abonado = resumen["importe_abonado"].sum()

            ### === Detalle por fecha (Página 2 en adelante) ===
            detalle_adquirencia = [
                        {
                            "fecha": op["transaction_date"],
                            "hora": op["transaction_hour"],
                            "pos": op["pos"],
                            "numero_operacion": op["entity_transaction_id"],
                            "importe_venta": op["transaction_amount"],
                            "comision_venta": op["comission_amount_igv"],
                            "importe_abonado": op["importe_abonado"]
                        }
                        for _, op in agente_ops.iterrows()
                    ]

            ### === Contexto para el template ===

            anio = config["periodo"]["anio"]
            mes_num = config["periodo"]["mes"]

            ### Traducción manual del mes
            meses_es = [
                    "", "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO",
                    "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"
                ]
            nombre_mes = meses_es[mes_num] 

            context = {
                "nombre_mes": nombre_mes,
                "anio": anio,
                "cod_agente": store_id,
                "nombre_agente": row["merchant"],
                "titular": row["store_owner"],
                "direccion": row["address"],
                "provincia": row["province"],
                "departamento": row["region"],
                "periodo": f"{config['periodo']['mes']}/{config['periodo']['anio']}",
                "logo_base64": config["logo_base64"],
                "informacion_adicional": config["textos_fijos"]["informacion_adicional"],
                "nota_importante": config["textos_fijos"]["nota_importante"],
                "campanas_activas": config["textos_fijos"]["campanas_activas"],

                "resumen_adquirencia": [
                    {
                        "fecha": r.transaction_date,
                        "cantidad": r.cantidad,
                        "importe_venta": r.importe_venta,
                        "comision_venta": r.comision_venta,
                        "importe_abonado": r.importe_abonado
                    }
                    for _, r in resumen.iterrows()
                ],
                "total_cantidad": total_cantidad,
                "total_importe_venta": total_importe_venta,
                "total_comision_venta": total_comision_venta,
                "total_importe_abonado": total_importe_abonado,

                "detalle_adquirencia": detalle_adquirencia

            }
            
            ### nuevo
            output_file = f"{store_id}.pdf"
            self._render_pdf(config["rutas"]["template_adquirencia"], context, tipo_actual="adquirencia", store_id=store_id)

            logging.info(f"[PDFGeneratorAdquirencia] - PDF generado: {output_file}")
            generados += 1

        logging.info(f"[PDFGeneratorAdquirencia] - Completado: {generados} generados, {fallidos} fallidos.")
