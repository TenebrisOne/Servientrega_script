import os
import sys
import logging
import requests
from flask import Flask, request, jsonify
from odoo_rpc import safe_read, safe_write, message_post, create
from dotenv import load_dotenv

from xml.etree.ElementTree import fromstring

# --------------------------------------------------
# ENV & SWITCHES
# --------------------------------------------------
load_dotenv()

PORT = int(os.getenv("PORT", "5000"))

# Switch de Odoo (se importa de odoo_rpc pero aquí lo usamos para logs)
from odoo_rpc import USE_PRODUCTION

# Switch de Servientrega
SERVI_USE_PRODUCTION = os.getenv("SERVI_USE_PRODUCTION", "false").lower() in [
    "true",
    "1",
    "yes",
]

if SERVI_USE_PRODUCTION:
    SERVI_URL = os.getenv("SERVI_URL_PROD")
    SERVI_MSG = "🚀 SERVIENTREGA: PRODUCCIÓN"
else:
    SERVI_URL = os.getenv("SERVI_URL_QA")
    SERVI_MSG = "🧪 SERVIENTREGA: PRUEBAS (QA)"

SERVI_TIMEOUT = int(os.getenv("SERVI_TIMEOUT", "35"))

if not SERVI_URL:
    raise RuntimeError("No se pudo determinar SERVI_URL (faltan variables en .env)")

# --------------------------------------------------
# LOGGING
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("servientrega_webhook")

# --------------------------------------------------
# FLASK
# --------------------------------------------------
app = Flask(__name__)
logger.info("🔥 webhook_servientrega_ws22.py CARGADO")
logger.info("📍 ODOO: %s", "🚀 PRODUCCIÓN" if USE_PRODUCTION else "🧪 PRUEBAS")
logger.info("📍 %s", SERVI_MSG)


# --------------------------------------------------
# ENDPOINTS BASE
# --------------------------------------------------
@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.get("/ping")
def ping():
    return "PONG", 200


# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def error_response(code, detail, http_code=400):
    logger.warning("Error %s | %s", code, detail)
    return jsonify({"error": code, "detail": detail}), http_code


def safe_read_one(model, record_id, fields):
    if record_id is None:
        logger.warning("Intentando leer %s con ID=None", model)
        return None
    try:
        logger.info("Leyendo %s ID=%s", model, record_id)
        ok, resp, _ = safe_read(model, [int(record_id)], fields)
        if not ok or "result" not in resp or not resp["result"]:
            logger.warning("No se encontró %s ID=%s", model, record_id)
            return None
        return resp["result"][0]
    except (ValueError, TypeError) as e:
        logger.error("Error al convertir ID: %s", str(e))
        return None


# --------------------------------------------------
# VALIDACIÓN PICKING
# --------------------------------------------------
def validate_picking(picking, shipping_partner_id):
    logger.info("Validando picking %s", picking.get("name"))

    errors = []

    if picking.get("state") != "done":
        errors.append("El picking no está en estado DONE")

    if picking.get("carrier_tracking_ref"):
        errors.append("El picking ya tiene guía asignada")

    if not shipping_partner_id:
        errors.append("No se pudo resolver dirección de entrega")

    if not picking.get("move_line_ids"):
        errors.append("El picking no tiene líneas de producto")

    if errors:
        raise ValueError(" | ".join(errors))


# --------------------------------------------------
# WS22 PAYLOAD
# --------------------------------------------------
def construir_payload_ws22(picking, partner, valor_real=5000, contenido="PRODUCTOS"):
    logger.info("🧩 Construyendo payload WS22")

    # Peso Dínámico (Mínimo 1kg)
    peso_real = float(picking.get("shipping_weight") or picking.get("weight") or 1.0)
    if peso_real < 1:
        peso_real = 1.0

    # Valor Declarado Dinámico (Mínimo 5000)
    valor_declarado = 0.0
    # Intentar calcular valor desde los movimientos de stock (precio del producto * cantidad)
    # Nota: Odoo devuelve los IDs, habría que haber leído los moves.
    # Para simplificar y no hacer más lecturas costosas, si no tenemos valor, usamos 5000.
    # Si quieres valor exacto, necesitamos leer 'stock.move' con 'price_unit' y 'product_uom_qty'.
    # Por ahora, usaré una lógica segura:
    valor_declarado = 5000.0  # Placeholder seguro.
    # TODO: Si el usuario quiere VALOR REAL, debemos leer los moves.
    # Voy a implementar la lectura de moves abajo en el webhook() para pasarla aquí.

    payload = {
        "envios": [
            {
                "referencia": picking["name"],
                "contenido": contenido,
                "tipoEnvio": "NORMAL",
                "formaPago": "CREDITO",
                "numeroPiezas": 1,
                "pesoTotal": peso_real,
                "valorDeclarado": valor_real,
                "remitente": {
                    "nombre": "WONDERTECH S.A.S",
                    "direccion": "Cra 00 #00-00",
                    "ciudad": "BOGOTA",
                    "pais": "CO",
                    "telefono": "0000000",
                },
                "destinatario": {
                    "nombre": partner["name"],
                    "direccion": partner["street"],
                    "ciudad": partner["city"],
                    "pais": "CO",
                    "telefono": partner.get("phone") or partner.get("mobile") or "",
                    "identificacion": partner.get("vat") or "0000000000",
                },
            }
        ]
    }

    logger.info("📦 Payload WS22 construido correctamente")
    return payload


# --------------------------------------------------
# WS22 SEND SOAP (QA) - CargueMasivoExterno
# --------------------------------------------------
def enviar_ws22_test(payload_ws22: dict) -> dict:
    logger.info("🚀 Enviando WS22 SOAP")
    logger.info("🌐 URL usada: %s", SERVI_URL)

    envio = payload_ws22["envios"][0]

    soap_xml = f"""<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:tem="http://tempuri.org/">
   <soap:Header>
      <tem:AuthHeader>
         <tem:login>{os.getenv("SERVI_LOGIN")}</tem:login>
         <tem:pwd>{os.getenv("SERVI_PWD_ENC")}</tem:pwd>
         <tem:Id_CodFacturacion>{os.getenv("SERVI_COD_FACT")}</tem:Id_CodFacturacion>
         <tem:Nombre_Cargue>Odoo Servientrega</tem:Nombre_Cargue>
      </tem:AuthHeader>
   </soap:Header>
   <soap:Body>
      <tem:CargueMasivoExterno>
         <tem:envios>
            <tem:CargueMasivoExternoDTO>
               <tem:objEnvios>
                  <tem:EnviosExterno>
                     <tem:Num_Guia>0</tem:Num_Guia>
                     <tem:Num_Sobreporte>0</tem:Num_Sobreporte>
                     <tem:Num_SobreCajaPorte>0</tem:Num_SobreCajaPorte>
                     <tem:Fec_TiempoEntrega>1</tem:Fec_TiempoEntrega>
                     <tem:Des_TipoTrayecto>1</tem:Des_TipoTrayecto>
                     <tem:Ide_CodFacturacion>{os.getenv("SERVI_COD_FACT")}</tem:Ide_CodFacturacion>
                     <tem:Num_Piezas>{envio["numeroPiezas"]}</tem:Num_Piezas>
                     <tem:Des_FormaPago>2</tem:Des_FormaPago>
                     <tem:Des_MedioTransporte>1</tem:Des_MedioTransporte>
                     <tem:Des_TipoDuracionTrayecto>1</tem:Des_TipoDuracionTrayecto>
                     <tem:Nom_TipoTrayecto>1</tem:Nom_TipoTrayecto>
                     <tem:Num_Alto>5</tem:Num_Alto>
                     <tem:Num_Ancho>5</tem:Num_Ancho>
                     <tem:Num_Largo>5</tem:Num_Largo>
                     <tem:Num_PesoTotal>{envio["pesoTotal"]}</tem:Num_PesoTotal>
                     <tem:Des_UnidadLongitud>cm</tem:Des_UnidadLongitud>
                     <tem:Des_UnidadPeso>kg</tem:Des_UnidadPeso>
                     <tem:Nom_UnidadEmpaque>GENERICA</tem:Nom_UnidadEmpaque>
                     <tem:Gen_Cajaporte>false</tem:Gen_Cajaporte>
                     <tem:Gen_Sobreporte>false</tem:Gen_Sobreporte>
                     <tem:Des_DiceContenerSobre></tem:Des_DiceContenerSobre>
                     <tem:Doc_Relacionado>{envio["referencia"]}</tem:Doc_Relacionado>
                     <tem:Des_VlrCampoPersonalizado1></tem:Des_VlrCampoPersonalizado1>
                     <tem:Ide_Num_Referencia_Dest></tem:Ide_Num_Referencia_Dest>
                     <tem:Num_Factura></tem:Num_Factura>
                     <tem:Ide_Producto>2</tem:Ide_Producto>
                     <tem:Num_Recaudo>0</tem:Num_Recaudo>
                     <tem:Ide_Destinatarios>00000000-0000-0000-0000-000000000000</tem:Ide_Destinatarios>
                     <tem:Ide_Manifiesto>00000000-0000-0000-0000-000000000000</tem:Ide_Manifiesto>
                     <tem:Num_BolsaSeguridad>0</tem:Num_BolsaSeguridad>
                     <tem:Num_Precinto>0</tem:Num_Precinto>
                     <tem:Num_VolumenTotal>0</tem:Num_VolumenTotal>
                     <tem:Des_DireccionRecogida></tem:Des_DireccionRecogida>
                     <tem:Des_TelefonoRecogida></tem:Des_TelefonoRecogida>
                     <tem:Des_CiudadRecogida></tem:Des_CiudadRecogida>
                     <tem:Num_PesoFacturado>0</tem:Num_PesoFacturado>
                     <tem:Des_TipoGuia>2</tem:Des_TipoGuia>
                     <tem:Id_ArchivoCargar></tem:Id_ArchivoCargar>
                     <tem:Des_CiudadOrigen>0</tem:Des_CiudadOrigen>
                     <tem:Num_ValorDeclaradoTotal>{envio["valorDeclarado"]}</tem:Num_ValorDeclaradoTotal>
                     <tem:Num_ValorLiquidado>0</tem:Num_ValorLiquidado>
                     <tem:Num_VlrSobreflete>0</tem:Num_VlrSobreflete>
                     <tem:Num_VlrFlete>0</tem:Num_VlrFlete>
                     <tem:Num_Descuento>0</tem:Num_Descuento>
                     <tem:Num_ValorDeclaradoSobreTotal>0</tem:Num_ValorDeclaradoSobreTotal>
                     <tem:Des_Telefono>{envio["destinatario"]["telefono"]}</tem:Des_Telefono>
                     <tem:Des_Ciudad>11001000</tem:Des_Ciudad>
                     <tem:Des_DepartamentoDestino>11001000</tem:Des_DepartamentoDestino>
                     <tem:Des_Direccion>{envio["destinatario"]["direccion"]}</tem:Des_Direccion>
                     <tem:Nom_Contacto>{envio["destinatario"]["nombre"]}</tem:Nom_Contacto>
                     <tem:Des_DiceContener>{envio["contenido"]}</tem:Des_DiceContener>
                     <tem:Ide_Num_Identific_Dest>{envio["destinatario"]["identificacion"]}</tem:Ide_Num_Identific_Dest>
                     <tem:Tipo_Doc_Destinatario>NIT</tem:Tipo_Doc_Destinatario>
                     <tem:Num_Celular></tem:Num_Celular>
                     <tem:Des_CorreoElectronico></tem:Des_CorreoElectronico>
                     <tem:Des_CiudadRemitente></tem:Des_CiudadRemitente>
                     <tem:Des_DireccionRemitente></tem:Des_DireccionRemitente>
                     <tem:Des_DepartamentoOrigen></tem:Des_DepartamentoOrigen>
                     <tem:Num_TelefonoRemitente></tem:Num_TelefonoRemitente>
                     <tem:Num_IdentiRemitente></tem:Num_IdentiRemitente>
                     <tem:Nom_Remitente></tem:Nom_Remitente>
                     <tem:nombrecontacto_remitente></tem:nombrecontacto_remitente>
                     <tem:celular_remitente></tem:celular_remitente>
                     <tem:correo_remitente></tem:correo_remitente>
                     <tem:Est_CanalMayorista>false</tem:Est_CanalMayorista>
                     <tem:Nom_RemitenteCanal></tem:Nom_RemitenteCanal>
                     <tem:Des_IdArchivoOrigen>123</tem:Des_IdArchivoOrigen>
                     <tem:objEnviosUnidadEmpaqueCargue>
                        <tem:EnviosUnidadEmpaqueCargue>
                           <tem:Num_Alto>5</tem:Num_Alto>
                           <tem:Num_Distribuidor>0</tem:Num_Distribuidor>
                           <tem:Num_Ancho>5</tem:Num_Ancho>
                           <tem:Num_Cantidad>1</tem:Num_Cantidad>
                           <tem:Des_DiceContener>{envio["contenido"]}</tem:Des_DiceContener>
                           <tem:Des_IdArchivoOrigen>123</tem:Des_IdArchivoOrigen>
                           <tem:Num_Largo>5</tem:Num_Largo>
                           <tem:Nom_UnidadEmpaque>GENERICA</tem:Nom_UnidadEmpaque>
                           <tem:Num_Peso>1</tem:Num_Peso>
                           <tem:Des_UnidadLongitud>cm</tem:Des_UnidadLongitud>
                           <tem:Des_UnidadPeso>kg</tem:Des_UnidadPeso>
                           <tem:Ide_UnidadEmpaque>00000000-0000-0000-0000-000000000000</tem:Ide_UnidadEmpaque>
                           <tem:Ide_Envio>00000000-0000-0000-0000-000000000000</tem:Ide_Envio>
                           <tem:Num_Volumen>0</tem:Num_Volumen>
                           <tem:Num_Consecutivo>0</tem:Num_Consecutivo>
                           <tem:Cod_Facturacion></tem:Cod_Facturacion>
                           <tem:Num_ValorDeclarado>{envio["valorDeclarado"]}</tem:Num_ValorDeclarado>
                           <tem:Indicador>1</tem:Indicador>
                           <tem:NumeroDeCaja></tem:NumeroDeCaja>
                           <tem:Id_archivo></tem:Id_archivo>
                        </tem:EnviosUnidadEmpaqueCargue>
                     </tem:objEnviosUnidadEmpaqueCargue>
                  </tem:EnviosExterno>
               </tem:objEnvios>
            </tem:CargueMasivoExternoDTO>
         </tem:envios>
      </tem:CargueMasivoExterno>
   </soap:Body>
</soap:Envelope>"""

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
    }

    logger.info("📤 SOAP XML ENVIADO:\n%s", soap_xml)

    resp = requests.post(
        SERVI_URL,
        data=soap_xml.encode("utf-8"),
        headers=headers,
        timeout=SERVI_TIMEOUT,
    )

    logger.info("📡 WS22 HTTP %s", resp.status_code)
    logger.info("📥 WS22 RESPONSE RAW:\n%s", resp.text)

    if resp.status_code != 200:
        return {"ok": False, "raw": resp.text}

    return {"ok": True, "raw": resp.text}


# --------------------------------------------------
# GENERAR PDF DE LA GUÍA - GenerarGuiaSticker
# --------------------------------------------------
def generar_pdf_guia(num_guia: str) -> dict:
    logger.info("📄 Generando PDF para guía %s", num_guia)

    soap_xml = f"""<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:tem="http://tempuri.org/">
   <soap:Header>
      <tem:AuthHeader>
         <tem:login>{os.getenv("SERVI_LOGIN")}</tem:login>
         <tem:pwd>{os.getenv("SERVI_PWD_ENC")}</tem:pwd>
         <tem:Id_CodFacturacion>{os.getenv("SERVI_COD_FACT")}</tem:Id_CodFacturacion>
         <tem:Nombre_Cargue>Odoo Servientrega</tem:Nombre_Cargue>
      </tem:AuthHeader>
   </soap:Header>
   <soap:Body>
      <tem:GenerarGuiaSticker>
         <tem:num_Guia>{num_guia}</tem:num_Guia>
         <tem:num_GuiaFinal>{num_guia}</tem:num_GuiaFinal>
         <tem:ide_CodFacturacion>{os.getenv("SERVI_COD_FACT")}</tem:ide_CodFacturacion>
         <tem:sFormatoImpresionGuia>1</tem:sFormatoImpresionGuia>
         <tem:interno>false</tem:interno>
      </tem:GenerarGuiaSticker>
   </soap:Body>
</soap:Envelope>"""

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
    }

    logger.info("📤 Solicitando PDF de guía...")

    resp = requests.post(
        SERVI_URL,
        data=soap_xml.encode("utf-8"),
        headers=headers,
        timeout=SERVI_TIMEOUT,
    )

    logger.info("📡 PDF HTTP %s", resp.status_code)

    if resp.status_code != 200:
        logger.error("❌ Error al generar PDF: HTTP %s", resp.status_code)
        return {"ok": False, "error": f"HTTP {resp.status_code}"}
    # Parsear respuesta para obtener el PDF en base64
    try:
        root = fromstring(resp.text)
        pdf_b64 = root.findtext(".//{http://tempuri.org/}bytesReport") or root.findtext(
            ".//bytesReport"
        )

        if pdf_b64:
            logger.info("✅ PDF generado correctamente")
            return {"ok": True, "pdf_base64": pdf_b64}
        else:
            logger.error("❌ No se encontró el PDF en la respuesta")
            return {"ok": False, "error": "No se encontró bytesReport en la respuesta"}
    except Exception as e:
        logger.error("❌ Error al parsear respuesta PDF: %s", str(e))
        return {"ok": False, "error": str(e)}


# --------------------------------------------------
# WS22 PARSE RESPONSE XML (PASO 5 REAL)
# --------------------------------------------------
def parsear_respuesta_ws22_xml(xml_text: str) -> dict:
    logger.info("🧪 Parseando respuesta WS22 XML")

    root = fromstring(xml_text)

    # Intentar varios posibles nombres de campo para el número de guía
    guia = (
        root.findtext(".//{http://tempuri.org/}Num_Guia")
        or root.findtext(".//Num_Guia")
        or root.findtext(".//{http://tempuri.org/}NumeroGuia")
        or root.findtext(".//NumeroGuia")
    )

    if guia and guia != "0":
        logger.info("✅ Guía obtenida: %s", guia)
        return {"ok": True, "guia": guia}

    # Si no hay guía, buscar mensajes de error en arrayGuias
    errores = []
    for string_elem in root.findall(".//{http://tempuri.org/}string"):
        if string_elem.text:
            errores.append(string_elem.text)

    if errores:
        mensaje_error = " | ".join(errores)
        logger.error("❌ Error en WS22: %s", mensaje_error)
        return {"ok": False, "mensaje": mensaje_error}

    return {"ok": False, "mensaje": "Respuesta sin número de guía"}


# --------------------------------------------------
# PERSISTIR RESULTADO EN ODOO
# --------------------------------------------------
def persistir_resultado_ws22(
    picking_id: int, num_guia: str, url_rastreo: str, pdf_base64: str = None
):
    logger.info("💾 Persistiendo guía %s en picking ID=%s", num_guia, picking_id)

    ok, resp, _ = safe_write(
        "stock.picking",
        [picking_id],
        {
            "carrier_tracking_ref": num_guia,
            "carrier_tracking_url": url_rastreo,
            "x_studio_servientrega": True,
            "x_studio_tcc": False,
        },
    )

    if ok:
        logger.info("✅ Guía persistida correctamente")
        message_post(
            "stock.picking", picking_id, f"✅ Guía Servientrega generada: {num_guia}"
        )

        if pdf_base64:
            logger.info("📎 Adjuntando PDF...")
            create(
                "ir.attachment",
                {
                    "name": f"Guia_{num_guia}.pdf",
                    "type": "binary",
                    "datas": pdf_base64,
                    "res_model": "stock.picking",
                    "res_id": picking_id,
                    "mimetype": "application/pdf",
                },
            )
            logger.info("✅ PDF adjuntado correctamente")

    else:
        logger.error("❌ Error al persistir guía: %s", resp)


# --------------------------------------------------
# WEBHOOK
# --------------------------------------------------
@app.post("/webhook")
def webhook():
    payload = request.get_json(silent=True) or {}
    logger.info("Payload recibido: %s", payload)

    # 🛑 FILTRO DE SEGURIDAD: Solo procesar stock.picking
    # Evita el ruido de otros webhooks (como Instagram/mail.message)
    modelo = payload.get("_model")
    if modelo and modelo != "stock.picking":
        logger.info(
            "⏭️ Ignorando webhook del modelo: %s (Solo procesamos stock.picking)", modelo
        )
        return (
            jsonify({"ok": True, "skipped": True, "reason": "non_picking_model"}),
            200,
        )

    # Buscamos el ID en 'id' o en '_id' (formato común en Odoo)
    picking_id = payload.get("id") or payload.get("_id")

    if picking_id is None or str(picking_id).strip() == "":
        return error_response(
            "missing_id",
            "No se encontró 'id' o '_id' en el JSON. Payload recibido: " + str(payload),
            400,
        )

    try:
        picking_id = int(picking_id)
    except (TypeError, ValueError):
        return error_response(
            "invalid_id",
            "El campo 'id' debe ser numérico.",
            400,
        )

    # 📋 Determinar campos a leer (Evita error si x_studio_servientrega no existe en Prod)
    fields_to_read = [
        "id",
        "name",
        "state",
        "carrier_tracking_ref",
        "move_line_ids",
        "partner_id",
        "shipping_weight",
        "weight",
        "move_ids",
        "carrier_id",
    ]
    if not USE_PRODUCTION:
        fields_to_read.append("x_studio_servientrega")

    picking = safe_read_one("stock.picking", picking_id, fields_to_read)

    if not picking:
        return error_response(
            "picking_not_found",
            f"No se encontró stock.picking con ID={picking_id}",
            404,
        )

    # 🛡️ VALIDACIÓN DE IDEMPOTENCIA (Solo en Producción para evitar cobros dobles)
    # Si ya tiene guía, devolvemos la existente y no llamamos a Servientrega
    if SERVI_USE_PRODUCTION and picking.get("carrier_tracking_ref"):
        guia = picking["carrier_tracking_ref"]
        url = f"https://www.servientrega.com/rastreo/{guia}"
        logger.info(
            "⚠️ El picking %s ya tiene guía: %s. Saltando duplicado.", picking_id, guia
        )
        return (
            jsonify(
                {
                    "ok": True,
                    "guia": guia,
                    "url": url,
                    "message": "Guía ya existente en Odoo. No se generó una nueva.",
                }
            ),
            200,
        )

    # 🏁 VALIDACIÓN: ¿Es Servientrega?
    # En producción solo usamos carrier_id. En pruebas usamos carrier_id O el check.
    es_carrier = False
    if picking.get("carrier_id"):
        c_name = str(picking["carrier_id"][1]).upper()
        if "SERVIENTREGA" in c_name:
            es_carrier = True

    es_check = False
    if not USE_PRODUCTION:
        es_check = picking.get("x_studio_servientrega")

    if not (es_carrier or es_check):
        logger.info(
            "🚫 No es Servientrega (Check=%s, Carrier=%s). Saltando.",
            es_check,
            es_carrier,
        )
        return jsonify({"ok": True, "skipped": True}), 200

    partner = safe_read_one(
        "res.partner",
        picking["partner_id"][0],
        ["name", "street", "city", "phone", "mobile", "vat"],
    )

    if not partner:
        return error_response(
            "partner_not_found",
            f"No se encontró res.partner asociado al picking (partner_id={picking.get('partner_id')}).",
            404,
        )

    ok_moves, resp_moves, _ = safe_read(
        "stock.move",
        picking.get("move_ids") or [],
        ["product_id", "product_uom_qty", "price_unit"],
    )
    moves = (resp_moves or {}).get("result", []) if ok_moves else []

    valor_total = sum(
        [(m.get("product_uom_qty") or 0) * (m.get("price_unit") or 0) for m in moves]
    )
    if valor_total < 5000:
        valor_total = 5000

    # --- MEJORA: Limpieza de nombres para la guía (Max 50 chars) ---
    logger.info(
        "🔍 Productos encontrados en stock.move: %s",
        [m["product_id"][1] for m in moves if m.get("product_id")],
    )

    nombres_cortos = []
    for m in moves:
        if m.get("product_id"):
            full_name = m["product_id"][1]

            # 1. Intentar tomar lo que hay después del ]
            if "]" in full_name:
                name_after_bracket = full_name.split("]", 1)[1].strip()
            else:
                name_after_bracket = full_name.strip()

            # 2. Tomar las dos primeras palabras
            words = name_after_bracket.split()
            short_name = " ".join(words[:2])

            if short_name:
                nombres_cortos.append(short_name)

    # Unir productos y recortar a 50 caracteres (Límite de la API)
    contenido = ", ".join(nombres_cortos)[:50]

    if not contenido:
        contenido = "MERCANCIA GENERAL"

    logger.info("📦 Contenido final para la guía: %s", contenido)

    ws22_payload = construir_payload_ws22(
        picking, partner, valor_real=valor_total, contenido=contenido
    )
    envio = enviar_ws22_test(ws22_payload)

    resultado = parsear_respuesta_ws22_xml(envio["raw"])

    if resultado.get("ok"):
        guia = resultado["guia"]
        url = f"https://www.servientrega.com/rastreo/{guia}"

        # Generar PDF de la guía
        pdf_result = generar_pdf_guia(guia)
        pdf_base64 = pdf_result.get("pdf_base64") if pdf_result.get("ok") else None

        persistir_resultado_ws22(picking_id, guia, url, pdf_base64)

        return jsonify({"ok": True, "guia": guia, "url": url}), 200

    return jsonify({"ok": False, "detail": resultado}), 502


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
