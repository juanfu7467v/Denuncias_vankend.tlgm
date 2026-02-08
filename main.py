import os
import re
import asyncio
import time
import json
import hashlib
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# --- Configuración y Variables de Entorno ---
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip("/")
SESSION_STRING = os.getenv("SESSION_STRING", None)
PORT = int(os.getenv("PORT", 8080))

# --- Configuración Interna ---
DOWNLOAD_DIR = "downloads"
CACHE_DIR = "cache"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# --- Bots Configuración ---
LEDERDATA_PRIMARY_BOT_ID = "@LEDERDATA_OFC_BOT"
LEDERDATA_BACKUP_BOT_ID = "@lederdata_publico_bot"

# --- Timeouts ---
TIMEOUT_PRIMARY = 35  # 35 segundos para bot principal
TIMEOUT_BACKUP = 18   # 18 segundos para bot de respaldo (cuando hay anti-spam)
TIMEOUT_BACKUP_NORMAL = 50  # 50 segundos para respaldo normal

# --- Trackeo de Fallos de Bots ---
bot_fail_tracker = {}

def is_bot_blocked(bot_id: str) -> bool:
    """Verifica si un bot está bloqueado (no responde)"""
    last_fail_time = bot_fail_tracker.get(bot_id)
    if not last_fail_time:
        return False
    now = datetime.now()
    block_time_ago = now - timedelta(hours=3)
    if last_fail_time > block_time_ago:
        return True
    bot_fail_tracker.pop(bot_id, None)
    return False

def record_bot_failure(bot_id: str):
    """Registra un fallo de bot (no respuesta)"""
    bot_fail_tracker[bot_id] = datetime.now()

# --- Sistema de Caché ---
def get_cache_key(command: str, param: str) -> str:
    """Genera una clave única para el caché basada en comando y parámetro"""
    key_string = f"{command}:{param}"
    return hashlib.md5(key_string.encode()).hexdigest()

def get_cached_response(cache_key: str):
    """Obtiene respuesta del caché si existe"""
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return None
    return None

def save_to_cache(cache_key: str, response: dict):
    """Guarda respuesta en caché"""
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(response, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error guardando en caché: {e}")

# --- PARSER UNIVERSAL (MEJORADO PARA JSON ESTRUCTURADO) ---
def universal_parser(raw_text: str):
    """
    Convierte texto plano con formato "CLAVE : VALOR" en JSON.
    Regla global: cada ':' indica clave/valor.

    - Si hay múltiples bloques/registros, devuelve una lista de objetos.
    - Si hay un solo bloque, devuelve un objeto dict.
    - Mantiene claves tal cual aparecen (sin normalizar).
    - Mantiene URLs como texto sin alteraciones.
    """
    if not raw_text or not raw_text.strip():
        return {}

    text = raw_text.replace("\r\n", "\n").replace("\r", "\n").strip()

    # Separación en bloques: dos o más saltos de línea suelen indicar registros separados.
    # (No alteramos lógica del sistema: solo estructuramos mejor la conversión)
    blocks = [b.strip() for b in re.split(r"\n\s*\n+", text) if b.strip()]

    def parse_block(block_text: str) -> dict:
        obj = {}

        # Captura "clave: valor" incluso si el valor ocupa varias líneas
        # hasta que aparezca otra línea con "algo:" o fin de bloque.
        pattern = r'^([^:\n]+?)\s*:\s*(.+?)(?=\n[^:\n]+?\s*:|\Z)'
        for match in re.finditer(pattern, block_text, flags=re.MULTILINE | re.DOTALL):
            key = (match.group(1) or "").strip()
            value = (match.group(2) or "").strip()

            if not key:
                continue

            # Limpieza mínima del valor (sin cambiar contenido)
            value = re.sub(r"[ \t]+", " ", value).strip()
            value = re.sub(r"\n+", "\n", value).strip()

            # Si la clave se repite dentro del mismo bloque, lo convertimos a lista
            if key in obj:
                if isinstance(obj[key], list):
                    obj[key].append(value)
                else:
                    obj[key] = [obj[key], value]
            else:
                obj[key] = value

        return obj

    parsed_blocks = [parse_block(b) for b in blocks]
    parsed_blocks = [b for b in parsed_blocks if b]  # quitar vacíos

    if not parsed_blocks:
        return {}

    # Si hay más de un bloque, se interpreta como múltiples denuncias/registros
    if len(parsed_blocks) > 1:
        return parsed_blocks

    return parsed_blocks[0]

# --- Lógica de Limpieza y Extracción de Datos (LederData) ---
def clean_and_extract(raw_text: str):
    if not raw_text:
        return {"text": "", "fields": {}}

    text = raw_text
    text = re.sub(r"\[#?LEDER_BOT\]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\[CONSULTA PE\]", "", text, flags=re.IGNORECASE)
    header_pattern = r"^\[.*?\]\s*→\s*.*?\[.*?\](\r?\n){1,2}"
    text = re.sub(header_pattern, "", text, flags=re.IGNORECASE | re.DOTALL)
    footer_pattern = r"((\r?\n){1,2}\[|Página\s*\d+\/\d+.*|(\r?\n){1,2}Por favor, usa el formato correcto.*|↞ Anterior|Siguiente ↠.*|Credits\s*:.+|Wanted for\s*:.+|\s*@lederdata.*|(\r?\n){1,2}\s*Marca\s*@lederdata.*|(\r?\n){1,2}\s*Créditos\s*:\s*\d+)"
    text = re.sub(footer_pattern, "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"\-{3,}", "", text, flags=re.IGNORECASE | re.DOTALL)

    # Ojo: aquí se estaba colapsando TODO a 1 línea. Se mantiene tal cual tu lógica actual.
    # (Esto puede dificultar el parsing por bloques, pero no cambiamos más lógica.)
    text = re.sub(r"\s+", " ", text)
    text = text.strip()

    fields = {}

    photo_type_match = re.search(r"Foto\s*:\s*(rostro|huella|firma|adverso|reverso).*", text, re.IGNORECASE)
    if photo_type_match:
        fields["photo_type"] = photo_type_match.group(1).lower()

    not_found_pattern = r"\[⚠️\]\s*(no se encontro información|no se han encontrado resultados|no se encontró una|no hay resultados|no tenemos datos|no se encontraron registros)"
    if re.search(not_found_pattern, text, re.IGNORECASE | re.DOTALL):
        fields["not_found"] = True

    text = re.sub(r"\n\s*\n", "\n", text).strip()
    return {"text": text, "fields": fields}

# --- Función principal con sistema de bots principal/respaldo y caché ---
async def send_telegram_command(command: str, param: str, endpoint_path: str = None):
    client = None
    try:
        if API_ID == 0 or not API_HASH or not SESSION_STRING:
            raise Exception("Credenciales de Telegram no configuradas.")

        session = StringSession(SESSION_STRING)
        client = TelegramClient(session, API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            raise Exception("Cliente no autorizado.")

        # Verificar si el bot principal está bloqueado
        primary_blocked = is_bot_blocked(LEDERDATA_PRIMARY_BOT_ID)

        # Decidir qué bot usar
        if primary_blocked:
            bot_to_use = LEDERDATA_BACKUP_BOT_ID
            use_backup = True
            timeout_val = TIMEOUT_BACKUP_NORMAL
        else:
            bot_to_use = LEDERDATA_PRIMARY_BOT_ID
            use_backup = False
            timeout_val = TIMEOUT_PRIMARY

        all_received_messages = []
        stop_collecting = asyncio.Event()
        last_message_time = [time.time()]
        anti_spam_detected = [False]

        @client.on(events.NewMessage(incoming=True))
        async def temp_handler(event):
            if stop_collecting.is_set():
                return

            try:
                entity = await client.get_entity(bot_to_use)
                if event.sender_id != entity.id:
                    return

                last_message_time[0] = time.time()
                raw_text = event.raw_text or ""

                # Detectar anti-spam
                if "[⛔] ANTI-SPAM" in raw_text and "INTENTA DESPUES" in raw_text:
                    anti_spam_detected[0] = True
                    stop_collecting.set()
                    return

                # Detectar "no se encontró información"
                if re.search(r"\[⚠️\]\s*no se encontro información", raw_text, re.IGNORECASE):
                    stop_collecting.set()
                    return

                cleaned = clean_and_extract(raw_text)

                msg_obj = {
                    "message": cleaned["text"],
                    "fields": cleaned["fields"],
                    "urls": [],
                    "event_message": event.message
                }
                all_received_messages.append(msg_obj)

            except Exception as e:
                print(f"Error en handler: {e}")

        # Enviar comando al bot
        full_command = f"{command} {param}" if param else command
        await client.send_message(bot_to_use, full_command)

        start_time = time.time()

        # Esperar respuesta
        while (time.time() - start_time) < timeout_val:
            if stop_collecting.is_set():
                break

            if all_received_messages and (time.time() - last_message_time[0]) > 4.5:
                break

            await asyncio.sleep(0.5)

        client.remove_event_handler(temp_handler)

        # Manejar anti-spam del bot principal
        if anti_spam_detected[0] and not use_backup:
            print("Anti-spam detectado, usando bot de respaldo...")
            bot_to_use = LEDERDATA_BACKUP_BOT_ID
            use_backup = True
            timeout_val = TIMEOUT_BACKUP

            all_received_messages = []
            stop_collecting.clear()
            last_message_time[0] = time.time()
            anti_spam_detected[0] = False

            @client.on(events.NewMessage(incoming=True))
            async def backup_handler(event):
                if stop_collecting.is_set():
                    return

                try:
                    entity = await client.get_entity(bot_to_use)
                    if event.sender_id != entity.id:
                        return

                    last_message_time[0] = time.time()
                    raw_text = event.raw_text or ""

                    if re.search(r"\[⚠️\]\s*no se encontro información", raw_text, re.IGNORECASE):
                        stop_collecting.set()
                        return

                    cleaned = clean_and_extract(raw_text)

                    msg_obj = {
                        "message": cleaned["text"],
                        "fields": cleaned["fields"],
                        "urls": [],
                        "event_message": event.message
                    }
                    all_received_messages.append(msg_obj)

                except Exception as e:
                    print(f"Error en backup handler: {e}")

            await client.send_message(bot_to_use, full_command)

            start_time = time.time()
            while (time.time() - start_time) < timeout_val:
                if stop_collecting.is_set():
                    break

                if all_received_messages and (time.time() - last_message_time[0]) > 4.5:
                    break

                await asyncio.sleep(0.5)

            client.remove_event_handler(backup_handler)

        # Si no hay respuesta del bot principal y no estamos usando respaldo
        if not all_received_messages and not use_backup:
            record_bot_failure(LEDERDATA_PRIMARY_BOT_ID)
            raise Exception("No se obtuvo respuesta del bot principal.")

        if not all_received_messages:
            raise Exception("No se obtuvo respuesta del bot.")

        return await process_bot_response(client, all_received_messages, endpoint_path)

    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        if client:
            await client.disconnect()

async def process_bot_response(client, all_received_messages, endpoint_path):
    if any("formato correcto" in (m["message"] or "").lower() for m in all_received_messages):
        return {"status": "error", "message": "Formato incorrecto."}

    if any(m.get("fields", {}).get("not_found") for m in all_received_messages):
        return {"status": "error", "message": "No se encontraron resultados."}

    # Descargar archivos adjuntos
    for msg in all_received_messages:
        event_msg = msg.get("event_message")
        if event_msg and getattr(event_msg, "media", None):
            try:
                ext = ".pdf" if "pdf" in str(event_msg.media).lower() else ".jpg"
                fname = f"{int(time.time())}_{event_msg.id}{ext}"
                path = await client.download_media(event_msg, file=os.path.join(DOWNLOAD_DIR, fname))
                if path:
                    msg["urls"].append({"url": f"{PUBLIC_URL}/files/{fname}", "type": "document"})
            except Exception as e:
                print(f"Error descargando archivo: {e}")

    # Combinar texto recibido (misma lógica)
    combined_text = ""
    for msg in all_received_messages:
        if msg.get("message"):
            combined_text += msg.get("message", "") + "\n"
    combined_text = combined_text.strip()

    # Aplicar Parser Universal mejorado
    parsed_data = universal_parser(combined_text)

    # Recolectar URLs
    urls = []
    for msg in all_received_messages:
        urls.extend(msg.get("urls", []))

    # Mantener fields existentes (no se altera lógica)
    final_fields = {}
    for msg in all_received_messages:
        for k, v in msg.get("fields", {}).items():
            if v and not final_fields.get(k):
                final_fields[k] = v

    # Construcción final:
    # - Si parsed_data es lista => múltiples registros => data.denuncias = [...]
    # - Si parsed_data es dict => un registro => data = {...}
    data = {}

    if isinstance(parsed_data, list):
        data["denuncias"] = parsed_data
        if final_fields:
            # Si hubiese fields globales (ej: photo_type), los conservamos sin romper estructura
            # (No se inventa lógica nueva: solo se agregan como campos extra en data)
            data.update(final_fields)
    elif isinstance(parsed_data, dict) and parsed_data:
        data.update(final_fields)
        data.update(parsed_data)
    else:
        # Si no se pudo parsear, devolvemos al menos los fields
        data.update(final_fields)

    # PDFs/Docs deben mantenerse como arreglo "urls" (como ya existe actualmente)
    if urls:
        data["urls"] = urls

    return {
        "status": "success",
        "data": data,
        "raw_message": combined_text
    }

def run_telegram_command_with_cache(command: str, param: str, endpoint_path: str = None):
    """Ejecuta comando con sistema de caché"""
    cache_key = get_cache_key(command, param)

    cached_response = get_cached_response(cache_key)
    if cached_response:
        print(f"Usando respuesta en caché para: {command} {param}")
        return cached_response

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(send_telegram_command(command, param, endpoint_path))

        if result.get("status") == "success":
            save_to_cache(cache_key, result)

        return result
    finally:
        loop.close()

# --- Validaciones de Parámetros ---
def validate_dni(dni: str) -> bool:
    """Valida que el DNI tenga 8 dígitos numéricos"""
    return dni.isdigit() and len(dni) == 8

def validate_ruc(ruc: str) -> bool:
    """Valida que el RUC tenga 11 dígitos numéricos"""
    return ruc.isdigit() and len(ruc) == 11

def validate_ce(ce: str) -> bool:
    """Valida carnet de extranjería (6-12 caracteres)"""
    return 6 <= len(ce) <= 12

def validate_pasaporte(pasaporte: str) -> bool:
    """Valida pasaporte (6-12 caracteres)"""
    return 6 <= len(pasaporte) <= 12

def validate_ci(ci: str) -> bool:
    """Valida cédula de identidad (6-12 caracteres)"""
    return 6 <= len(ci) <= 12

def validate_placa(placa: str) -> bool:
    """Valida placa (5-7 caracteres)"""
    return 5 <= len(placa) <= 7

def validate_serie_armamento(serie: str) -> bool:
    """Valida serie de armamento (5-13 caracteres)"""
    return 5 <= len(serie) <= 13

def validate_clave_denuncia(clave: str) -> bool:
    """Valida clave de denuncia (5-11 caracteres)"""
    return 5 <= len(clave) <= 11

def validate_nombres(nombres: str) -> bool:
    """Valida formato de nombres para búsqueda"""
    parts = nombres.split('|')
    if len(parts) != 3:
        return False
    return any(part.strip() for part in parts)

# --- APP FLASK ---
app = Flask(__name__)
CORS(app)

@app.route("/files/<path:filename>")
def files(filename):
    return send_from_directory(DOWNLOAD_DIR, filename)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"})

@app.route("/status", methods=["GET"])
def status():
    primary_blocked = is_bot_blocked(LEDERDATA_PRIMARY_BOT_ID)
    return jsonify({
        "status": "online",
        "bots": {
            "primary": LEDERDATA_PRIMARY_BOT_ID,
            "backup": LEDERDATA_BACKUP_BOT_ID
        },
        "primary_blocked": primary_blocked,
        "primary_blocked_until": bot_fail_tracker.get(LEDERDATA_PRIMARY_BOT_ID, {}),
        "cache_enabled": True,
        "cache_dir": CACHE_DIR
    })

# --- Endpoints para los comandos solicitados ---

# 1. REQUISITORIAS HISTORICAS
@app.route("/rqh", methods=["GET"])
def rqh_endpoint():
    dni = request.args.get("dni")
    if not dni:
        return jsonify({"status": "error", "message": "Parámetro 'dni' requerido"}), 400

    if not validate_dni(dni):
        return jsonify({"status": "error", "message": "DNI inválido. Debe tener 8 dígitos numéricos."}), 400

    result = run_telegram_command_with_cache("/rqh", dni, "/rqh")
    return jsonify(result)

# 2. DENUNCIAS POLICIALES - DNI
@app.route("/dend", methods=["GET"])
def dend_endpoint():
    dni = request.args.get("dni")
    if not dni:
        return jsonify({"status": "error", "message": "Parámetro 'dni' requerido"}), 400

    if not validate_dni(dni):
        return jsonify({"status": "error", "message": "DNI inválido. Debe tener 8 dígitos numéricos."}), 400

    result = run_telegram_command_with_cache("/dend", dni, "/dend")
    return jsonify(result)

# 3. DENUNCIAS POLICIALES - CARNET EXTRANJERIA
@app.route("/dence", methods=["GET"])
def dence_endpoint():
    ce = request.args.get("ce")
    if not ce:
        return jsonify({"status": "error", "message": "Parámetro 'ce' requerido"}), 400

    if not validate_ce(ce):
        return jsonify({"status": "error", "message": "Carnet de extranjería inválido. Debe tener entre 6 y 12 caracteres."}), 400

    result = run_telegram_command_with_cache("/dence", ce, "/dence")
    return jsonify(result)

# 4. DENUNCIAS POLICIALES - PASAPORTE
@app.route("/denpas", methods=["GET"])
def denpas_endpoint():
    pasaporte = request.args.get("pasaporte")
    if not pasaporte:
        return jsonify({"status": "error", "message": "Parámetro 'pasaporte' requerido"}), 400

    if not validate_pasaporte(pasaporte):
        return jsonify({"status": "error", "message": "Pasaporte inválido. Debe tener entre 6 y 12 caracteres."}), 400

    result = run_telegram_command_with_cache("/denpas", pasaporte, "/denpas")
    return jsonify(result)

# 5. DENUNCIAS POLICIALES - CÉDULA IDENTIDAD
@app.route("/denci", methods=["GET"])
def denci_endpoint():
    ci = request.args.get("ci")
    if not ci:
        return jsonify({"status": "error", "message": "Parámetro 'ci' requerido"}), 400

    if not validate_ci(ci):
        return jsonify({"status": "error", "message": "Cédula de identidad inválida. Debe tener entre 6 y 12 caracteres."}), 400

    result = run_telegram_command_with_cache("/denci", ci, "/denci")
    return jsonify(result)

# 6. DENUNCIAS POLICIALES - PLACA
@app.route("/denp", methods=["GET"])
def denp_endpoint():
    placa = request.args.get("placa")
    if not placa:
        return jsonify({"status": "error", "message": "Parámetro 'placa' requerido"}), 400

    if not validate_placa(placa):
        return jsonify({"status": "error", "message": "Placa inválida. Debe tener entre 5 y 7 caracteres."}), 400

    result = run_telegram_command_with_cache("/denp", placa, "/denp")
    return jsonify(result)

# 7. DENUNCIAS POLICIALES - SERIE ARMAMENTO
@app.route("/denar", methods=["GET"])
def denar_endpoint():
    serie = request.args.get("serie")
    if not serie:
        return jsonify({"status": "error", "message": "Parámetro 'serie' requerido"}), 400

    if not validate_serie_armamento(serie):
        return jsonify({"status": "error", "message": "Serie de armamento inválida. Debe tener entre 5 y 13 caracteres."}), 400

    result = run_telegram_command_with_cache("/denar", serie, "/denar")
    return jsonify(result)

# 8. DENUNCIAS POLICIALES - CLAVE DENUNCIA
@app.route("/dencl", methods=["GET"])
def dencl_endpoint():
    clave = request.args.get("clave")
    if not clave:
        return jsonify({"status": "error", "message": "Parámetro 'clave' requerido"}), 400

    if not validate_clave_denuncia(clave):
        return jsonify({"status": "error", "message": "Clave de denuncia inválida. Debe tener entre 5 y 11 caracteres."}), 400

    result = run_telegram_command_with_cache("/dencl", clave, "/dencl")
    return jsonify(result)

# 9. FISCALIA PERSONAS - DNI
@app.route("/fis", methods=["GET"])
def fis_endpoint():
    dni = request.args.get("dni")
    if not dni:
        return jsonify({"status": "error", "message": "Parámetro 'dni' requerido"}), 400

    if not validate_dni(dni):
        return jsonify({"status": "error", "message": "DNI inválido. Debe tener 8 dígitos numéricos."}), 400

    result = run_telegram_command_with_cache("/fis", dni, "/fis")
    return jsonify(result)

# 10. FISCALIA EMPRESAS - RUC
@app.route("/fisruc", methods=["GET"])
def fisruc_endpoint():
    ruc = request.args.get("ruc")
    if not ruc:
        return jsonify({"status": "error", "message": "Parámetro 'ruc' requerido"}), 400

    if not validate_ruc(ruc):
        return jsonify({"status": "error", "message": "RUC inválido. Debe tener 11 dígitos numéricos."}), 400

    result = run_telegram_command_with_cache("/fisruc", ruc, "/fisruc")
    return jsonify(result)

# 11. FISCALIA PERSONAS NOMBRES
@app.route("/fisnm", methods=["GET"])
def fisnm_endpoint():
    nombres = request.args.get("nombres")
    paterno = request.args.get("paterno", "")
    materno = request.args.get("materno", "")

    if not nombres and not paterno and not materno:
        return jsonify({"status": "error", "message": "Se requiere al menos un parámetro: 'nombres', 'paterno' o 'materno'"}), 400

    # Formato: nombres|paterno|materno
    param = f"{nombres or ''}|{paterno or ''}|{materno or ''}"

    result = run_telegram_command_with_cache("/nm", param, "/fisnm")
    return jsonify(result)

# Endpoint general para comandos personalizados (mantenido por compatibilidad)
@app.route("/command", methods=["GET"])
def command_endpoint():
    """Endpoint para ejecutar comandos personalizados"""
    cmd = request.args.get("cmd")
    param = request.args.get("param", "")

    if not cmd:
        return jsonify({"status": "error", "message": "Parámetro 'cmd' requerido"}), 400

    result = run_telegram_command_with_cache(cmd, param)
    return jsonify(result)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
