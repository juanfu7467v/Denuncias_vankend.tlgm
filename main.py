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
TIMEOUT_PRIMARY = 35
TIMEOUT_BACKUP = 18
TIMEOUT_BACKUP_NORMAL = 50

# --- Trackeo de Fallos de Bots ---
bot_fail_tracker = {}

def is_bot_blocked(bot_id: str) -> bool:
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
    bot_fail_tracker[bot_id] = datetime.now()

# --- Sistema de Caché ---
def get_cache_key(command: str, param: str) -> str:
    key_string = f"{command}:{param}"
    return hashlib.md5(key_string.encode()).hexdigest()

def get_cached_response(cache_key: str):
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return None
    return None

def save_to_cache(cache_key: str, response: dict):
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    try:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(response, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error guardando en caché: {e}")

# -------------------------------------------------------------------
# PARSER UNIVERSAL "SI O SI" A JSON LIMPIO
# -------------------------------------------------------------------
def _extract_pairs_anywhere(text: str):
    """
    Extrae pares (clave, valor) aunque estén en la misma línea:
    ... DNI : 100 FECHA : 01-01 ...
    """
    if not text:
        return []

    t = text.replace("\r\n", "\n").replace("\r", "\n")

    # Captura "CLAVE : VALOR" donde VALOR es "lo que sea" hasta el siguiente "CLAVE :"
    # Soporta claves con espacios, letras, números, símbolos comunes.
    pattern = r'([^:\n]+?)\s*:\s*(.*?)(?=(?:\s+[^:\n]+?\s*:\s*)|\Z)'
    pairs = []
    for m in re.finditer(pattern, t, flags=re.DOTALL):
        key = (m.group(1) or "").strip()
        val = (m.group(2) or "").strip()
        if not key:
            continue
        # Limpieza mínima del valor: no alterar URLs, solo compactar espacios externos
        val = re.sub(r"[ \t]+", " ", val).strip()
        pairs.append((key, val))
    return pairs

def universal_parser(raw_text: str):
    """
    Regla global: cada ':' indica clave/valor.
    Devuelve:
      - dict si hay un solo registro
      - list[dict] si hay múltiples registros (ej: RQH con 2 resultados)
    Detección robusta de múltiples registros:
      - Si se repite una clave "pivot" típica (DNI, Nro, CLAVE, etc.) se parte en bloques.
      - Si no, se devuelve un dict simple con todos los campos.
    """
    if not raw_text or not raw_text.strip():
        return {}

    text = raw_text.replace("\r\n", "\n").replace("\r", "\n").strip()

    # 1) Extraer todos los pares en orden
    pairs = _extract_pairs_anywhere(text)
    if not pairs:
        return {}

    # 2) Definir claves pivote que suelen marcar inicio de un nuevo registro
    pivot_keys = {
        "DNI", "RUC", "CE", "CI", "PASAPORTE",
        "Nro", "NRO", "N°", "CLAVE",
        "FECHA REGISTRO", "FECHA HORA REGISTRO"
    }

    def is_pivot(k: str) -> bool:
        k_norm = re.sub(r"\s+", " ", k).strip().upper()
        return k_norm in {pk.upper() for pk in pivot_keys}

    # 3) Construir registros: si aparece una clave pivote repetida, iniciamos nuevo objeto
    records = []
    current = {}
    pivot_seen_once = False

    for k, v in pairs:
        k_clean = re.sub(r"\s+", " ", k).strip()  # mantener clave "tal cual" pero sin dobles espacios

        if is_pivot(k_clean):
            # Si ya vimos pivote y current tiene data => nuevo registro
            if pivot_seen_once and current:
                records.append(current)
                current = {}
            pivot_seen_once = True

        # Si la clave se repite dentro del mismo registro, convertir a lista
        if k_clean in current:
            if isinstance(current[k_clean], list):
                current[k_clean].append(v)
            else:
                current[k_clean] = [current[k_clean], v]
        else:
            current[k_clean] = v

    if current:
        records.append(current)

    # Si se armó más de un registro => lista
    if len(records) > 1:
        return records

    # Si solo uno => dict
    return records[0]

# -------------------------------------------------------------------
# Limpieza y extracción (NO ROMPER FORMATO NECESARIO PARA PARSEAR)
# -------------------------------------------------------------------
def clean_and_extract(raw_text: str):
    """
    Mantiene misma intención de tu limpieza, pero SIN destruir saltos de línea.
    Esto es clave para que el parser detecte mejor estructuras.
    """
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

    # IMPORTANTE: NO colapsar \n. Solo compactar espacios y tabs.
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()

    fields = {}

    photo_type_match = re.search(r"Foto\s*:\s*(rostro|huella|firma|adverso|reverso).*", text, re.IGNORECASE)
    if photo_type_match:
        fields["photo_type"] = photo_type_match.group(1).lower()

    not_found_pattern = r"\[⚠️\]\s*(no se encontro información|no se han encontrado resultados|no se encontró una|no hay resultados|no tenemos datos|no se encontraron registros)"
    if re.search(not_found_pattern, text, re.IGNORECASE | re.DOTALL):
        fields["not_found"] = True

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

        primary_blocked = is_bot_blocked(LEDERDATA_PRIMARY_BOT_ID)

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

                if "[⛔] ANTI-SPAM" in raw_text and "INTENTA DESPUES" in raw_text:
                    anti_spam_detected[0] = True
                    stop_collecting.set()
                    return

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

        full_command = f"{command} {param}" if param else command
        await client.send_message(bot_to_use, full_command)

        start_time = time.time()

        while (time.time() - start_time) < timeout_val:
            if stop_collecting.is_set():
                break

            if all_received_messages and (time.time() - last_message_time[0]) > 4.5:
                break

            await asyncio.sleep(0.5)

        client.remove_event_handler(temp_handler)

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

    # Combinar texto
    combined_text = ""
    for msg in all_received_messages:
        if msg.get("message"):
            combined_text += msg.get("message", "") + "\n\n"
    combined_text = combined_text.strip()

    parsed = universal_parser(combined_text)

    urls = []
    for msg in all_received_messages:
        urls.extend(msg.get("urls", []))

    # fields auxiliares (photo_type, etc.) se mantienen
    final_fields = {}
    for msg in all_received_messages:
        for k, v in msg.get("fields", {}).items():
            if v and not final_fields.get(k):
                final_fields[k] = v

    # Construir data "limpio"
    data = {}

    # Si parsed es lista => múltiples registros => denuncias: [...]
    if isinstance(parsed, list):
        data["denuncias"] = parsed
        if final_fields:
            data.update(final_fields)
    elif isinstance(parsed, dict) and parsed:
        # Un solo registro
        data.update(final_fields)
        data.update(parsed)
    else:
        data.update(final_fields)

    if urls:
        data["urls"] = urls

    return {
        "status": "success",
        "data": data,
        "raw_message": combined_text
    }

def run_telegram_command_with_cache(command: str, param: str, endpoint_path: str = None):
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
    return dni.isdigit() and len(dni) == 8

def validate_ruc(ruc: str) -> bool:
    return ruc.isdigit() and len(ruc) == 11

def validate_ce(ce: str) -> bool:
    return 6 <= len(ce) <= 12

def validate_pasaporte(pasaporte: str) -> bool:
    return 6 <= len(pasaporte) <= 12

def validate_ci(ci: str) -> bool:
    return 6 <= len(ci) <= 12

def validate_placa(placa: str) -> bool:
    return 5 <= len(placa) <= 7

def validate_serie_armamento(serie: str) -> bool:
    return 5 <= len(serie) <= 13

def validate_clave_denuncia(clave: str) -> bool:
    return 5 <= len(clave) <= 11

def validate_nombres(nombres: str) -> bool:
    parts = nombres.split("|")
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

# --- Endpoints ---
@app.route("/rqh", methods=["GET"])
def rqh_endpoint():
    dni = request.args.get("dni")
    if not dni:
        return jsonify({"status": "error", "message": "Parámetro 'dni' requerido"}), 400
    if not validate_dni(dni):
        return jsonify({"status": "error", "message": "DNI inválido. Debe tener 8 dígitos numéricos."}), 400
    result = run_telegram_command_with_cache("/rqh", dni, "/rqh")
    return jsonify(result)

@app.route("/dend", methods=["GET"])
def dend_endpoint():
    dni = request.args.get("dni")
    if not dni:
        return jsonify({"status": "error", "message": "Parámetro 'dni' requerido"}), 400
    if not validate_dni(dni):
        return jsonify({"status": "error", "message": "DNI inválido. Debe tener 8 dígitos numéricos."}), 400
    result = run_telegram_command_with_cache("/dend", dni, "/dend")
    return jsonify(result)

@app.route("/dence", methods=["GET"])
def dence_endpoint():
    ce = request.args.get("ce")
    if not ce:
        return jsonify({"status": "error", "message": "Parámetro 'ce' requerido"}), 400
    if not validate_ce(ce):
        return jsonify({"status": "error", "message": "Carnet de extranjería inválido. Debe tener entre 6 y 12 caracteres."}), 400
    result = run_telegram_command_with_cache("/dence", ce, "/dence")
    return jsonify(result)

@app.route("/denpas", methods=["GET"])
def denpas_endpoint():
    pasaporte = request.args.get("pasaporte")
    if not pasaporte:
        return jsonify({"status": "error", "message": "Parámetro 'pasaporte' requerido"}), 400
    if not validate_pasaporte(pasaporte):
        return jsonify({"status": "error", "message": "Pasaporte inválido. Debe tener entre 6 y 12 caracteres."}), 400
    result = run_telegram_command_with_cache("/denpas", pasaporte, "/denpas")
    return jsonify(result)

@app.route("/denci", methods=["GET"])
def denci_endpoint():
    ci = request.args.get("ci")
    if not ci:
        return jsonify({"status": "error", "message": "Parámetro 'ci' requerido"}), 400
    if not validate_ci(ci):
        return jsonify({"status": "error", "message": "Cédula de identidad inválida. Debe tener entre 6 y 12 caracteres."}), 400
    result = run_telegram_command_with_cache("/denci", ci, "/denci")
    return jsonify(result)

@app.route("/denp", methods=["GET"])
def denp_endpoint():
    placa = request.args.get("placa")
    if not placa:
        return jsonify({"status": "error", "message": "Parámetro 'placa' requerido"}), 400
    if not validate_placa(placa):
        return jsonify({"status": "error", "message": "Placa inválida. Debe tener entre 5 y 7 caracteres."}), 400
    result = run_telegram_command_with_cache("/denp", placa, "/denp")
    return jsonify(result)

@app.route("/denar", methods=["GET"])
def denar_endpoint():
    serie = request.args.get("serie")
    if not serie:
        return jsonify({"status": "error", "message": "Parámetro 'serie' requerido"}), 400
    if not validate_serie_armamento(serie):
        return jsonify({"status": "error", "message": "Serie de armamento inválida. Debe tener entre 5 y 13 caracteres."}), 400
    result = run_telegram_command_with_cache("/denar", serie, "/denar")
    return jsonify(result)

@app.route("/dencl", methods=["GET"])
def dencl_endpoint():
    clave = request.args.get("clave")
    if not clave:
        return jsonify({"status": "error", "message": "Parámetro 'clave' requerido"}), 400
    if not validate_clave_denuncia(clave):
        return jsonify({"status": "error", "message": "Clave de denuncia inválida. Debe tener entre 5 y 11 caracteres."}), 400
    result = run_telegram_command_with_cache("/dencl", clave, "/dencl")
    return jsonify(result)

@app.route("/fis", methods=["GET"])
def fis_endpoint():
    dni = request.args.get("dni")
    if not dni:
        return jsonify({"status": "error", "message": "Parámetro 'dni' requerido"}), 400
    if not validate_dni(dni):
        return jsonify({"status": "error", "message": "DNI inválido. Debe tener 8 dígitos numéricos."}), 400
    result = run_telegram_command_with_cache("/fis", dni, "/fis")
    return jsonify(result)

@app.route("/fisruc", methods=["GET"])
def fisruc_endpoint():
    ruc = request.args.get("ruc")
    if not ruc:
        return jsonify({"status": "error", "message": "Parámetro 'ruc' requerido"}), 400
    if not validate_ruc(ruc):
        return jsonify({"status": "error", "message": "RUC inválido. Debe tener 11 dígitos numéricos."}), 400
    result = run_telegram_command_with_cache("/fisruc", ruc, "/fisruc")
    return jsonify(result)

@app.route("/fisnm", methods=["GET"])
def fisnm_endpoint():
    nombres = request.args.get("nombres")
    paterno = request.args.get("paterno", "")
    materno = request.args.get("materno", "")

    if not nombres and not paterno and not materno:
        return jsonify({"status": "error", "message": "Se requiere al menos un parámetro: 'nombres', 'paterno' o 'materno'"}), 400

    param = f"{nombres or ''}|{paterno or ''}|{materno or ''}"
    result = run_telegram_command_with_cache("/nm", param, "/fisnm")
    return jsonify(result)

@app.route("/command", methods=["GET"])
def command_endpoint():
    cmd = request.args.get("cmd")
    param = request.args.get("param", "")
    if not cmd:
        return jsonify({"status": "error", "message": "Parámetro 'cmd' requerido"}), 400
    result = run_telegram_command_with_cache(cmd, param)
    return jsonify(result)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
