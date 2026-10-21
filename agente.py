#!/usr/bin/env python3
# =============================================================================
# CIT Mobility, S.A.P.I. de C.V.
# Edge Mobility. agente.py :: agente publicador unico hacia la VM (ADR-001)
#
#   Es el lado de a bordo de la interfaz definida en el ADR-001 ("interfaz de
#   datos entre Edge Mobility y las plataformas RuTi"). Atiende dos de los
#   tres planos; el tercero, el video, lo mueve el contador por SRT y aqui
#   solo se ordena.
#
#   PLANO DE TELEMETRIA (subida)
#       lector_gps.py ──► /dev/shm/cit/gps_actual.json ─┐
#       contador_p2p_v17.py ──► datos/eventos.jsonl ────┤ (cruces -> pax)
#       contador_p2p_v17.py ──► GET 127.0.0.1:8080/estado ┤ (camara, video)
#                                                       ▼
#                                  muestra de la tabla 3 del ADR, 1 Hz en
#                                  marcha y cada 10 s detenida
#                                                       │
#                                  datos/salida_vm.jsonl   (diario en disco)
#                                                       │  QoS 1, avanza solo
#                                                       ▼  con el PUBACK
#                                  cit/v1/<device_id>/telemetria
#
#   PLANO DE CONTROL (bajada)
#       cit/v1/<device_id>/cmd  ──►  validar  ──►  marcar consumido (disco)
#                                                   ──►  ejecutar  ──►
#       cit/v1/<device_id>/resultado  (por el mismo diario, no se pierde)
#
#   Lista cerrada de comandos. Cualquier otro se rechaza y queda anotado:
#       media.open           abre una sesion de video (el contador la corta)
#       media.close          la cierra antes de tiempo
#       app.restart          reinicia los servicios (cit.sh reiniciar)
#       module.restart       reinicio ordenado del sistema operativo
#       module.shutdown      apagado ordenado del sistema operativo (NO vuelve
#                            a encender solo: hace falta cortar y devolver la
#                            energia, o la ignicion cuando exista R08)
#       diagnostics.request  devuelve una foto del estado del equipo
#   El corte de energia NO es un comando: es atribucion del watchdog (R25).
#
# REGLAS QUE ESTE PROGRAMA HACE CUMPLIR (tabla 1 del ADR)
#   - Nada se pierde sin red: primero se escribe en disco y solo se avanza
#     cuando el broker confirma. Cada muestra lleva sample_id para que la VM
#     descarte duplicados; buffered=true si salio con retraso.
#   - Un comando se marca como consumido ANTES de ejecutarlo. Si se marcara
#     despues, un reinicio dejaria la orden viva y el equipo entraria en bucle.
#   - Comando repetido (mismo command_id): no se ejecuta otra vez; se vuelve a
#     informar el resultado que ya tuvo.
#   - Comando caducado (ts_issued + ttl_s ya paso): se descarta aunque llegue.
#   - Como mucho MAX_REINICIOS_HORA reinicios o apagados (app o modulo) por hora
#     (0 = sin limite; solo para pruebas, el ADR pide 3).
#   - Otro device_id, un tipo fuera de la lista o un campo obligatorio vacio:
#     se rechaza.
#
# QUE NO HACE
#   - No toca AWS. El envio a AWS IoT Core sigue siendo reenviador.py y se
#     conserva de reserva (ENVIAR_AWS=1 en cit.sh).
#   - No decide nada del video: duracion, cuota mensual y cierre los impone el
#     contador, que es quien gasta los datos.
#
# Uso (normalmente lo lanza cit.sh, ventana "vm"):
#   CIT_UNIDAD=cit-001 CIT_VEHICULO=A-01 CIT_BROKER=<vm> CIT_PUERTO=8883 \
#   CIT_TLS=1 CIT_USUARIO=cit-001 CIT_CONTRASENA=... CIT_CLAVE_LOCAL=... \
#   python3 agente.py
#
#   Prueba en banco, broker local sin TLS:
#   CIT_BROKER=127.0.0.1 CIT_PUERTO=1883 CIT_TLS=0 python3 agente.py
#
# Variables de entorno:
#   CIT_UNIDAD=cit-001        device_id: el MODULO, no el camion
#   CIT_VEHICULO=             vehicle_id: unidad donde esta instalado
#   CIT_BROKER, CIT_PUERTO    broker MQTT de la VM
#   CIT_TLS=1                 el ADR exige TLS; 0 solo para banco
#   CIT_CA, CIT_CERT, CIT_LLAVE   CA propia y, si la VM usa TLS mutuo, el
#                             certificado del equipo. CA vacia = las del sistema
#   CIT_USUARIO, CIT_CONTRASENA   credencial propia del equipo en el broker
#   CIT_CONTADOR=http://127.0.0.1:8080   control local del contador
#   CIT_CLAVE_LOCAL           clave del control local (la genera cit.sh)
#   CIT_GPS_ACTUAL=/dev/shm/cit/gps_actual.json
#   CIT_EVENTOS=datos/eventos.jsonl
#   CIT_VEL_MARCHA=3          km/h a partir de los cuales la unidad va en marcha
#   CIT_PERIODO_MARCHA=1      segundos entre muestras en marcha (1 Hz)
#   CIT_PERIODO_DETENIDA=10   segundos entre muestras detenida (0.1 Hz)
#   CIT_MAX_REINICIOS_HORA=3     0 = sin limite (pruebas)
#   CIT_CMD_REINICIO_APP      por omision: <este directorio>/cit.sh reiniciar
#   CIT_CMD_REINICIO_SO       por omision: sudo -n /usr/bin/systemctl reboot
#   CIT_CMD_APAGADO_SO        por omision: sudo -n /usr/bin/systemctl poweroff
#
# Requisito: pip3 install paho-mqtt   (API v2)
# =============================================================================

import json
import os
import queue
import re
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.exit("Falta paho-mqtt. Instalalo con: pip3 install paho-mqtt")

VERSION = "1.0"
ESQUEMA = 1                       # schema_version de la telemetria y del resultado

# --- Configuracion -----------------------------------------------------------
AQUI = os.path.dirname(os.path.abspath(__file__))
DATOS = os.environ.get("CIT_DATOS", os.path.join(AQUI, "datos"))

DEVICE_ID = os.environ.get("CIT_UNIDAD", "cit-001")
VEHICULO_ID = os.environ.get("CIT_VEHICULO", "") or "sin-asignar"
BROKER = os.environ.get("CIT_BROKER", "127.0.0.1")
PUERTO = int(os.environ.get("CIT_PUERTO", "8883"))
USAR_TLS = os.environ.get("CIT_TLS", "1") == "1"
CA = os.environ.get("CIT_CA", "")
CERT = os.environ.get("CIT_CERT", "")
LLAVE = os.environ.get("CIT_LLAVE", "")
USUARIO = os.environ.get("CIT_USUARIO", "")
CONTRASENA = os.environ.get("CIT_CONTRASENA", "")

CONTADOR_URL = os.environ.get("CIT_CONTADOR", "http://127.0.0.1:8080").rstrip("/")
CLAVE_LOCAL = os.environ.get("CIT_CLAVE_LOCAL", "")
GPS_ACTUAL = os.environ.get("CIT_GPS_ACTUAL", "/dev/shm/cit/gps_actual.json")
EVENTOS = os.environ.get("CIT_EVENTOS", os.path.join(DATOS, "eventos.jsonl"))

VEL_MARCHA = float(os.environ.get("CIT_VEL_MARCHA", "3"))
PERIODO_MARCHA = float(os.environ.get("CIT_PERIODO_MARCHA", "1"))
PERIODO_DETENIDA = float(os.environ.get("CIT_PERIODO_DETENIDA", "10"))
MAX_REINICIOS_HORA = int(os.environ.get("CIT_MAX_REINICIOS_HORA", "3"))
CMD_REINICIO_APP = os.environ.get(
    "CIT_CMD_REINICIO_APP", "{} reiniciar".format(shlex.quote(os.path.join(AQUI, "cit.sh"))))
CMD_REINICIO_SO = os.environ.get("CIT_CMD_REINICIO_SO", "sudo -n /usr/bin/systemctl reboot")
CMD_APAGADO_SO = os.environ.get("CIT_CMD_APAGADO_SO", "sudo -n /usr/bin/systemctl poweroff")

# Archivos propios, todos en datos/ (fuera de git)
DIARIO = os.path.join(DATOS, "salida_vm.jsonl")        # lo que falta por subir
POS_DIARIO = os.path.join(DATOS, "salida_vm.pos")      # hasta donde confirmo la VM
ESTADO = os.path.join(DATOS, "agente_estado.json")     # pax, ordenes vistas...
BITACORA = os.path.join(DATOS, "ordenes.jsonl")        # historial local de comandos

PREFIJO = "cit/v1"
TEMA_CMD = "{}/{}/cmd".format(PREFIJO, DEVICE_ID)
TEMAS_SALIDA = ("telemetria", "resultado")

TIPOS = ("media.open", "media.close", "app.restart", "module.restart",
         "module.shutdown", "diagnostics.request")
# Comandos que matan al propio agente: su resultado final se cierra al volver.
REINICIOS = ("app.restart", "module.restart", "module.shutdown")
OBLIGATORIOS = ("command_id", "device_id", "type", "issued_by", "ts_issued",
                "ttl_s", "reason")

INTERVALO = 0.2            # vuelta del ciclo principal
LOTE = 50                  # muestras publicadas antes de esperar confirmaciones
ESPERA_PUBACK = 15.0
DIFERIDA_S = 5.0           # una muestra que sale con mas retraso va buffered=true
GPS_VIEJO_S = 5.0          # gps_actual.json mas viejo que esto = sin posicion
MARCHA_RETIENE_S = 5.0     # tras bajar de VEL_MARCHA se sigue a 1 Hz este tiempo
ROTAR_BYTES = 20 * 1024 * 1024
MAX_VISTAS = 500           # command_id recordados para no repetir

# Calidad del GGA -> gnss_fix. Propuesta de enumerado para el contrato v1.
CALIDADES = {0: "none", 1: "gps", 2: "dgps", 3: "pps", 4: "rtk_fixed",
             5: "rtk_float", 6: "estimated", 7: "manual", 8: "simulated"}

_parar = threading.Event()


# --- Utilidades --------------------------------------------------------------
def ahora_iso(ms=True):
    return datetime.now(timezone.utc).isoformat(
        timespec="milliseconds" if ms else "seconds").replace("+00:00", "Z")


def leer_iso(texto):
    """'2026-09-24T20:00:00Z' (con o sin fraccion) -> epoch. ValueError si no."""
    if not isinstance(texto, str) or not texto.endswith("Z"):
        raise ValueError("se espera UTC terminado en Z")
    d = datetime.fromisoformat(texto[:-1] + "+00:00")
    return d.timestamp()


def aviso(txt):
    print("[{}] {}".format(time.strftime("%H:%M:%S"), txt), flush=True)


def guardar_json(ruta, obj):
    """Escritura atomica con fsync: sobrevive a un corte de luz."""
    os.makedirs(os.path.dirname(ruta) or ".", exist_ok=True)
    tmp = ruta + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, ruta)


def cargar_json(ruta, defecto):
    try:
        with open(ruta, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else defecto
    except (OSError, ValueError):
        return defecto


def boot_id():
    """Cambia en cada arranque del sistema. Marca el inicio del servicio."""
    try:
        with open("/proc/sys/kernel/random/boot_id") as f:
            return f.read().strip()
    except OSError:
        return "desconocido"


def uptime_s():
    try:
        with open("/proc/uptime") as f:
            return int(float(f.read().split()[0]))
    except (OSError, ValueError, IndexError):
        return None


def temperaturas():
    """{zona: grados C} de las zonas termicas legibles."""
    salida = {}
    base = "/sys/class/thermal"
    try:
        zonas = sorted(z for z in os.listdir(base) if z.startswith("thermal_zone"))
    except OSError:
        return salida
    for z in zonas:
        try:
            with open(os.path.join(base, z, "type")) as f:
                nombre = f.read().strip()
            # En binario: las zonas cv*-thermal responden EAGAIN y la lectura
            # de texto devuelve None en vez de lanzar un OSError.
            with open(os.path.join(base, z, "temp"), "rb") as f:
                crudo = f.read()
            if not crudo:
                continue
            salida[nombre] = round(int(crudo.strip()) / 1000.0, 1)
        except (OSError, ValueError, TypeError):
            continue            # zonas que no se dejan leer; se omiten
    return salida


# --- Lectura de archivos que solo crecen -------------------------------------
class Lector:
    """Lee lineas completas de un archivo que crece, desde una posicion.

    Detecta si el archivo se roto (otro inodo) o se vacio (encogio) y vuelve a
    empezar. Una linea a medio escribir se deja para la siguiente vuelta.
    """

    def __init__(self, ruta, inodo=0, pos=0):
        self.ruta = ruta
        self.inodo = inodo
        self.pos = pos

    def revisar(self):
        try:
            st = os.stat(self.ruta)
        except FileNotFoundError:
            return None
        if self.inodo and st.st_ino != self.inodo:
            self.inodo, self.pos = st.st_ino, 0
        elif st.st_size < self.pos:
            self.pos = 0
        if not self.inodo:
            self.inodo = st.st_ino
        return st

    def pendientes(self, maximo=500):
        """[(posicion_tras_la_linea, dict_o_None)]"""
        if self.revisar() is None:
            return []
        salida = []
        try:
            with open(self.ruta, "rb") as f:
                f.seek(self.pos)
                for cruda in f:
                    if not cruda.endswith(b"\n"):
                        break
                    try:
                        d = json.loads(cruda.decode("utf-8", errors="ignore"))
                        d = d if isinstance(d, dict) else None
                    except ValueError:
                        d = None
                    salida.append((f.tell(), d))
                    if len(salida) >= maximo:
                        break
        except FileNotFoundError:
            return []
        return salida


# --- Diario de salida --------------------------------------------------------
class Diario:
    """Todo lo que sale hacia la VM pasa primero por aqui.

    Una linea por mensaje: {"e": epoch de creacion, "tema": ..., "m": {...}}.
    La posicion confirmada solo avanza con el PUBACK del broker, asi que un
    corte de red, de luz o un reinicio no pierden nada: al volver se sube lo
    pendiente, en orden.

    Lo escribe y lo lee este mismo proceso, asi que se puede rotar sin riesgo
    en cuanto todo lo escrito este confirmado.
    """

    def __init__(self):
        p = cargar_json(POS_DIARIO, {})
        self.lector = Lector(DIARIO, int(p.get("inodo", 0)), int(p.get("pos", 0)))
        self._lock = threading.Lock()

    def anotar(self, tema, mensaje):
        linea = json.dumps({"e": round(time.time(), 3), "tema": tema, "m": mensaje},
                           separators=(",", ":"), ensure_ascii=False)
        with self._lock:
            os.makedirs(DATOS, exist_ok=True)
            with open(DIARIO, "a", encoding="utf-8") as f:
                f.write(linea + "\n")
                f.flush()
                os.fsync(f.fileno())

    def confirmar(self, pos):
        with self._lock:
            self.lector.pos = pos
            guardar_json(POS_DIARIO, {"inodo": self.lector.inodo, "pos": pos})

    def pendiente_bytes(self):
        try:
            return max(0, os.path.getsize(DIARIO) - self.lector.pos)
        except OSError:
            return 0

    def rotar_si_toca(self):
        """Si todo esta confirmado y el archivo paso de ROTAR_BYTES, empieza
        uno nuevo. Se conserva el anterior como .1 (el historial vive en la VM)."""
        with self._lock:
            try:
                tam = os.path.getsize(DIARIO)
            except OSError:
                return
            if tam < ROTAR_BYTES or self.lector.pos < tam:
                return
            os.replace(DIARIO, DIARIO + ".1")
            open(DIARIO, "a").close()
            self.lector.inodo = os.stat(DIARIO).st_ino
            self.lector.pos = 0
            guardar_json(POS_DIARIO, {"inodo": self.lector.inodo, "pos": 0})
        aviso("Diario rotado: {} MB ya confirmados pasan a {}.1".format(
            tam // (1024 * 1024), os.path.basename(DIARIO)))


# --- Contador (control local) ------------------------------------------------
def contador(metodo, ruta, cuerpo=None, espera=3.0):
    """Llama al control local del contador. Devuelve (dict, None) o (None, motivo)."""
    url = "{}{}?k={}".format(CONTADOR_URL, ruta, urllib.parse.quote(CLAVE_LOCAL))
    datos = None if cuerpo is None else json.dumps(cuerpo).encode("utf-8")
    if metodo == "POST" and datos is None:
        datos = b""
    pedido = urllib.request.Request(url, method=metodo, data=datos,
                                    headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(pedido, timeout=espera) as r:
            return json.loads(r.read().decode("utf-8")), None
    except urllib.error.HTTPError as e:
        return None, "el contador respondio {}".format(e.code)
    except Exception as e:
        return None, "el contador no responde: {}".format(e)


def leer_gps():
    """Ultima lectura de lector_gps.py, o None si no hay o esta vieja."""
    try:
        edad = time.time() - os.path.getmtime(GPS_ACTUAL)
        if edad > GPS_VIEJO_S:
            return None
        with open(GPS_ACTUAL, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else None
    except (OSError, ValueError):
        return None


# =============================================================================
class Agente:
    def __init__(self):
        self.diario = Diario()
        self.cli = None
        self.conectado = False
        self.enviados = 0
        self.ordenes = queue.Queue(maxsize=50)
        self._lock_estado = threading.Lock()
        self.boot = boot_id()

        e = cargar_json(ESTADO, {})
        self.estado = {
            "boot_id": e.get("boot_id"),
            "pax_in": int(e.get("pax_in", 0)),
            "pax_out": int(e.get("pax_out", 0)),
            "eventos": e.get("eventos"),          # {"inodo","pos"} en eventos.jsonl
            "vistas": e.get("vistas", {}),        # command_id -> resultado
            "reinicios": e.get("reinicios", []),  # epochs de reinicios ejecutados
            "last_command": e.get("last_command"),
        }

        # pax_in / pax_out son acumulados "desde el inicio del servicio"
        # (tabla 3). El servicio empieza cuando la ignicion enciende el
        # modulo, o sea en cada arranque del sistema: por eso se ponen a cero
        # cuando cambia el boot_id, y NO cuando solo se reinicia un programa.
        if self.estado["boot_id"] != self.boot:
            if self.estado["boot_id"] is not None:
                aviso("Arranque nuevo del modulo: pax_in y pax_out vuelven a 0.")
            self.estado.update(boot_id=self.boot, pax_in=0, pax_out=0)

        ev = self.estado["eventos"]
        if ev:
            self.eventos = Lector(EVENTOS, int(ev.get("inodo", 0)), int(ev.get("pos", 0)))
        else:
            # Primera vez: se empieza por el final. Los cruces viejos del
            # historial no son de este servicio.
            self.eventos = Lector(EVENTOS)
            st = self.eventos.revisar()
            self.eventos.pos = st.st_size if st else 0
        self._guardar_estado()
        self._cerrar_reinicio_pendiente()

        # Lo ultimo que se mando, para publicar al instante si algo cambia
        self.ultima_muestra = 0.0
        self.ultima_firma = None
        self.pax_pendiente = False
        self.ultimo_en_marcha = 0.0
        self.cont = None
        self.gps = None
        self.ultimo_sondeo = 0.0

    # ------------------------------------------------------------------ estado
    def _guardar_estado(self):
        with self._lock_estado:
            self.estado["eventos"] = {"inodo": self.eventos.inodo, "pos": self.eventos.pos} \
                if hasattr(self, "eventos") else self.estado.get("eventos")
            vistas = self.estado["vistas"]
            if len(vistas) > MAX_VISTAS:
                for cid in sorted(vistas, key=lambda c: vistas[c].get("_t", 0))[:len(vistas) - MAX_VISTAS]:
                    del vistas[cid]
            hace_hora = time.time() - 3600
            self.estado["reinicios"] = [t for t in self.estado["reinicios"] if t > hace_hora]
            guardar_json(ESTADO, self.estado)

    def _cerrar_reinicio_pendiente(self):
        """Si el ultimo comando fue un reinicio y seguimos aqui, informar como
        termino. Es el "estado reportado" que cierra el ciclo del comando."""
        lc = self.estado.get("last_command")
        if not lc or lc.get("status") != "en_curso" or lc.get("type") not in REINICIOS:
            return
        if lc["type"] == "module.restart" and lc.get("boot_id") == self.boot:
            lc.update(status="fallida", detail="el sistema no se reinicio",
                      ts_applied=ahora_iso())
        elif lc["type"] == "module.shutdown" and lc.get("boot_id") == self.boot:
            lc.update(status="fallida", detail="el sistema no se apago",
                      ts_applied=ahora_iso())
        elif lc["type"] == "module.shutdown":
            lc.update(status="aplicada", detail="el equipo se apago y volvio a encender",
                      ts_applied=ahora_iso())
        else:
            lc.update(status="aplicada", detail="el equipo volvio en linea",
                      ts_applied=ahora_iso())
        self._registrar_resultado(lc)

    # ------------------------------------------------------------------- mqtt
    def _crear_cliente(self):
        # Sesion persistente (clean_session=False): si la VM manda un comando
        # mientras la unidad no tiene senal, el broker lo guarda y lo entrega
        # al volver. Que siga vigente o no lo decide su ttl_s, aqui.
        cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=DEVICE_ID,
                          clean_session=False, protocol=mqtt.MQTTv311)
        if USAR_TLS:
            cli.tls_set(ca_certs=CA or None, certfile=CERT or None, keyfile=LLAVE or None)
        if USUARIO:
            cli.username_pw_set(USUARIO, CONTRASENA or None)
        cli.will_set("{}/{}/estado".format(PREFIJO, DEVICE_ID),
                     self._mensaje_estado("desconectada"), qos=1, retain=True)
        # Tope de 20 s entre reintentos: al volver la cobertura, la telemetria
        # debe reanudarse pronto (tabla 1: retraso de posicion <= 3 s).
        cli.reconnect_delay_set(min_delay=2, max_delay=20)
        cli.on_connect = self._al_conectar
        cli.on_disconnect = self._al_desconectar
        cli.on_message = self._al_recibir
        return cli

    def _mensaje_estado(self, estado):
        return json.dumps({"schema_version": ESQUEMA, "device_id": DEVICE_ID,
                           "vehicle_id": VEHICULO_ID, "status": estado,
                           "ts_device": ahora_iso(), "agent_version": VERSION})

    def _al_conectar(self, cli, userdata, flags, rc, props=None):
        if rc.is_failure:
            aviso("El broker rechazo la conexion: {}".format(rc))
            return
        self.conectado = True
        aviso("Conectado a {}:{} como {} (sesion {})".format(
            BROKER, PUERTO, DEVICE_ID,
            "retomada" if flags.session_present else "nueva"))
        cli.publish("{}/{}/estado".format(PREFIJO, DEVICE_ID),
                    self._mensaje_estado("en linea"), qos=1, retain=True)
        cli.subscribe(TEMA_CMD, qos=1)

    def _al_desconectar(self, cli, userdata, flags, rc, props=None):
        if self.conectado:
            aviso("Desconectado del broker ({}). Se sigue guardando en disco.".format(rc))
        self.conectado = False

    def _al_recibir(self, cli, userdata, msg):
        """Hilo de red de paho: solo se encola."""
        # Nunca un retenido: un comando retenido se repetiria en cada conexion.
        if msg.retain or msg.topic != TEMA_CMD:
            return
        try:
            self.ordenes.put_nowait(msg.payload)
        except queue.Full:
            aviso("Cola de comandos llena; se descarta uno")

    # --------------------------------------------------------------- subida
    def bombear(self):
        """Publica lo pendiente del diario, en lotes, y confirma lo recibido."""
        if not self.conectado:
            return
        pendientes = self.diario.lector.pendientes(LOTE)
        if not pendientes:
            return
        enviados = []
        for pos, entrada in pendientes:
            if entrada is None or entrada.get("tema") not in TEMAS_SALIDA:
                enviados.append((pos, None))       # linea corrupta: se salta
                continue
            m = dict(entrada.get("m") or {})
            if entrada["tema"] == "telemetria":
                m["buffered"] = (time.time() - float(entrada.get("e", 0))) > DIFERIDA_S
            datos = json.dumps(m, separators=(",", ":"), ensure_ascii=False)
            try:
                info = self.cli.publish("{}/{}/{}".format(PREFIJO, DEVICE_ID, entrada["tema"]),
                                        datos, qos=1)
            except Exception as e:
                aviso("Error al publicar: {}".format(e))
                break
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                break
            enviados.append((pos, info))

        avance = None
        for pos, info in enviados:
            if info is not None:
                try:
                    info.wait_for_publish(timeout=ESPERA_PUBACK)
                except Exception:
                    break
                if not info.is_published():
                    break
                self.enviados += 1
            avance = pos            # solo lo contiguo y confirmado
        if avance is not None:
            self.diario.confirmar(avance)

    # ------------------------------------------------------------- telemetria
    def leer_cruces(self):
        """Suma los cruces nuevos del bus a pax_in / pax_out."""
        cambio = False
        ultima = None
        for pos, ev in self.eventos.pendientes(2000):
            ultima = pos
            if ev and ev.get("tipo") == "cruce":
                if ev.get("direccion") == "entrada":
                    self.estado["pax_in"] += 1
                    cambio = True
                elif ev.get("direccion") == "salida":
                    self.estado["pax_out"] += 1
                    cambio = True
        if ultima is not None:
            self.eventos.pos = ultima
            self._guardar_estado()
        return cambio

    def sondear(self):
        """Una vez por segundo: estado del contador y ultima posicion."""
        self.cont, _ = contador("GET", "/estado", espera=0.8)
        self.gps = leer_gps()

    def armar_muestra(self):
        """Mensaje de la tabla 3 del ADR-001, version 1 del contrato."""
        g = self.gps or {}
        fix = bool(g.get("fix")) and "lat" in g
        cont = self.cont or {}
        video = cont.get("video") or {}
        camara = cont.get("camara") or {}
        pax_in, pax_out = self.estado["pax_in"], self.estado["pax_out"]
        try:
            libre = round(shutil.disk_usage(DATOS).free / 1e9, 1)
        except OSError:
            libre = None
        temps = temperaturas()

        sesion = {}
        if video.get("abierta"):
            sesion = {"camera": video.get("camara"),
                      "requested_by": video.get("solicitante"),
                      "command_id": video.get("orden"),
                      "remaining_s": video.get("restan_s")}

        return {
            "schema_version": ESQUEMA,
            "device_id": DEVICE_ID,
            "vehicle_id": VEHICULO_ID,
            "sample_id": uuid.uuid4().hex,
            "ts_device": ahora_iso(),
            # Tal cual las entrega el receptor, sin redondeo adicional.
            "lat": g.get("lat") if fix else None,
            "lon": g.get("lon") if fix else None,
            # Velocidad del receptor GNSS (RMC, efecto Doppler). NO se deriva
            # de las coordenadas y la plataforma no la recalcula (ADR 1.6).
            "speed_kph": g.get("vel") if fix else None,
            "heading_deg": g.get("rumbo") if fix else None,
            "gnss_fix": CALIDADES.get(g.get("calidad"), "gps") if fix else "none",
            "pax_in": pax_in,
            "pax_out": pax_out,
            "pax_onboard": max(0, pax_in - pax_out),
            # Sin senal de ignicion en el hardware actual (R08 de la carrier
            # board pendiente). null = no se sabe; nunca se inventa.
            "ignition": None,
            "health": {
                "temp_c": max(temps.values()) if temps else None,
                "disk_free_gb": libre,
                "cameras_active": 1 if camara.get("activa") else 0,
                "uptime_s": uptime_s(),
                # R32 propuesto: la carrier board aun no existe.
                "watchdog_resets": None,
                "counter_alive": bool(self.cont),
                "gps_alive": self.gps is not None,
            },
            "last_command": self._last_command_publico(),
            "media_session": sesion,
        }

    def _last_command_publico(self):
        lc = self.estado.get("last_command")
        if not lc:
            return None
        return {k: lc.get(k) for k in ("command_id", "type", "status", "ts_applied")}

    def tocar_muestra(self, cambio_pax):
        """Decide si toca muestra: por periodo, o al instante si algo cambio."""
        ahora = time.time()
        g = self.gps or {}
        vel = g.get("vel") if g.get("fix") else None
        if vel is not None and vel >= VEL_MARCHA:
            self.ultimo_en_marcha = ahora
        periodo = PERIODO_MARCHA if ahora - self.ultimo_en_marcha < MARCHA_RETIENE_S \
            else PERIODO_DETENIDA

        video = (self.cont or {}).get("video") or {}
        lc = self.estado.get("last_command") or {}
        firma = (bool(g.get("fix")), bool(video.get("abierta")), bool(self.cont),
                 lc.get("command_id"), lc.get("status"))
        # El cambio de pax queda pendiente hasta que salga una muestra: si
        # llego a menos de 1 s de la anterior, no debe esperar al periodo.
        self.pax_pendiente = self.pax_pendiente or cambio_pax
        cambio = self.pax_pendiente or firma != self.ultima_firma

        if ahora - self.ultima_muestra >= periodo or (cambio and ahora - self.ultima_muestra >= 1.0):
            self.ultima_firma = firma
            self.ultima_muestra = ahora
            self.pax_pendiente = False
            return True
        return False

    # --------------------------------------------------------------- comandos
    def _registrar_resultado(self, r):
        """Guarda en estado + bitacora local + diario hacia la VM."""
        publico = {k: v for k, v in r.items() if not k.startswith("_") and k != "boot_id"}
        publico.setdefault("schema_version", ESQUEMA)
        publico.setdefault("device_id", DEVICE_ID)
        with self._lock_estado:
            self.estado["last_command"] = r
            if r.get("command_id"):
                previo = self.estado["vistas"].get(r["command_id"], {})
                self.estado["vistas"][r["command_id"]] = dict(publico, _t=previo.get("_t", time.time()))
        self._guardar_estado()
        try:
            with open(BITACORA, "a", encoding="utf-8") as f:
                f.write(json.dumps(dict(publico, ts_log=ahora_iso()), ensure_ascii=False) + "\n")
        except OSError:
            pass
        self.diario.anotar("resultado", publico)
        aviso("COMANDO {} {} -> {}{}".format(
            publico.get("type"), publico.get("command_id"), publico.get("status"),
            "  ({})".format(publico["detail"]) if publico.get("detail") else ""))

    def validar(self, c):
        """Devuelve (status, detalle) si se rechaza, o None si es ejecutable."""
        faltan = [k for k in OBLIGATORIOS if c.get(k) in (None, "")]
        if faltan:
            return "rechazada", "faltan campos obligatorios: {}".format(", ".join(faltan))
        if c["device_id"] != DEVICE_ID:
            return "rechazada", "device_id no corresponde a este modulo"
        if c["type"] not in TIPOS:
            return "rechazada", "tipo fuera de la lista cerrada: {}".format(c["type"])
        try:
            emitida = leer_iso(c["ts_issued"])
            ttl = float(c["ttl_s"])
        except (ValueError, TypeError) as e:
            return "rechazada", "ts_issued o ttl_s invalidos ({})".format(e)
        if ttl <= 0:
            return "rechazada", "ttl_s debe ser positivo"
        ahora = time.time()
        if emitida - ahora > 120:
            return "rechazada", "ts_issued en el futuro; revisar relojes"
        if ahora > emitida + ttl:
            return "caducada", "llego {:.0f} s despues de vencer".format(ahora - emitida - ttl)
        if c["type"] in REINICIOS and MAX_REINICIOS_HORA > 0:
            hace_hora = ahora - 3600
            recientes = [t for t in self.estado["reinicios"] if t > hace_hora]
            if len(recientes) >= MAX_REINICIOS_HORA:
                return "rechazada", "limite de {} reinicios por hora".format(MAX_REINICIOS_HORA)
        return None

    def atender(self, crudo):
        try:
            c = json.loads(crudo.decode("utf-8") if isinstance(crudo, bytes) else crudo)
            if not isinstance(c, dict):
                raise ValueError
        except (ValueError, UnicodeDecodeError):
            self._registrar_resultado({"command_id": None, "type": None,
                                       "status": "rechazada",
                                       "detail": "comando ilegible, se espera JSON",
                                       "ts_received": ahora_iso()})
            return

        base = {"command_id": c.get("command_id"), "type": c.get("type"),
                "issued_by": c.get("issued_by"), "ts_received": ahora_iso()}

        # Repetido: no se ejecuta otra vez. Se vuelve a informar lo que paso,
        # por si la VM no alcanzo a recibirlo.
        previo = self.estado["vistas"].get(c.get("command_id")) if c.get("command_id") else None
        if previo:
            aviso("COMANDO {} repetido; se reenvia su resultado ({})".format(
                c.get("command_id"), previo.get("status")))
            self.diario.anotar("resultado", dict(
                {k: v for k, v in previo.items() if not k.startswith("_")}, duplicate=True))
            return

        rechazo = self.validar(c)
        if rechazo:
            self._registrar_resultado(dict(base, status=rechazo[0], detail=rechazo[1]))
            return

        # Marcar consumido ANTES de ejecutar (nota de la tabla 4 del ADR).
        en_curso = dict(base, status="en_curso", boot_id=self.boot)
        if c["type"] in REINICIOS:
            with self._lock_estado:
                self.estado["reinicios"].append(time.time())
        self._registrar_resultado(en_curso)

        try:
            status, detalle, datos = self.ejecutar(c)
        except Exception as e:
            status, detalle, datos = "fallida", "error interno: {}".format(e), None
        if status is None:
            return          # reinicio lanzado: el resultado final llega al volver
        final = dict(base, status=status, detail=detalle, ts_applied=ahora_iso())
        if datos is not None:
            final["data"] = datos
        self._registrar_resultado(final)

    def ejecutar(self, c):
        """Devuelve (status, detalle, datos). status None = reinicio en marcha."""
        tipo = c["type"]
        params = c.get("params") if isinstance(c.get("params"), dict) else {}

        if tipo == "media.open":
            cuerpo = {"segundos": params.get("duration_s"), "camara": params.get("camera"),
                      "solicitante": c["issued_by"], "orden": c["command_id"]}
            r, error = contador("POST", "/video/abrir", cuerpo, espera=8)
            if r is None:
                return "fallida", error, None
            v = r.get("video") or {}
            if not r.get("ok"):
                return "rechazada", r.get("motivo"), {"sessions_left": v.get("sesiones_restantes")}
            return "aplicada", None, {"camera": v.get("camara"), "duration_s": r.get("segundos"),
                                      "stream": v.get("flujo"),
                                      "sessions_left": v.get("sesiones_restantes")}

        if tipo == "media.close":
            r, error = contador("POST", "/video/cerrar", espera=8)
            if r is None:
                return "fallida", error, None
            return "aplicada", None, None

        if tipo == "diagnostics.request":
            return "aplicada", None, self.diagnostico()

        if tipo == "app.restart":
            return self._reiniciar(CMD_REINICIO_APP, "servicios")

        if tipo == "module.restart":
            return self._reiniciar(CMD_REINICIO_SO, "sistema operativo")

        if tipo == "module.shutdown":
            return self._reiniciar(CMD_APAGADO_SO, "sistema operativo", accion="apagado")

        return "rechazada", "tipo sin implementar", None

    def _reiniciar(self, comando, que, accion="reinicio"):
        # El "en_curso" tiene que llegar a la VM antes de que el reinicio nos
        # mate: se espera a que el diario este confirmado (hasta 10 s).
        limite = time.time() + 10
        while time.time() < limite and self.diario.pendiente_bytes() > 0:
            time.sleep(0.2)
        entorno = dict(os.environ)
        entorno.pop("TMUX", None)        # cit.sh se niega a arrancar dentro de tmux
        aviso("{} ({}): {}".format(accion.capitalize(), que, comando))
        # La salida va a un archivo, no a una tuberia: este proceso va a
        # morir durante el reinicio y una tuberia llena bloquearia a cit.sh.
        ruta_log = os.path.join(DATOS, "reinicio.log")
        try:
            with open(ruta_log, "w") as log:
                proc = subprocess.Popen(shlex.split(comando), stdin=subprocess.DEVNULL,
                                        stdout=log, stderr=subprocess.STDOUT,
                                        start_new_session=True, env=entorno)
        except OSError as e:
            return "fallida", "no se pudo lanzar el {}: {}".format(accion, e), None
        # Si falla, suele ser al instante (p.ej. sudo sin permiso). Si no ha
        # fallado en 5 s, se da por lanzado y el resultado se cierra al volver.
        try:
            codigo = proc.wait(timeout=5)
            if codigo != 0:
                try:
                    with open(ruta_log, encoding="utf-8", errors="ignore") as f:
                        salida = f.read().strip()
                except OSError:
                    salida = ""
                return "fallida", "el {} fallo (codigo {}): {}".format(
                    accion, codigo, salida[-200:]), None
        except subprocess.TimeoutExpired:
            pass
        # Si en 120 s este proceso sigue vivo, el reinicio (o apagado) no ocurrio.
        threading.Timer(120, self._reinicio_no_ocurrio).start()
        return None, None, None

    def _reinicio_no_ocurrio(self):
        lc = self.estado.get("last_command") or {}
        if lc.get("status") == "en_curso" and lc.get("type") in REINICIOS:
            que = "apago" if lc.get("type") == "module.shutdown" else "reinicio"
            self._registrar_resultado(dict(lc, status="fallida", ts_applied=ahora_iso(),
                                           detail="pasaron 120 s y el equipo no se " + que))

    def _hilo_ordenes(self):
        while not _parar.is_set():
            try:
                crudo = self.ordenes.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self.atender(crudo)
            except Exception as e:
                aviso("Error atendiendo un comando: {}".format(e))

    def diagnostico(self):
        procesos = {}
        for nombre, patron in (("contador", r"python3 (.*/)?contador_p2p"),
                               ("gps", r"python3 (.*/)?lector_gps\.py"),
                               ("agente", r"python3 (.*/)?agente\.py"),
                               ("envio_srt", r"gst-launch-1\.0 udpsrc .*srtsink"),
                               ("aws", r"python3 (.*/)?reenviador\.py")):
            try:
                r = subprocess.run(["pgrep", "-f", "^" + patron], capture_output=True,
                                   text=True, timeout=3)
                procesos[nombre] = [int(p) for p in r.stdout.split()]
            except Exception:
                procesos[nombre] = None
        srt = []
        try:
            with open(os.path.join(DATOS, "emisor_srt.log"), encoding="utf-8",
                      errors="ignore") as f:
                srt = [l.rstrip() for l in f.readlines()[-5:]]
        except OSError:
            pass
        cont = dict(self.cont or {})
        cont.pop("gps", None)
        return {
            "agent_version": VERSION,
            "boot_id": self.boot,
            "uptime_s": uptime_s(),
            "procesos": procesos,
            "contador": cont or None,
            "gps": self.gps,
            "temperaturas": temperaturas(),
            "disco_libre_gb": round(shutil.disk_usage(DATOS).free / 1e9, 1),
            "pendiente_por_subir_bytes": self.diario.pendiente_bytes(),
            "reinicios_ultima_hora": len([t for t in self.estado["reinicios"]
                                          if t > time.time() - 3600]),
            "srt_log": srt,
        }

    # ------------------------------------------------------------------ ciclo
    def correr(self):
        threading.Thread(target=self._hilo_ordenes, daemon=True).start()
        self.cli = self._crear_cliente()
        self.cli.connect_async(BROKER, PUERTO, keepalive=60)
        self.cli.loop_start()

        ultimo_resumen = time.time()
        while not _parar.is_set():
            try:
                cambio_pax = self.leer_cruces()
                if time.time() - self.ultimo_sondeo >= 1.0:
                    self.ultimo_sondeo = time.time()
                    self.sondear()
                if self.tocar_muestra(cambio_pax):
                    self.diario.anotar("telemetria", self.armar_muestra())
                self.bombear()
                self.diario.rotar_si_toca()
            except Exception as e:
                aviso("Error en la vuelta: {}".format(e))

            if time.time() - ultimo_resumen >= 60:
                ultimo_resumen = time.time()
                aviso("{}  enviados={}  pendiente={} B  pax {}/{}  gps {}  contador {}".format(
                    "en linea" if self.conectado else "SIN RED", self.enviados,
                    self.diario.pendiente_bytes(), self.estado["pax_in"],
                    self.estado["pax_out"], "si" if self.gps else "no",
                    "si" if self.cont else "no"))
            _parar.wait(INTERVALO)

        aviso("Deteniendo")
        try:
            if self.conectado:
                self.cli.publish("{}/{}/estado".format(PREFIJO, DEVICE_ID),
                                 self._mensaje_estado("desconectada"),
                                 qos=1, retain=True).wait_for_publish(timeout=3)
            self.cli.disconnect()
            self.cli.loop_stop()
        except Exception:
            pass
        aviso("Total enviados: {}  pendiente en disco: {} B".format(
            self.enviados, self.diario.pendiente_bytes()))


def main():
    signal.signal(signal.SIGINT, lambda *_: _parar.set())
    signal.signal(signal.SIGTERM, lambda *_: _parar.set())

    print("=" * 70)
    print(" CIT Mobility :: agente publicador (ADR-001)  v{}".format(VERSION))
    print(" device_id  : {}      vehicle_id: {}".format(DEVICE_ID, VEHICULO_ID))
    print(" Broker     : {}:{}  TLS: {}  usuario: {}".format(
        BROKER, PUERTO, "si" if USAR_TLS else "NO", USUARIO or "(ninguno)"))
    print(" Sube       : {p}/{d}/telemetria  {p}/{d}/resultado  {p}/{d}/estado".format(
        p=PREFIJO, d=DEVICE_ID))
    print(" Escucha    : {}".format(TEMA_CMD))
    print(" Ritmo      : cada {:g} s en marcha (>= {:g} km/h), cada {:g} s detenida".format(
        PERIODO_MARCHA, VEL_MARCHA, PERIODO_DETENIDA))
    print(" Diario     : {}".format(DIARIO))
    print("=" * 70)
    if not USAR_TLS:
        aviso("AVISO: SIN TLS. El ADR-001 exige TLS; esto solo vale para pruebas en banco.")
    if VEHICULO_ID == "sin-asignar":
        aviso("AVISO: CIT_VEHICULO vacio; vehicle_id ira como 'sin-asignar'.")
    if not CLAVE_LOCAL:
        aviso("AVISO: sin CIT_CLAVE_LOCAL. El contador rechazara media.open y no habra "
              "datos de camara ni de sesion.")

    Agente().correr()
    return 0


if __name__ == "__main__":
    sys.exit(main())
