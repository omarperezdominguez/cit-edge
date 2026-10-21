#!/usr/bin/env python3
# =============================================================================
# CIT Mobility, S.A.P.I. de C.V.
# Edge Mobility. contador_p2p_v17.py :: conteo de pasajeros + sesion de video
# acotada (ADR-001, modo VM)
#
#   RECONSTRUIDO el 20-oct-2026 a partir de AGENTS.md (secciones 4, 4.5bis y
#   4quinquies) y del contrato que espera agente.py, porque la carpeta cit/
#   se borro de la Jetson y este archivo nunca estuvo en git. Cuenta igual
#   que la v17 original: misma linea de config_nvdsanalytics.txt, mismo
#   conteo propio por track ID, misma sesion de video sin renovacion.
#
#   PIPELINE
#     v4l2src (/dev/videoN) -> caps 30 c/s -> videoconvert -> I420
#       -> nvvideoconvert -> NVMM NV12 -> nvstreammux 640x480
#       -> nvinfer (PeopleNet) -> nvtracker (NvDCF) -> nvdsanalytics
#            ^-- SONDA sonda_analitica(): cuenta y publica. Corre SIEMPRE.
#       -> valve (cerrada sin sesion: no se dibuja, ni convierte, ni comprime)
#            ^-- SONDA sonda_ritmo(): deja pasar FPS_VIDEO c/s
#       -> queue (leaky) -> [nvdsosd solo con DIAGNOSTICO=1]
#       -> nvvideoconvert -> I420 en CPU -> capssetter (FPS_VIDEO/1)
#       -> x264enc (CPU: la Orin Nano no tiene NVENC) -> h264parse
#       -> mpegtsmux -> udpsink 127.0.0.1:PUERTO_UDP_LOCAL
#     y, solo con sesion abierta, un proceso aparte:
#       gst-launch-1.0 udpsrc port=PUERTO_UDP_LOCAL ! srtsink -> MediaMTX (VM)
#
#   CONTROL LOCAL (solo lo usa agente.py; escucha en 127.0.0.1)
#     GET  /estado        aforo, entradas, salidas, video{...}, camara{...}
#     POST /video/abrir   {"segundos","camara","solicitante","orden"}
#     POST /video/cerrar
#     Todas exigen ?k=CLAVE (la genera cit.sh en datos/sesion.env).
#
#   BUS (cit_core/bus.py, origen ORIGEN): publica "aforo" al cambiar,
#   "cruce" por cada persona que cruza y "video" al abrir y cerrar.
#
# Uso (normalmente lo lanza cit.sh, ventana "contador"):
#   python3 contador_p2p_v17.py          # /dev/video0
#   python3 contador_p2p_v17.py 1        # /dev/video1
#
# Variables de entorno:
#   MODELO=peoplenet          yolo26 = detector retirado (solo comparar)
#   ORIGEN=camion01           origen en el bus
#   UNIDAD, UNIDAD_ID         nombre visible / device_id (cit-001)
#   CLAVE                     clave del control local; vacia = aleatoria
#   PUERTO_WEB=8080 ESCUCHAR=127.0.0.1
#   SRT_DESTINO=srt://<vm>:8890   SRT_STREAMID=publish:cit-001
#   SRT_PASSPHRASE=           vacia = video sin cifrar
#   SRT_LATENCIA_MS=300       PUERTO_UDP_LOCAL=5600
#   FPS_VIDEO=8  BITRATE=400 (kbps)  KEYFRAME_S=2
#   VENTANA_S=60              duracion maxima de sesion; tope duro 60
#   TOPE_SESIONES_MES=100     cuota mensual (UTC), en datos/cuota_video.json
#   CAMARA_ID=cam0
#   CAM_FORMATO=raw           mjpg si la camara solo da 30 c/s en MJPEG
#   CAM_ANCHO, CAM_ALTO       vacios = lo que negocie la camara
#   BANDA_CRUCE=8             franja muerta en px (4.5bis)
#   PUBLICAR_CRUCE=1          0 = no publica eventos cruce
#   DIAGNOSTICO=0             1 = linea, aforo y cajas sobre el video
#   CAJAS, MOSTRAR_LINEA, AFORO_EN_VIDEO, ETIQUETAS   superposiciones sueltas
#   SIN_GPS                   se acepta y se ignora: este contador no lee GPS
#
# Requisitos: DeepStream 7.1, pyds, gstreamer1.0-plugins-ugly (x264enc),
#   gstreamer1.0-plugins-bad (srtsink, mpegtsmux).
# =============================================================================

import hmac
import json
import math
import os
import secrets
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

AQUI = os.path.dirname(os.path.abspath(__file__))
CIT = os.path.dirname(AQUI)
sys.path.insert(0, CIT)

from cit_core import bus as bus_mod      # noqa: E402

VERSION = "1.7r"          # r = reconstruida el 20-oct-2026


def _env_bool(nombre, defecto="0"):
    return os.environ.get(nombre, defecto).strip().lower() in ("1", "true", "si", "yes")


# --- Parametros ---------------------------------------------------------------
ANCHO, ALTO, FPS = 640, 480, 30           # deben cuadrar con config_nvdsanalytics.txt
ETIQUETA_ENTRADA = "entrada"              # contrato con config_nvdsanalytics.txt
ETIQUETA_SALIDA = "salida"

MODELO = os.environ.get("MODELO", "").strip().lower()
ORIGEN = os.environ.get("ORIGEN", "camion01")
UNIDAD = os.environ.get("UNIDAD", "Unidad de prueba")
UNIDAD_ID = os.environ.get("UNIDAD_ID", "cit-001")
CLAVE = os.environ.get("CLAVE", "")
PUERTO_WEB = int(os.environ.get("PUERTO_WEB", "8080"))
ESCUCHAR = os.environ.get("ESCUCHAR", "127.0.0.1")

SRT_DESTINO = os.environ.get("SRT_DESTINO", "").strip()
SRT_STREAMID = os.environ.get("SRT_STREAMID", "publish:{}".format(UNIDAD_ID))
SRT_PASSPHRASE = os.environ.get("SRT_PASSPHRASE", "")
SRT_LATENCIA_MS = int(os.environ.get("SRT_LATENCIA_MS", "300"))
PUERTO_UDP_LOCAL = int(os.environ.get("PUERTO_UDP_LOCAL", "5600"))

FPS_VIDEO = max(1, int(os.environ.get("FPS_VIDEO", "8")))
BITRATE = int(os.environ.get("BITRATE", "400"))
KEYFRAME_S = max(1, int(os.environ.get("KEYFRAME_S", "2")))
VENTANA_S = min(60, max(1, int(os.environ.get("VENTANA_S", "60"))))   # tope duro 60
TOPE_SESIONES_MES = int(os.environ.get("TOPE_SESIONES_MES", "100"))
CAMARA_ID = os.environ.get("CAMARA_ID", "cam0")

CAM_FORMATO = os.environ.get("CAM_FORMATO", "raw").strip().lower()
CAM_ANCHO = os.environ.get("CAM_ANCHO", "").strip()
CAM_ALTO = os.environ.get("CAM_ALTO", "").strip()

BANDA = float(os.environ.get("BANDA_CRUCE", "8"))
OLVIDO = 5.0               # s sin ver un track antes de olvidarlo
PERMITIR_NEGATIVO = False  # v17: el aforo se fija en 0 (topes_en_cero)
PUBLICAR_CRUCE = _env_bool("PUBLICAR_CRUCE", "1")

DIAGNOSTICO = _env_bool("DIAGNOSTICO")
MOSTRAR_CAJAS = DIAGNOSTICO or _env_bool("CAJAS")
MOSTRAR_LINEA = DIAGNOSTICO or _env_bool("MOSTRAR_LINEA")
AFORO_EN_VIDEO = DIAGNOSTICO or _env_bool("AFORO_EN_VIDEO")
ETIQUETAS = _env_bool("ETIQUETAS")
CON_OSD = MOSTRAR_CAJAS or MOSTRAR_LINEA or AFORO_EN_VIDEO

DATOS = bus_mod.DIR_DATOS
CUOTA = os.path.join(DATOS, "cuota_video.json")
LOG_SRT = os.path.join(DATOS, "emisor_srt.log")

CONFIG_ANALYTICS = os.path.join(AQUI, "config_nvdsanalytics.txt")
CONFIG_TRACKER = os.path.join(AQUI, "config_tracker_NvDCF.yml")
TRACKER_MUESTRA = ("/opt/nvidia/deepstream/deepstream/samples/configs/"
                   "deepstream-app/config_tracker_NvDCF_perf.yml")
LIB_TRACKER = "/opt/nvidia/deepstream/deepstream/lib/libnvds_nvmultiobjecttracker.so"

VERDE_CIT = (0.0, 1.0, 0.42, 1.0)
UNTRACKED = 0xFFFFFFFFFFFFFFFF


def ahora_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def log(texto):
    print("[{}] {}".format(time.strftime("%H:%M:%S"), texto), flush=True)


# =============================================================================
# Conteo propio por track ID (AGENTS.md 4.5bis)
#   La linea sale de config_nvdsanalytics.txt: el config manda, el codigo
#   solo lo aplica. Un ID puede cruzar cuantas veces quiera; nvdsanalytics
#   (objLCCumCnt) gasta cada ID una sola vez por linea y por eso no se usa.
# =============================================================================
_entradas = 0
_salidas = 0
_aforo = 0
_topes_en_cero = 0
_tracks = {}              # id -> {"lado": -1|0|1, "visto": t}
_cerrojo_conteo = threading.Lock()


def _leer_linea_de_conteo(ruta=CONFIG_ANALYTICS, etiqueta=ETIQUETA_ENTRADA):
    """Devuelve {"dir": (dx1,dy1,dx2,dy2), "seg": (lx1,ly1,lx2,ly2), "extended": bool}."""
    seccion, linea, extended = None, None, True
    with open(ruta, encoding="utf-8") as f:
        for crudo in f:
            l = crudo.split("#", 1)[0].strip()
            if not l:
                continue
            if l.startswith("[") and l.endswith("]"):
                seccion = l[1:-1].strip()
                continue
            if seccion != "line-crossing-stream-0" or "=" not in l:
                continue
            clave, valor = (p.strip() for p in l.split("=", 1))
            if clave == "line-crossing-{}".format(etiqueta):
                numeros = [float(v) for v in valor.split(";") if v.strip()]
                if len(numeros) != 8:
                    raise ValueError("{} necesita 8 numeros, tiene {}".format(clave, len(numeros)))
                linea = numeros
            elif clave == "extended":
                extended = valor.strip() == "1"
    if linea is None:
        raise ValueError("no hay line-crossing-{} en {}".format(etiqueta, ruta))
    return {"dir": tuple(linea[:4]), "seg": tuple(linea[4:]), "extended": extended}


def _preparar_conteo(linea):
    """Precalcula lo que el conteo usa en cada cuadro."""
    dx1, dy1, dx2, dy2 = linea["dir"]
    lx1, ly1, lx2, ly2 = linea["seg"]
    ux, uy = lx2 - lx1, ly2 - ly1
    largo = math.hypot(ux, uy)
    if largo == 0:
        raise ValueError("el segmento de la linea de conteo mide 0")
    # Normal (sin normalizar) de la recta: la distancia con signo es
    # cross(u, c - p1) / |u|, y es positiva del lado de (-uy, ux).
    nx, ny = -uy, ux
    # Cruzar del lado negativo al positivo es moverse a lo largo de +n. Es
    # "entrada" si ese sentido coincide con la flecha del config.
    signo_entrada = 1 if (dx2 - dx1) * nx + (dy2 - dy1) * ny > 0 else -1
    return {"p1": (lx1, ly1), "u": (ux, uy), "largo": largo,
            "signo_entrada": signo_entrada, "extended": linea["extended"],
            "seg": (lx1, ly1, lx2, ly2)}


def _lado(geo, x, y):
    (x1, y1), (ux, uy) = geo["p1"], geo["u"]
    d = (ux * (y - y1) - uy * (x - x1)) / geo["largo"]
    if abs(d) < BANDA:
        return 0                 # pisando la linea: no se decide nada
    return 1 if d > 0 else -1


def _sobre_segmento(geo, x, y):
    (x1, y1), (ux, uy) = geo["p1"], geo["u"]
    t = ((x - x1) * ux + (y - y1) * uy) / (geo["largo"] ** 2)
    return 0.0 <= t <= 1.0


def _contar_cruces(geo, personas, t):
    """personas: [(id, x_centro, y_centro)]. Devuelve [(id, etiqueta)]."""
    global _entradas, _salidas, _aforo, _topes_en_cero
    cruces = []
    with _cerrojo_conteo:
        for ident, x, y in personas:
            lado = _lado(geo, x, y)
            previo = _tracks.get(ident)
            if previo is None:
                _tracks[ident] = {"lado": lado, "visto": t}
                continue
            previo["visto"] = t
            if lado == 0:
                continue
            if previo["lado"] != 0 and lado != previo["lado"]:
                if geo["extended"] or _sobre_segmento(geo, x, y):
                    # lado pasa de -1 a +1 => movimiento a lo largo de +n
                    if (lado - previo["lado"]) * geo["signo_entrada"] > 0:
                        etiqueta = ETIQUETA_ENTRADA
                        _entradas += 1
                        _aforo += 1
                    else:
                        etiqueta = ETIQUETA_SALIDA
                        _salidas += 1
                        if _aforo > 0 or PERMITIR_NEGATIVO:
                            _aforo -= 1
                        else:
                            _topes_en_cero += 1
                    cruces.append((ident, etiqueta))
            previo["lado"] = lado
        for ident in [i for i, d in _tracks.items() if t - d["visto"] > OLVIDO]:
            del _tracks[ident]
    return cruces


def conteo_actual():
    with _cerrojo_conteo:
        return {"aforo": _aforo, "entradas": _entradas, "salidas": _salidas,
                "topes_en_cero": _topes_en_cero}


def _recta_completa(seg, ancho=ANCHO, alto=ALTO):
    """La recta infinita del segmento, recortada al cuadro (extended=1)."""
    x1, y1, x2, y2 = seg
    ux, uy = x2 - x1, y2 - y1
    t0, t1 = -1e9, 1e9
    for p, q in ((-ux, x1), (ux, ancho - 1 - x1), (-uy, y1), (uy, alto - 1 - y1)):
        if p == 0:
            if q < 0:
                return seg
            continue
        r = q / p
        if p < 0:
            t0 = max(t0, r)
        else:
            t1 = min(t1, r)
    if t0 > t1:
        return seg
    return (x1 + t0 * ux, y1 + t0 * uy, x1 + t1 * ux, y1 + t1 * uy)


# =============================================================================
# Cuota de sesiones al mes (UTC), en disco con fsync
# =============================================================================
def _mes():
    return datetime.now(timezone.utc).strftime("%Y-%m")


def leer_cuota():
    try:
        with open(CUOTA, encoding="utf-8") as f:
            c = json.load(f)
        if c.get("mes") == _mes():
            return int(c.get("usadas", 0))
    except (OSError, ValueError):
        pass
    return 0


def guardar_cuota(usadas):
    os.makedirs(DATOS, exist_ok=True)
    tmp = CUOTA + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"mes": _mes(), "usadas": usadas, "tope": TOPE_SESIONES_MES}, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, CUOTA)


# =============================================================================
# Sesion de video (AGENTS.md 4quinquies)
# =============================================================================
class ControlVideo:
    """Lleva la sesion EN LA JETSON: abre, corta sola al vencer y cuenta la
    cuota. La VM solo pide; quien paga los datos celulares es la unidad."""

    def __init__(self, publicador):
        self.bus = publicador
        self.cerrojo = threading.RLock()
        self.abierta = False
        self.fin = 0.0
        self.inicio = 0.0
        self.camara = None
        self.solicitante = None
        self.orden = None
        self.srt = "apagado"
        self.proc = None
        self.valvula = None           # elemento valve, lo pone el pipeline
        self.codificador = None       # x264enc, para pedir cuadro clave
        self.ultimo_cuadro = 0.0      # time.time() del ultimo buffer de camara
        self._parar = threading.Event()
        threading.Thread(target=self._vigilar, daemon=True).start()

    # -- estado ---------------------------------------------------------------
    def estado(self):
        with self.cerrojo:
            restan = max(0, int(math.ceil(self.fin - time.time()))) if self.abierta else 0
            return {"abierta": self.abierta, "restan_s": restan,
                    "camara": self.camara if self.abierta else None,
                    "solicitante": self.solicitante if self.abierta else None,
                    "orden": self.orden if self.abierta else None,
                    "sesiones_restantes": max(0, TOPE_SESIONES_MES - leer_cuota()),
                    "tope_sesiones_mes": TOPE_SESIONES_MES,
                    "ventana_s": VENTANA_S, "srt": self.srt, "flujo": UNIDAD_ID}

    def estado_camara(self):
        edad = time.time() - self.ultimo_cuadro if self.ultimo_cuadro else None
        return {"id": CAMARA_ID, "activa": edad is not None and edad < 3.0,
                "edad_s": round(edad, 1) if edad is not None else None}

    # -- abrir / cerrar -------------------------------------------------------
    def abrir(self, segundos=None, camara=None, solicitante=None, orden=None):
        with self.cerrojo:
            if self.abierta:
                restan = max(0, int(math.ceil(self.fin - time.time())))
                return False, "sesion en curso, restan {} s".format(restan), None
            if camara not in (None, "", CAMARA_ID):
                return False, "camara inexistente: {}".format(camara), None
            try:
                seg = int(segundos) if segundos not in (None, "") else VENTANA_S
            except (TypeError, ValueError):
                return False, "duracion invalida: {}".format(segundos), None
            seg = max(1, min(seg, VENTANA_S))
            if not SRT_DESTINO:
                return False, "no se pudo iniciar el envio: sin destino", None
            if not self.estado_camara()["activa"]:
                return False, "la camara no entrega cuadros", None
            usadas = leer_cuota()
            if usadas >= TOPE_SESIONES_MES:
                return False, "tope de sesiones del mes agotado ({})".format(TOPE_SESIONES_MES), None
            # La sesion se cuenta ANTES de abrir: un corte a mitad no la regala.
            try:
                guardar_cuota(usadas + 1)
            except OSError as e:
                return False, "no se pudo guardar la cuota: {}".format(e), None
            if not self._lanzar_srt():
                guardar_cuota(usadas)
                return False, "no se pudo iniciar el envio: {}".format(self.srt), None

            self.abierta = True
            self.inicio = time.time()
            self.fin = self.inicio + seg
            self.camara = CAMARA_ID
            self.solicitante = solicitante
            self.orden = orden
            self._pedir_cuadro_clave()
            if self.valvula is not None:
                self.valvula.set_property("drop", False)
            log("VIDEO abierto {} s  solicitante={} orden={}  restan {} sesiones".format(
                seg, solicitante, orden, TOPE_SESIONES_MES - usadas - 1))
            self.bus.publicar("video", {"estado": "abierto", "segundos": seg,
                                        "camara": CAMARA_ID, "solicitante": solicitante,
                                        "orden": orden,
                                        "sesiones_restantes": TOPE_SESIONES_MES - usadas - 1})
            return True, None, seg

    def cerrar(self, motivo="cerrado por orden"):
        with self.cerrojo:
            if not self.abierta:
                return False
            if self.valvula is not None:
                self.valvula.set_property("drop", True)
            duro = time.time() - self.inicio
            datos = {"estado": "cerrado", "motivo": motivo, "segundos": round(duro, 1),
                     "camara": self.camara, "solicitante": self.solicitante,
                     "orden": self.orden}
            self.abierta = False
            self.camara = self.solicitante = self.orden = None
        # Medio segundo para que salgan los ultimos paquetes antes de cortar.
        time.sleep(0.5)
        self._apagar_srt()
        datos["sesiones_restantes"] = max(0, TOPE_SESIONES_MES - leer_cuota())
        datos["srt"] = self.srt
        log("VIDEO cerrado ({}) tras {} s".format(motivo, datos["segundos"]))
        self.bus.publicar("video", datos)
        return True

    def _pedir_cuadro_clave(self):
        """La sesion debe empezar con cuadro clave: sin el, el receptor espera
        hasta KEYFRAME_S segundos viendo gris."""
        if self.codificador is None:
            return
        try:
            from gi.repository import Gst, GstVideo
            ev = GstVideo.video_event_new_downstream_force_key_unit(
                Gst.CLOCK_TIME_NONE, Gst.CLOCK_TIME_NONE, Gst.CLOCK_TIME_NONE, True, 0)
            self.codificador.get_static_pad("sink").send_event(ev)
        except Exception as e:          # no es mortal: llega a los KEYFRAME_S
            log("AVISO: no se pudo pedir cuadro clave ({})".format(e))

    # -- envio SRT (proceso aparte) ------------------------------------------
    def _comando_srt(self):
        # OJO: debe empezar con "gst-launch-1.0 udpsrc ..." porque cit.sh y
        # agente.py lo buscan con '^gst-launch-1\.0 udpsrc .*srtsink'.
        cmd = ["gst-launch-1.0", "udpsrc", "port={}".format(PUERTO_UDP_LOCAL),
               "address=127.0.0.1", "!", "queue", "!", "srtsink",
               "uri={}".format(SRT_DESTINO), "mode=caller",
               "latency={}".format(SRT_LATENCIA_MS), "wait-for-connection=false",
               "streamid={}".format(SRT_STREAMID)]
        if SRT_PASSPHRASE:
            cmd += ["passphrase={}".format(SRT_PASSPHRASE), "pbkeylen=16"]
        return cmd

    def _lanzar_srt(self):
        self._apagar_srt()
        try:
            os.makedirs(DATOS, exist_ok=True)
            salida = open(LOG_SRT, "a")
            salida.write("\n=== {} sesion nueva hacia {} ===\n".format(ahora_iso(), SRT_DESTINO))
            salida.flush()
            self.proc = subprocess.Popen(self._comando_srt(), stdin=subprocess.DEVNULL,
                                         stdout=salida, stderr=subprocess.STDOUT,
                                         start_new_session=True)
            salida.close()
        except OSError as e:
            self.srt = "error al lanzar: {}".format(e)
            return False
        self.srt = "enviando"
        return True

    def _apagar_srt(self):
        p, self.proc = self.proc, None
        if p is not None and p.poll() is None:
            try:
                os.killpg(p.pid, signal.SIGTERM)
                p.wait(timeout=3)
            except (ProcessLookupError, subprocess.TimeoutExpired, PermissionError):
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
        self.srt = "apagado"

    # -- vigilancia: el equipo corta solo -------------------------------------
    def _vigilar(self):
        while not self._parar.wait(0.5):
            vencida = False
            with self.cerrojo:
                if not self.abierta:
                    continue
                if time.time() >= self.fin:
                    vencida = True
                elif self.proc is not None and self.proc.poll() is not None:
                    # srtsink en modo caller muere si la VM no contesta. Se
                    # relanza en la siguiente vuelta (0.5 s), sin cerrar la
                    # sesion: el conteo sigue y el tope de tiempo tambien.
                    log("AVISO: el envio SRT termino; se reintenta")
                    self._lanzar_srt()
                    self.srt = "sin respuesta de la vm, reintentando"
                elif self.srt.startswith("sin respuesta") and self.proc is not None \
                        and self.proc.poll() is None:
                    self.srt = "enviando"
            if vencida:
                self.cerrar("se agoto la ventana")

    def apagar(self):
        self._parar.set()
        self.cerrar("el contador se detuvo")
        self._apagar_srt()


# =============================================================================
# Control local HTTP (solo agente.py, en 127.0.0.1)
# =============================================================================
class Manejador(BaseHTTPRequestHandler):
    control = None
    rendimiento = None
    server_version = "cit-contador/" + VERSION

    def log_message(self, *args):
        pass                         # el agente pregunta cada segundo: sin ruido

    def _json(self, codigo, cuerpo):
        datos = json.dumps(cuerpo).encode("utf-8")
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(datos)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(datos)
        except (BrokenPipeError, ConnectionResetError):
            pass                     # el agente se canso de esperar; no es un error

    def _autorizado(self, url):
        k = (parse_qs(url.query).get("k") or [""])[0]
        return bool(CLAVE) and hmac.compare_digest(k.encode(), CLAVE.encode())

    def _estado(self):
        e = conteo_actual()
        e.update({"unidad": UNIDAD, "unidad_id": UNIDAD_ID, "ts_utc": ahora_iso(),
                  "version": VERSION, "modelo": MODELO_EN_USO,
                  "video": self.control.estado(), "camara": self.control.estado_camara()})
        if self.rendimiento:
            e["fps"] = self.rendimiento.fps()
            e["personas_en_cuadro"] = self.rendimiento.ultimas
            e["personas_con_id"] = self.rendimiento.ultimas_con_id
        return e

    def do_GET(self):
        url = urlparse(self.path)
        if not self._autorizado(url):
            return self._json(403, {"ok": False, "motivo": "clave invalida"})
        if url.path in ("/estado", "/aforo"):
            return self._json(200, self._estado())
        return self._json(404, {"ok": False, "motivo": "no existe"})

    def do_POST(self):
        url = urlparse(self.path)
        if not self._autorizado(url):
            return self._json(403, {"ok": False, "motivo": "clave invalida"})
        largo = int(self.headers.get("Content-Length") or 0)
        cuerpo = {}
        if largo:
            try:
                cuerpo = json.loads(self.rfile.read(min(largo, 65536)).decode("utf-8")) or {}
            except (ValueError, UnicodeDecodeError):
                return self._json(400, {"ok": False, "motivo": "se espera JSON"})
            if not isinstance(cuerpo, dict):
                cuerpo = {}
        if url.path == "/video/abrir":
            ok, motivo, seg = self.control.abrir(cuerpo.get("segundos"), cuerpo.get("camara"),
                                                 cuerpo.get("solicitante"), cuerpo.get("orden"))
            r = {"ok": ok, "video": self.control.estado()}
            if ok:
                r["segundos"] = seg
            else:
                r["motivo"] = motivo
            return self._json(200, r)
        if url.path == "/video/cerrar":
            cerro = self.control.cerrar("cerrado por orden")
            return self._json(200, {"ok": True, "estaba_abierta": cerro,
                                    "video": self.control.estado()})
        return self._json(404, {"ok": False, "motivo": "no existe"})


# =============================================================================
# Rendimiento (FPS y detecciones por cuadro)
# =============================================================================
class Rendimiento:
    def __init__(self):
        self.cuadros = 0
        self.detecciones = 0
        self.t0 = None
        self._ventana = []
        self.ultimas = 0
        self.ultimas_con_id = 0
        self.max_reciente = 0

    def anotar(self, detecciones, con_id=0):
        self.ultimas = detecciones
        self.ultimas_con_id = con_id
        self.max_reciente = max(self.max_reciente, detecciones)
        t = time.time()
        if self.t0 is None:
            self.t0 = t
        self.cuadros += 1
        self.detecciones += detecciones
        self._ventana.append(t)
        if len(self._ventana) > 60:
            self._ventana.pop(0)

    def fps(self):
        v = self._ventana
        if len(v) < 2 or v[-1] == v[0]:
            return 0.0
        return round((len(v) - 1) / (v[-1] - v[0]), 1)

    def resumen(self):
        if not self.cuadros or self.t0 is None:
            return "sin cuadros"
        dur = max(1e-6, time.time() - self.t0)
        return "{} :: {:.1f} FPS de promedio, {:.2f} detecciones por frame ({} frames)".format(
            MODELO_EN_USO, self.cuadros / dur, self.detecciones / self.cuadros, self.cuadros)


# =============================================================================
# Pipeline de DeepStream
# =============================================================================
MODELO_EN_USO = None


def elegir_modelo():
    """PeopleNet por omision. Si no esta descargado y nadie pidio modelo,
    se cae a YOLO26 (retirado) con aviso, para que la unidad no se quede
    sin contar."""
    pedido = MODELO or "peoplenet"
    ruta = os.path.join(AQUI, "config_infer_primary_{}.txt".format(pedido))
    if pedido == "peoplenet" and not os.path.exists(os.path.join(AQUI, "peoplenet",
                                                               "resnet34_peoplenet.onnx")):
        alterno = os.path.join(AQUI, "config_infer_primary_yolo26.txt")
        if not MODELO and os.path.exists(alterno):
            log("AVISO: falta peoplenet/resnet34_peoplenet.onnx; se usa YOLO26 (retirado).")
            log("       Corre ./preparar_jetson.sh para descargar PeopleNet.")
            return "yolo26", alterno
        sys.exit("ERROR: falta DeepStream-Yolo/peoplenet/resnet34_peoplenet.onnx. "
                 "Corre ./preparar_jetson.sh")
    if not os.path.exists(ruta):
        sys.exit("ERROR: no existe {}".format(ruta))
    return pedido, ruta


def crear(Gst, fabrica, nombre, **props):
    e = Gst.ElementFactory.make(fabrica, nombre)
    if e is None:
        sys.exit("ERROR: no se pudo crear '{}' ({}). Falta el plugin; corre "
                 "./preparar_jetson.sh".format(nombre, fabrica))
    for k, v in props.items():
        nombre_prop = k.rstrip("_").replace("_", "-")     # async_ -> async
        if isinstance(v, str) and nombre_prop not in ("device", "config-file-path",
                                                      "config-file", "ll-lib-file",
                                                      "ll-config-file", "host"):
            # Enums y flags por su nombre, igual que en gst-launch.
            Gst.util_set_object_arg(e, nombre_prop, v)
        else:
            e.set_property(nombre_prop, v)
    return e


def armar_pipeline(Gst, camara_dev, config_infer, control):
    p = Gst.Pipeline.new("contador-v17")

    camara = crear(Gst, "v4l2src", "camara", device=camara_dev)
    medidas = ""
    if CAM_ANCHO and CAM_ALTO:
        medidas = ",width={},height={}".format(int(CAM_ANCHO), int(CAM_ALTO))
    if CAM_FORMATO == "mjpg":
        caps_cam = "image/jpeg,framerate={}/1{}".format(FPS, medidas)
    else:
        caps_cam = "video/x-raw,framerate={}/1{}".format(FPS, medidas)
    f_cam = crear(Gst, "capsfilter", "caps-camara", caps=Gst.Caps.from_string(caps_cam))
    elementos = [camara, f_cam]
    if CAM_FORMATO == "mjpg":
        elementos.append(crear(Gst, "jpegdec", "decodificar-mjpg"))
    elementos += [
        crear(Gst, "videoconvert", "conv-cpu"),
        crear(Gst, "capsfilter", "caps-i420", caps=Gst.Caps.from_string("video/x-raw,format=I420")),
        crear(Gst, "nvvideoconvert", "conv-nvmm"),
        crear(Gst, "capsfilter", "caps-nvmm",
              caps=Gst.Caps.from_string("video/x-raw(memory:NVMM),format=NV12")),
    ]
    muxer = crear(Gst, "nvstreammux", "muxer", width=ANCHO, height=ALTO, batch_size=1,
                  live_source=1, batched_push_timeout=40000)
    inferencia = crear(Gst, "nvinfer", "inferencia-primaria", config_file_path=config_infer)

    tracker = crear(Gst, "nvtracker", "tracker", ll_lib_file=LIB_TRACKER,
                    ll_config_file=CONFIG_TRACKER if os.path.exists(CONFIG_TRACKER)
                    else TRACKER_MUESTRA,
                    tracker_width=640, tracker_height=384)
    try:
        tracker.set_property("display-tracking-id", 1 if ETIQUETAS else 0)
    except TypeError:
        pass
    analitica = crear(Gst, "nvdsanalytics", "analitica", config_file=CONFIG_ANALYTICS)

    valvula = crear(Gst, "valve", "valvula-video", drop=True)
    if valvula.find_property("drop-mode") is not None:
        # Que los eventos fijos (caps, segment) sigan llegando al codificador
        # aunque la valvula este cerrada; asi abre al instante.
        Gst.util_set_object_arg(valvula, "drop-mode", "forward-sticky-events")
    # La cola separa el hilo del codificador (CPU) del de la inferencia: si
    # x264 se atrasa, se tiran cuadros de video, nunca se frena el conteo.
    cola = crear(Gst, "queue", "cola-video", leaky="downstream", max_size_buffers=4,
                 max_size_bytes=0, max_size_time=0)

    salida = [valvula, cola]
    if CON_OSD:
        salida += [crear(Gst, "nvvideoconvert", "conv-osd"),
                   crear(Gst, "capsfilter", "caps-rgba",
                         caps=Gst.Caps.from_string("video/x-raw(memory:NVMM),format=RGBA")),
                   crear(Gst, "nvdsosd", "osd")]
    codificador = crear(Gst, "x264enc", "codificador", bitrate=BITRATE,
                        key_int_max=FPS_VIDEO * KEYFRAME_S, threads=2,
                        speed_preset="ultrafast", tune="zerolatency")
    mux_ts = crear(Gst, "mpegtsmux", "mux-ts")
    if mux_ts.find_property("alignment") is not None:
        mux_ts.set_property("alignment", 7)      # 7 x 188 = 1316 B, lo que SRT espera
    salida += [
        crear(Gst, "nvvideoconvert", "conv-salida"),
        crear(Gst, "capsfilter", "caps-salida",
              caps=Gst.Caps.from_string("video/x-raw,format=I420")),
        # videorate no sirve aqui (trampa 25): el ritmo se baja en sonda_ritmo
        # y se declara al codificador con capssetter.
        crear(Gst, "capssetter", "ritmo",
              caps=Gst.Caps.from_string("video/x-raw,framerate={}/1".format(FPS_VIDEO))),
        codificador,
        crear(Gst, "h264parse", "h264parse", config_interval=-1),
        mux_ts,
        crear(Gst, "udpsink", "envio-local", host="127.0.0.1", port=PUERTO_UDP_LOCAL,
              sync=False, async_=False),
    ]

    for e in elementos + [muxer, inferencia, tracker, analitica] + salida:
        p.add(e)
    for a, b in zip(elementos, elementos[1:]):
        if not a.link(b):
            sys.exit("ERROR: no enlaza {} -> {}".format(a.get_name(), b.get_name()))
    pad_mux = muxer.request_pad_simple("sink_0") if hasattr(muxer, "request_pad_simple") \
        else muxer.get_request_pad("sink_0")
    if elementos[-1].get_static_pad("src").link(pad_mux) != Gst.PadLinkReturn.OK:
        sys.exit("ERROR: no enlaza la camara con nvstreammux")
    cadena = [muxer, inferencia, tracker, analitica] + salida
    for a, b in zip(cadena, cadena[1:]):
        if not a.link(b):
            sys.exit("ERROR: no enlaza {} -> {}".format(a.get_name(), b.get_name()))

    control.valvula = valvula
    control.codificador = codificador
    return p, analitica, valvula


def main():
    global MODELO_EN_USO, CLAVE
    import gi
    gi.require_version("Gst", "1.0")
    gi.require_version("GstVideo", "1.0")
    from gi.repository import GLib, Gst
    import pyds

    Gst.init(None)
    camara_n = sys.argv[1] if len(sys.argv) > 1 else "0"
    camara_dev = camara_n if camara_n.startswith("/dev/") else "/dev/video{}".format(camara_n)
    if not os.path.exists(camara_dev):
        sys.exit("ERROR: no existe {}. Revisa la camara (ls /dev/video*).".format(camara_dev))

    MODELO_EN_USO, config_infer = elegir_modelo()
    geo = _preparar_conteo(_leer_linea_de_conteo())
    recta = _recta_completa(geo["seg"]) if geo["extended"] else geo["seg"]

    if not CLAVE:
        CLAVE = secrets.token_urlsafe(15)
        log("AVISO: sin CLAVE; se genero una: {}".format(CLAVE))

    publicador = bus_mod.Publicador(ORIGEN)
    control = ControlVideo(publicador)
    rendimiento = Rendimiento()
    pipeline, analitica, valvula = armar_pipeline(Gst, camara_dev, config_infer, control)

    # -- sonda principal: cuenta y publica. Va ANTES de la valvula ----------
    ultimo_publicado = [None]

    def sonda_analitica(pad, info):
        buf = info.get_buffer()
        if not buf:
            return Gst.PadProbeReturn.OK
        control.ultimo_cuadro = time.time()
        batch = pyds.gst_buffer_get_nvds_batch_meta(hash(buf))
        if not batch:
            return Gst.PadProbeReturn.OK
        l_frame = batch.frame_meta_list
        while l_frame is not None:
            try:
                frame = pyds.NvDsFrameMeta.cast(l_frame.data)
            except StopIteration:
                break
            personas, detecciones = [], 0
            l_obj = frame.obj_meta_list
            while l_obj is not None:
                try:
                    obj = pyds.NvDsObjectMeta.cast(l_obj.data)
                except StopIteration:
                    break
                if obj.class_id == 0:
                    detecciones += 1
                    if obj.object_id != UNTRACKED:
                        r = obj.rect_params
                        personas.append((int(obj.object_id), r.left + r.width / 2.0,
                                         r.top + r.height / 2.0))
                if CON_OSD:
                    if not MOSTRAR_CAJAS:
                        obj.rect_params.border_width = 0
                    if not ETIQUETAS:
                        obj.text_params.display_text = ""
                        obj.text_params.set_bg_clr = 0
                try:
                    l_obj = l_obj.next
                except StopIteration:
                    break
            rendimiento.anotar(detecciones, len(personas))

            for ident, etiqueta in _contar_cruces(geo, personas, time.time()):
                c = conteo_actual()
                log("CRUCE {:<8} id={}  aforo={}".format(etiqueta, ident, c["aforo"]))
                if PUBLICAR_CRUCE:
                    publicador.publicar("cruce", {"direccion": etiqueta, "id": ident,
                                                  "aforo": c["aforo"], "unidad": UNIDAD_ID})
            c = conteo_actual()
            firma = (c["aforo"], c["entradas"], c["salidas"])
            if firma != ultimo_publicado[0]:
                ultimo_publicado[0] = firma
                publicador.publicar("aforo", {"aforo": c["aforo"], "entradas": c["entradas"],
                                              "salidas": c["salidas"],
                                              "topes_en_cero": c["topes_en_cero"],
                                              "unidad": UNIDAD_ID})

            if CON_OSD and control.abierta and (MOSTRAR_LINEA or AFORO_EN_VIDEO):
                meta = pyds.nvds_acquire_display_meta_from_pool(batch)
                if AFORO_EN_VIDEO:
                    meta.num_labels = 1
                    t = meta.text_params[0]
                    t.display_text = "AFORO: {}".format(c["aforo"])
                    t.x_offset, t.y_offset = 10, 12
                    t.font_params.font_name = "Serif"
                    t.font_params.font_size = 20
                    t.font_params.font_color.set(*VERDE_CIT)
                    t.set_bg_clr = 1
                    t.text_bg_clr.set(0.0, 0.0, 0.0, 0.6)
                if MOSTRAR_LINEA:
                    meta.num_lines = 1
                    ln = meta.line_params[0]
                    ln.x1, ln.y1, ln.x2, ln.y2 = (int(v) for v in recta)
                    ln.line_width = 3
                    ln.line_color.set(*VERDE_CIT)
                pyds.nvds_add_display_meta_to_frame(frame, meta)
            try:
                l_frame = l_frame.next
            except StopIteration:
                break
        return Gst.PadProbeReturn.OK

    # -- sonda de ritmo: deja pasar FPS_VIDEO c/s hacia el codificador -------
    periodo = Gst.SECOND // FPS_VIDEO
    siguiente = [None]

    def sonda_ritmo(pad, info):
        buf = info.get_buffer()
        if not buf or buf.pts == Gst.CLOCK_TIME_NONE:
            return Gst.PadProbeReturn.OK
        if siguiente[0] is None or buf.pts >= siguiente[0] or buf.pts + 5 * Gst.SECOND < siguiente[0]:
            siguiente[0] = buf.pts + periodo
            return Gst.PadProbeReturn.OK
        return Gst.PadProbeReturn.DROP

    analitica.get_static_pad("src").add_probe(Gst.PadProbeType.BUFFER, sonda_analitica)
    valvula.get_static_pad("src").add_probe(Gst.PadProbeType.BUFFER, sonda_ritmo)

    # -- control local --------------------------------------------------------
    Manejador.control = control
    Manejador.rendimiento = rendimiento
    try:
        servidor = ThreadingHTTPServer((ESCUCHAR, PUERTO_WEB), Manejador)
    except OSError as e:
        sys.exit("ERROR: no se pudo abrir {}:{} ({}). ¿Otro contador corriendo?".format(
            ESCUCHAR, PUERTO_WEB, e))
    servidor.daemon_threads = True
    threading.Thread(target=servidor.serve_forever, daemon=True).start()

    # -- bucle ----------------------------------------------------------------
    bucle = GLib.MainLoop()
    codigo = [0]

    def al_mensaje(_bus, msg):
        t = msg.type
        if t == Gst.MessageType.EOS:
            log("Fin del flujo de la camara.")
            bucle.quit()
        elif t == Gst.MessageType.ERROR:
            err, dbg = msg.parse_error()
            log("ERROR de {}: {}".format(msg.src.get_name() if msg.src else "?", err))
            if dbg:
                log("   {}".format(dbg.splitlines()[0][:300]))
            codigo[0] = 1
            bucle.quit()
        elif t == Gst.MessageType.WARNING:
            w, _ = msg.parse_warning()
            log("AVISO de {}: {}".format(msg.src.get_name() if msg.src else "?", w))
        return True

    def latido():
        c = conteo_actual()
        log("estado: {} fps, hasta {} personas vistas en 30 s, aforo {} (E {} / S {}), camara {}".format(
            rendimiento.fps(), rendimiento.max_reciente, c["aforo"], c["entradas"], c["salidas"],
            "viva" if control.estado_camara()["activa"] else "SIN CUADROS"))
        rendimiento.max_reciente = 0
        return True
    GLib.timeout_add_seconds(30, latido)

    gbus = pipeline.get_bus()
    gbus.add_signal_watch()
    gbus.connect("message", al_mensaje)

    def detener(*_):
        log("Deteniendo...")
        bucle.quit()

    signal.signal(signal.SIGINT, detener)
    signal.signal(signal.SIGTERM, detener)

    print("=" * 70)
    print(" CIT Mobility :: contador v{}  (modo VM, ADR-001)".format(VERSION))
    print("-" * 70)
    print(" Unidad     : {} ({})".format(UNIDAD_ID, UNIDAD))
    print(" Camara     : {}  id={}  formato={}".format(camara_dev, CAMARA_ID, CAM_FORMATO))
    print(" Modelo     : {}  ({})".format(MODELO_EN_USO, os.path.basename(config_infer)))
    print(" Linea      : {} -> {}  extended={}  banda={} px".format(
        geo["seg"][:2], geo["seg"][2:], int(geo["extended"]), BANDA))
    print(" Bus        : {}  origen={}".format(bus_mod.descripcion(), ORIGEN))
    print(" Control    : http://{}:{}/estado".format(ESCUCHAR, PUERTO_WEB))
    print(" Video      : {}  {}x{} {} c/s  {} kbps  sesion <= {} s  tope {}/mes".format(
        SRT_DESTINO or "SIN DESTINO (no habra video)", ANCHO, ALTO, FPS_VIDEO, BITRATE,
        VENTANA_S, TOPE_SESIONES_MES))
    print("=" * 70, flush=True)
    if MODELO_EN_USO == "peoplenet" and not os.path.exists(os.path.join(
            AQUI, "peoplenet", "resnet34_peoplenet.onnx_b1_gpu0_int8.engine")):
        log("AVISO: el motor TensorRT no existe; construirlo tarda varios minutos. NO interrumpir.")

    publicador.publicar("aforo", dict(conteo_actual(), unidad=UNIDAD_ID))
    pipeline.set_state(Gst.State.PLAYING)
    try:
        bucle.run()
    finally:
        control.apagar()
        pipeline.set_state(Gst.State.NULL)
        servidor.shutdown()
        publicador.cerrar()
        c = conteo_actual()
        log("Rendimiento: {}".format(rendimiento.resumen()))
        log("Final: aforo {}  entradas {}  salidas {}  topes_en_cero {}".format(
            c["aforo"], c["entradas"], c["salidas"], c["topes_en_cero"]))
    sys.exit(codigo[0])


if __name__ == "__main__":
    main()
