# AGENTS.md — Unidad Edge Mobility de CIT Mobility

Contexto para agentes de IA y para cualquiera que llegue nuevo al proyecto.
Describe cómo funciona la unidad que va a bordo: **lee el GPS, cuenta
pasajeros con visión artificial, manda ambos datos a la nube y sirve video
bajo demanda** a quien tenga la liga. La carpeta tiene mucho material que no
participa; está listado al final, en
[Archivos que NO forman parte del sistema](#9-archivos-que-no-forman-parte-del-sistema),
para que no se toque por error.

Última revisión: **24 de septiembre de 2026**, verificada contra el código en
disco y probada en banco.

> **Decisión vigente: ADR-001 v1.6** (*Interfaz de datos entre Edge Mobility y
> las plataformas RuTi*). La unidad habla con una **VM propia** por tres
> planos: **telemetría** (MQTT con TLS, mensaje único de la tabla 3 del ADR),
> **medios** (video H.264 por SRT a MediaMTX, solo por sesión acotada) y
> **control** (comandos de una lista cerrada, de bajada por la misma conexión
> MQTT). La Jetson ya está lista para ello; falta la VM. Contrato completo,
> pruebas y pendientes: **[PLAN_VM.md](PLAN_VM.md)**.
>
> **AWS IoT Core queda de reserva.** `reenviador.py`, `~/cit.sh` v3 y `certs/`
> se conservan **sin cambios** para usarlos más adelante. No se borran ni se
> modifican (sección 0.2).

Cambios del 20-oct-2026:

- **La carpeta `cit/` se borró de la Jetson (12-oct).** Lo que estaba en git
  (rama `respaldo`) se recuperó; `DeepStream-Yolo/` no, porque estaba entera
  en `.gitignore`. Se **reconstruyó** `contador_p2p_v17.py` (v1.7r) a partir
  de este documento, junto con `config_nvdsanalytics.txt` y
  `config_infer_primary_peoplenet.txt` (ahora con rutas relativas).
  `preparar_jetson.sh` revisa dependencias, descarga PeopleNet de NGC, crea
  el tracker desde la muestra de DeepStream y construye el motor (`--motor`).
- **`.gitignore` ya no ignora `DeepStream-Yolo/` entera**: solo modelos,
  motores y binarios. El código del contador y sus configs van a git.
- **`module.shutdown`** nuevo en `agente.py`: apaga la Jetson
  (`systemctl poweroff`) en lugar de reiniciarla (5ter.3).


- **`agente.py` nuevo**: el agente publicador único del ADR. Arma la
  telemetría, la guarda en disco antes de subirla y atiende los comandos
  (sección 5ter). Sustituye al reenviador en la ventana `vm`.
- **`contador_p2p_v17.py` nuevo**: sesión de video sin renovación, con la
  duración que pida el comando (≤ 60 s) y cuota en **sesiones al mes**
  guardada en disco (sección 4quinquies).
- **`lector_gps.py`**: deja además la última lectura en
  `/dev/shm/cit/gps_actual.json` para la telemetría a 1 Hz. El bus no cambia.
- **`cit/cit.sh` v5**: ventana `vm` con el agente, `reiniciar`,
  `instalar-arranque`; TLS hacia la VM por omisión.

Cambios de la revisión anterior (22-sep-2026), que siguen vigentes: el
arranque es `cit.sh`, no `variables.py` (este queda como modo de banco,
sección 6); PeopleNet como modelo (4.2), el conteo propio por track ID
(4.5bis), el GPS con `cit_core/` (4bis) y el bus por archivo (5).

---

## 0. Resumen en una página

### 0.0 Arquitectura vigente (ADR-001, modo VM)

```
 cámara USB (/dev/video0)            módulo SIM7600G-H (/dev/ttyUSB*, NMEA 1 Hz)
        │                                        │
        ▼                                        ▼
 contador_p2p_v17.py                       lector_gps.py
   PeopleNet → NvDCF → cruce de línea        cit_core/modulo_gps + nmea
        │         │                              │           │
        │         │ aforo, cruce, video          │ gps       │ cada lectura
        │         ▼                              ▼           ▼
        │   datos/eventos.jsonl  ◄───────────────┘   /dev/shm/cit/gps_actual.json
        │         │ (cruces)                                  │
        │         ▼                                           │
        │    agente.py ◄──────────────────────────────────────┘
        │      │  muestra de la tabla 3 → datos/salida_vm.jsonl → MQTT QoS1 + TLS :8883
        │      │      cit/v1/<device>/{telemetria,resultado,estado}   ──────────► VM
        │      │  comandos (lista cerrada) ◄── cit/v1/<device>/cmd  ◄────────── VM
        │      ▼
        │   POST 127.0.0.1:8080/video/abrir | /video/cerrar  (control local, con clave)
        ▼
 válvula → H.264 8 c/s → UDP :5600 → srtsink (solo con sesión) ── SRT :8890 ──► MediaMTX (VM)
```

| Plano / función | Quién la hace | Hacia dónde va |
|---|---|---|
| **Leer el GPS** | `lector_gps.py` + `cit_core/` | Bus local y `/dev/shm/cit/gps_actual.json` |
| **Contar personas** | `DeepStream-Yolo/contador_p2p_v17.py` | Bus local (`cruce`, `aforo`, `video`) |
| **Telemetría** | `agente.py` | VM, `cit/v1/<device>/telemetria` (MQTT, TLS, QoS 1) |
| **Control** | `agente.py` | Recibe `cit/v1/<device>/cmd`, responde en `.../resultado` |
| **Medios** | `contador_p2p_v17.py` + `gst-launch srtsink` | MediaMTX en la VM, por SRT, solo con sesión abierta |
| **Arrancar / parar todo** | `cit/cit.sh` v5 + `unidad.env` | Sesión de tmux `cit`: `contador`, `gps`, `vm` (y `aws`/`tunel` si se piden) |

El punto de unión sigue siendo el **bus por archivo**: el contador y el GPS
solo escriben en `datos/eventos.jsonl`. El agente lee de ahí los cruces (para
`pax_in`/`pax_out`), la posición al segundo de `/dev/shm` y el estado de la
cámara y del video del control local del contador. Nadie más sabe que el
agente existe.

### 0.1 Cómo se arranca: `cit/cit.sh` v5

```bash
cd ~/Desktop/cit
cp unidad.env.ejemplo unidad.env && chmod 600 unidad.env   # una sola vez; llenarlo
./cit.sh arrancar          # levanta contador, gps y vm en tmux
./cit.sh estado            # procesos, cámara, video, sesiones restantes, agente
./cit.sh reiniciar         # parar + arrancar (lo usa el comando app.restart)
./cit.sh parar             # apagado ordenado
./cit.sh instalar-arranque # arrancar sola al encender (crontab @reboot). INSTALADO 27-sep
tmux a -t cit              # ver las ventanas (Ctrl+b d para salir sin apagar)
```

> ⚠️ **Hay dos copias de `cit.sh` y no son iguales** (trampa 20).
> - **`cit/cit.sh` (v5)**: modo VM del ADR-001. Es el que se describe aquí.
> - **`~/cit.sh` (v3)**: modo AWS + página MJPEG por túnel. **De reserva**,
>   sin cambios (sección 0.2).

Ventanas que crea `cit/cit.sh` v5 (todas cargan `unidad.env`; las contraseñas
no aparecen en el texto de los comandos):

| Ventana | Comando | Cuándo |
|---|---|---|
| `contador` | `SIN_GPS=1 SRT_DESTINO=srt://$VM_HOST:8890 … python3 contador_p2p_v17.py` | Siempre |
| `gps` | `python3 lector_gps.py` | Siempre |
| `vm` | `CIT_UNIDAD=… CIT_VEHICULO=… CIT_BROKER=$VM_HOST CIT_TLS=1 … python3 agente.py` | Si hay `VM_HOST` |
| `aws` | `reenviador.py` con los certificados de `certs/` | Solo con `ENVIAR_AWS=1` (reserva) |
| `tunel` | `cloudflared …` | Solo con `TUNEL=1` y `CONTADOR=contador_p2p_v15.py` |

Variables de `unidad.env` (plantilla en `unidad.env.ejemplo`):

| Variable | Default | Para qué |
|---|---|---|
| `UNIDAD_ID` | `cit-001` | `device_id` (el módulo) y nombre del flujo en MediaMTX |
| `VEHICULO_ID` | vacío | `vehicle_id`: camión donde está instalado. Vacío → `sin-asignar` |
| `VM_HOST` | vacío | Sin él no hay ventana `vm`: ni telemetría a la VM ni comandos ni video |
| `VM_MQTT_PUERTO` / `VM_MQTT_TLS` | `8883` / `1` | El ADR exige TLS. `0` + `1883` solo en banco |
| `VM_MQTT_CA` / `_CERT` / `_LLAVE` | vacíos | CA propia de la VM (vacío = las del sistema); cert/llave solo con TLS mutuo |
| `VM_MQTT_USUARIO` / `_CONTRASENA` | `cit-001` / vacío | Credencial **propia de este equipo** en el broker |
| `VM_SRT_PUERTO` / `VM_SRT_PASSPHRASE` / `VM_SRT_STREAMID` | `8890` / vacío / `publish:cit-001` | Video hacia MediaMTX |
| `FPS_VIDEO` / `BITRATE` | `8` / `400` | Ritmo y tope del H.264 |
| `VENTANA_S` | `60` | Duración máxima de una sesión (tope duro 60) |
| `TOPE_SESIONES_MES` | `100` | **Provisional**; lo fija Dirección general (ADR, tabla 5) |
| `ENVIAR_AWS` | `0` | `1` enciende la ventana `aws` (reserva) |
| `CONTADOR` / `TUNEL` | `contador_p2p_v17.py` / `0` | Volver al modo página: `contador_p2p_v15.py` + `TUNEL=1` |

`parar` pide salida limpia primero (`SIGTERM` al agente y al reenviador para
que publiquen "desconectada"; `SIGINT` al contador para que libere la
cámara), espera 6 s y barre lo que quede, incluido el envío SRT, que vive en
su propio grupo de procesos. Los patrones de `pkill` van anclados (trampa 24).

### 0.2 Modo de reserva: AWS IoT Core (`~/cit.sh` v3)

> **Se conserva tal cual, apagado.** Todo lo de esta subsección y la 5bis
> describe código que **no se tocó** el 24-sep-2026 y que se usará más
> adelante. No borrar ni modificar `reenviador.py`, `~/cit.sh`, `certs/` ni
> la ventana `aws` de `cit/cit.sh`.


```
 cámara USB (/dev/video0)            módulo SIM7600G-H (/dev/ttyUSB*, NMEA 1 Hz)
        │                                        │
        ▼                                        ▼
 contador_p2p_v15.py                       lector_gps.py
   PeopleNet → NvDCF → cruce de línea        cit_core/modulo_gps + nmea
        │              │                         │
        │ aforo, cruce │                         │ gps
        │              ▼                         ▼
        │        datos/eventos.jsonl  ◄──────────┘      (bus por archivo)
        │              │
        │              ▼
        │        reenviador.py ── MQTT QoS1 + TLS :8883 ──► AWS IoT Core
        │                                                   cit/v1/<unidad>/{aforo,cruce,gps,estado}
        ▼
 servidor web :8080  (página, /estado, /video.mjpg, /instantanea.jpg)
        │
        ▼
 cloudflared (Quick Tunnel) ──► https://<algo>.trycloudflare.com/?k=CLAVE ──► navegador
```

| Función | Quién la hace | Hacia dónde va |
|---|---|---|
| **Leer el GPS** | `lector_gps.py` + `cit_core/` | Bus local (`datos/eventos.jsonl`) |
| **Contar personas** | `DeepStream-Yolo/contador_p2p_v15.py` | Bus local + `/estado` de la página |
| **Enviar los datos** | `reenviador.py` | AWS IoT Core por MQTT con TLS mutuo |
| **Video** | `contador_p2p_v15.py` (MJPEG) + `cloudflared` | Navegador, por el túnel, solo bajo demanda |
| **Arrancar / parar todo** | `cit.sh` | Sesión de tmux `cit` con 4 ventanas |

El punto de unión es el **bus por archivo**: los productores (contador y GPS)
solo escriben líneas JSON en `datos/eventos.jsonl`; el reenviador las lee y
las sube. Ninguno sabe que los otros existen. Si se cae internet, los eventos
se acumulan en disco y se envían al volver (sección 5bis).

#### Cómo se arranca el modo de reserva

```bash
~/cit.sh arrancar     # levanta todo en tmux y muestra la liga del video
~/cit.sh estado       # procesos vivos, página local, prueba de video, liga
~/cit.sh liga         # solo la liga (con la clave ya incluida)
~/cit.sh parar        # apagado ordenado
tmux a -t cit         # ver las ventanas (Ctrl+b d para salir sin apagar)
```

> Ojo: esto es `~/cit.sh` (v3, en el directorio personal), **no**
> `cit/cit.sh` (v5). Ambos usan la sesión de tmux `cit`, así que no se pueden
> correr a la vez.

Ventanas que crea `cit.sh` v3 (sesión `cit`):

| # | Ventana | Comando | Carpeta |
|---|---|---|---|
| 0 | `contador` | `SIN_GPS=1 UNIDAD=… UNIDAD_ID=… CLAVE=… FPS_VIDEO=… CALIDAD_JPEG=… python3 contador_p2p_v15.py` | `DeepStream-Yolo/` |
| 1 | `gps` | `python3 lector_gps.py` | `cit/` |
| 2 | `nube` | `CIT_UNIDAD=… CIT_BROKER=… CIT_CA/CERT/LLAVE=certs/… python3 reenviador.py` | `cit/` |
| 3 | `tunel` | `cloudflared tunnel --protocol http2 --edge-ip-version 4 --url http://localhost:8080 \| tee ~/tunel.log` | `~` |

Variables con las que se puede llamar a `cit.sh`:

| Variable | Default | Para qué |
|---|---|---|
| `UNIDAD_ID` | `cit-001` | Identificador en la nube. También elige los certificados `certs/<UNIDAD_ID>.*` |
| `UNIDAD` | `Ruta A-01` | Nombre visible en la página |
| `CLAVE` | `citdemo2026` | Clave de la página. **Es fija y de demostración** (ver trampa 17) |
| `CIT_BROKER` | `a3g1b4rvksirbn-ats.iot.us-east-1.amazonaws.com` | Endpoint de AWS IoT Core |
| `CONTADOR` | `contador_p2p_v15.py` | Qué versión del contador lanzar |
| `FPS_VIDEO` | `8` | Cuadros por segundo del video |
| `CALIDAD_JPEG` | `60` | Calidad del JPEG (20–95) |

`SIN_GPS=1` va fijo en la ventana del contador **a propósito**: el GPS lo lee
`lector_gps.py`, y si el contador también abriera el puerto serie se robarían
los bytes (trampa 9). Consecuencia: la página web muestra *"modulo gps no
cargado"* aunque el GPS funcione y llegue a la nube (trampa 18).

`parar` pide salida limpia primero (`SIGTERM` al reenviador para que publique
"desconectada"; `SIGINT` al contador para que libere la cámara), espera 6 s y
luego barre con `pkill` todo lo que quede, incluidos `contador_neto.py`,
`variables.py` y `go2rtc` por si alguien los dejó corriendo.

La liga del túnel es de **Cloudflare Quick Tunnel**: cambia **cada vez** que se
arranca. Se extrae de `~/tunel.log` con una expresión que exige un guion en el
nombre (para no confundirse con `api.trycloudflare.com`).

---

## 1. Qué hace el sistema

Mide dos variables de una unidad de transporte, las manda a la nube y deja ver
el video de la cámara a quien tenga la liga:

| Variable | Qué responde |
|---|---|
| **Aforo** | Cuánta gente va dentro (y cada cruce individual) |
| **GPS** | Dónde está la unidad |

**Aforo.** Cuenta cuántas personas hay dentro de un espacio (un camión de ruta,
en el caso de uso original) usando una cámara USB y visión por computadora sobre
una Jetson. Una línea virtual atraviesa la imagen. Quien la cruza de **izquierda
a derecha** suma uno al aforo; quien la cruza de **derecha a izquierda** resta
uno. El aforo es la resta de ambos acumulados. Cada cruce se publica además como
evento propio, con el ID del track.

**GPS.** Lee el flujo NMEA del módulo SIM7600G-H y publica la posición. El GNSS
trabaja en modo standalone: **no necesita tarjeta SIM ni red**.

**Nube (ADR-001).** `agente.py` resume el estado de la unidad en un mensaje
único de telemetría (posición, velocidad, conteo, salud, sesión de video) y
lo sube a la VM por MQTT con TLS, QoS 1 y garantía de al menos una entrega,
pasando antes por un diario en disco. Por la misma conexión recibe los
comandos de la plataforma. *(De reserva: `reenviador.py` reenvía el bus tal
cual a AWS IoT Core; sección 5bis.)*

**Video.** Solo cuando la plataforma lo pide con un comando `media.open`, en
sesiones de 60 s como máximo que **no se renuevan** y que la Jetson corta
sola, con un tope de sesiones al mes contado en la Jetson (sección
4quinquies). *(Modo de reserva v15: página con MJPEG, 4ter.)*

En el modo VM corren **tres procesos** (contador, GPS y agente, sección 0.1),
más el envío SRT solo mientras hay sesión de video. Se comunican a través
de un bus de mensajes (`cit_core/bus.py`) que **no** depende de la relación
padre-hijo entre procesos. Esto es deliberado: permite correr cada programa por
separado y añadir productores o consumidores sin tocar a los demás. El GPS, el
reenviador y el agente se añadieron justamente así, sin tocar nada del conteo.

---

## 2. Plataforma

| Componente | Versión |
|---|---|
| Placa | NVIDIA Jetson Orin Nano Engineering Reference Developer Kit Super |
| L4T / JetPack | R36.5.0 (aarch64) |
| DeepStream SDK | 7.1 (`/opt/nvidia/deepstream/deepstream/`) |
| Python | 3.10.12 del sistema (**no** hay entorno virtual en uso) |
| Bindings | `pyds` (DeepStream Python Apps) |
| GStreamer | El que trae DeepStream, vía `gi` / PyGObject |

Detalle relevante: **la Orin Nano no tiene NVENC**. Cualquier codificación de
video tiene que hacerse por CPU. Por eso el video sale como JPEG por CPU
(`jpegenc`), limitado a 8 cuadros/s, y solo mientras alguien mira (4ter).

Dependencias fuera de la librería estándar:

| Qué | Quién la usa | Obligatoria |
|---|---|---|
| `paho-mqtt` (pip, API v2) | `reenviador.py`; también el bus si `CIT_TRANSPORTE=mqtt` | **Sí** para enviar a la nube |
| `gstreamer1.0-plugins-good` / `-base` (`jpegenc`, `appsink`) | `contador_p2p_v15.py` | Sí |
| `cloudflared` (`/usr/local/bin`) | túnel de la página | Sí, para acceso desde internet |
| `tmux` | `cit.sh` | Sí |
| `pyserial` | solo `gps_edge.py` (antiguo, 9.1) | **No**. `cit_core/serie.py` lo sustituye |

El GPS y el bus siguen sin necesitar nada de `pip`. Si al leer el código
aparece un `import serial`, se está mirando `gps_edge.py`, que ya no se usa
(con `SIN_GPS=1` el contador no lo carga).

Certificados de AWS IoT, en `cit/certs/` (permisos `700`):
`AmazonRootCA1.pem`, `cit-001.cert.pem`, `cit-001.private.key`. El nombre de
los dos últimos sale de `UNIDAD_ID`. **La carpeta no está en `.gitignore`**
(trampa 19).

---

## 3. Arquitectura general

Cuatro capas, con una frontera clara entre cada una:

1. **Productores** — `contador_p2p_v17.py` (aforo, cruces y sesiones de
   video; en reserva, la v15) y `lector_gps.py` (posición). Solo publican al
   bus (el GPS deja además su última lectura en `/dev/shm`).
2. **Bus** — `cit_core/bus.py`. Único punto que sabe *cómo* viajan los datos
   dentro de la Jetson. Por omisión, un archivo: `datos/eventos.jsonl`.
3. **Consumidores** — `agente.py` (modo VM: arma la telemetría del ADR y la
   sube; atiende comandos), `reenviador.py` (reserva: reenvía a AWS) y, en
   modo de banco, `variables.py` (pinta en la terminal). Leen el bus; no
   conocen a los productores.
4. **Acceso humano** — en el modo VM, las plataformas RuTi a través de la VM
   (API, tiempo real y video por MediaMTX): la Jetson no expone nada. En el
   modo de reserva, la página del contador v15 en `:8080` por `cloudflared`.

Ningún productor importa a un consumidor ni al revés. Los productores tampoco
se conocen entre sí: el contador no sabe que existe el GPS.

### 3.1 Recorrido de un dato, de punta a punta

Una persona sube al camión:

1. La cámara entrega el cuadro; PeopleNet la detecta y NvDCF le da un ID (42).
2. La sonda de `contador_p2p_v15.py` ve que el centro de su caja pasó de un lado
   de la línea al otro → `CRUCE entrada id=42` en su terminal.
3. Publica al bus dos líneas en `datos/eventos.jsonl`:
   ```json
   {"ts":"2026-09-09T05:20:11Z","origen":"camion01","tipo":"cruce","direccion":"entrada","id":42,"aforo":3}
   {"ts":"2026-09-09T05:20:11Z","origen":"camion01","tipo":"aforo","aforo":3,"entradas":5,"salidas":2}
   ```
4. **Modo VM:** `agente.py` (revisa cada 200 ms) ve el `cruce` de entrada,
   suma `pax_in` y lo guarda en `datos/agente_estado.json`. Como `pax` cambió,
   arma en ese momento una muestra de la tabla 3 del ADR con la posición de
   `/dev/shm/cit/gps_actual.json` y el estado de cámara y video del contador,
   la escribe en `datos/salida_vm.jsonl` y la publica QoS 1 en
   `cit/v1/cit-001/telemetria`. Cuando la VM confirma (PUBACK), avanza
   `datos/salida_vm.pos`.
5. *(Modo de reserva: `reenviador.py` reenvía las dos líneas tal cual a AWS,
   en `cit/v1/cit-001/cruce` y `.../aforo`, y la página ve `aforo: 3` en
   `/estado`.)*

El GPS escribe en el bus cada 5 s o al moverse más de 10 m. El agente, en
cambio, manda la posición en cada muestra: una por segundo en marcha y una
cada 10 s detenida.

---

## 4. La visión: `contador_p2p_v17.py` (modo VM), `contador_p2p_v15.py` (reserva) y `contador_neto.py` (banco)

Hay tres programas de visión en uso, y **cuentan exactamente igual**: la misma
geometría leída de `config_nvdsanalytics.txt`, el mismo conteo por track ID
(4.5bis), el mismo modelo y el mismo tracker. Difieren en lo que hacen con el
resultado:

| | `contador_p2p_v17.py` (**modo VM, ADR-001**) | `contador_p2p_v15.py` (reserva) | `contador_neto.py` (banco) |
|---|---|---|---|
| Lo lanza | `cit/cit.sh` v5 | `~/cit.sh` v3 | `variables.py` |
| Publica al bus | `aforo`, `cruce` y `video` | `aforo` y `cruce` | `aforo` |
| Salida de video | H.264 por SRT a la VM, solo por sesión (4quinquies) | MJPEG por su servidor web `:8080` | Ventana local `nveglglessink` |
| Dónde va la sonda | Src pad de `nvdsanalytics`, **antes** de la válvula | igual que v17 | Sink pad del `nvdsosd` |
| Superposición en el video | Apagada (`DIAGNOSTICO=1` la enciende) | Apagada | Encendida |
| Aforo negativo | Se fija en 0 (`topes_en_cero`) | igual | Permitido |

Lo que sigue en 4.1–4.6 se escribió sobre `contador_neto.py`. Todo aplica a
`contador_p2p_v15.py` y `_v17.py` salvo el final del pipeline y la posición de
la sonda, descritos en 4ter (v15) y 4quinquies (v17).

### 4.1 El pipeline de GStreamer

Se construye a mano, elemento por elemento, con `Gst.ElementFactory.make()`.
El nombre entre paréntesis es el que se le da en el código:

```
v4l2src (camara)                     /dev/video0 por defecto
  │
capsfilter (caps-camara)             framerate=30/1
  │
videoconvert (conv-cpu)              conversión en CPU
  │
capsfilter (caps-i420)               format=I420
  │
nvvideoconvert (conv-nvmm)           pasa a memoria de GPU
  │
capsfilter (caps-nvmm)               video/x-raw(memory:NVMM), format=NV12
  │
nvstreammux (muxer)                  640x480, batch-size=1, live-source=1
  │                                  batched-push-timeout=40000 (40 ms)
nvinfer (inferencia-primaria)        PeopleNet → detecta personas
  │
nvtracker (tracker)                  NvDCF → asigna un ID a cada persona
  │
nvdsanalytics (analitica)            detecta cruces de línea
  │
nvvideoconvert (conv-osd)
  │
nvdsosd (osd)                        dibuja cajas y el texto del aforo
  │                                  ◄── AQUÍ va la sonda (probe)
nvegltransform (transform)           solo en aarch64 con nveglglessink
  │
nveglglessink (sink)                 ventana en pantalla
```

Notas sobre el armado:

- El enlace `caps-nvmm → muxer` **no** es un `link()` normal. Al muxer hay que
  pedirle un pad de entrada: `muxer.request_pad_simple("sink_0")`, con
  `get_request_pad()` como alternativa en versiones viejas de GStreamer.
- `nvegltransform` solo se inserta si `platform.machine() == "aarch64"` y el
  sink es `nveglglessink`. En la Jetson siempre se cumple.
- El sink se puede cambiar con la variable `SINK` (ej. `SINK=nv3dsink`).
- `sync=0` en el sink: no sincroniza con el reloj, evita acumular retraso con
  una fuente en vivo.

### 4.2 La inferencia — `config_infer_primary_peoplenet.txt`

**PeopleNet es el modelo del proyecto desde el 29-ago-2026.** Es el valor por
omisión de `MODELO` en `contador_neto.py`: no hay que pasar nada por entorno
para usarlo. Modelo: NVIDIA TAO PeopleNet, `deployable_quantized_onnx_v2.6.3`
(ResNet34 / DetectNet_v2, cuantizado a INT8).

| Parámetro | Valor | Qué significa |
|---|---|---|
| `onnx-file` | `peoplenet/resnet34_peoplenet.onnx` | 89 MB |
| `model-engine-file` | `peoplenet/resnet34_peoplenet.onnx_b1_gpu0_int8.engine` | Motor TensorRT **ya construido** |
| `int8-calib-file` | `peoplenet/resnet34_peoplenet_int8.txt` | Tabla de calibración del fabricante |
| `network-mode` | `1` | INT8 |
| `num-detected-classes` | `3` | `person`, `bag`, `face` |
| `infer-dims` | `3;544;960` | **Fija**: el ONNX no admite otra cosa |
| `interval` | `2` | Infiere 1 de cada 3 frames; el tracker rellena los huecos |
| `cluster-mode` | `1` | DBSCAN, lo que NVIDIA recomienda para DetectNet_v2 |
| `output-blob-names` | `output_cov/Sigmoid:0;output_bbox/BiasAdd:0` | |
| `maintain-aspect-ratio` | `1` | Con `symmetric-padding=1`. Ver más abajo |

No lleva `custom-lib-path` ni `parse-bbox-func-name`: **DeepStream sabe leer
DetectNet_v2 de fábrica**. Esta es la diferencia práctica más grande con el
config retirado de YOLO26, que sí dependía de un `.so` compilado a mano.

**Truco importante que hay que entender antes de tocar nada:**

```ini
[class-attrs-all]
pre-cluster-threshold=1.0     # umbral imposible → descarta TODAS las clases

[class-attrs-0]
pre-cluster-threshold=0.30    # excepto la clase 0 = person
```

Se filtran todas las clases menos personas poniendo un umbral de confianza
inalcanzable (1.0) a todo y bajándolo solo para la clase 0. Aquí descarta `bag`
y `face`. **Si se sube `class-attrs-all` por encima de 0, empezarán a contarse
bolsas y caras como si fueran pasajeros.** El orden de las clases está en
`peoplenet/labels.txt` (3 líneas).

Bajar el umbral de la clase 0 a `0.20` detecta ~30% más, a costa de más falsos
positivos. Está en 0.30 porque era el umbral con el que se comparó contra
YOLO26 (sección 4.2ter).

**Aspecto de la imagen.** El `nvinfer_config.txt` oficial de NVIDIA trae
`maintain-aspect-ratio=0`, pensando en cámaras 16:9. La nuestra es 4:3 (640×480),
así que sin corregirlo la imagen se estira a 960×544 y las personas se deforman.
Medido: preservar la forma sube de 0.41 a 0.51 detecciones por frame (**+24%**),
sin costar FPS. La copia oficial de NVIDIA se conserva en
`peoplenet/nvinfer_config.txt` como referencia; **no es la que se carga**.

**Trampa del nombre del `.engine`.** DeepStream escribe el motor con *su*
convención (`<onnx>_b<batch>_gpu<id>_<modo>.engine`) e ignora el
`model-engine-file` para escribir, pero **sí lo usa para leer**. Si los dos no
coinciden, reconstruye el motor en cada arranque: **95 segundos perdidos cada
vez**. El config ya apunta al nombre correcto; comprobado, el motor se
deserializa en 0.5 s.

**Por qué el ONNX y no el `.etlt`.** Las versiones `pruned_quantized` de
PeopleNet vienen en `.etlt` con formato UFF, y **TensorRT 10 —el que trae
DeepStream 7.1— eliminó el parser UFF** (`libnvparsers` ya no existe en el
sistema). La única vía en esta plataforma es la versión
`deployable_quantized_onnx_v2.6.3`. Un `.etlt` descargado de NGC **no va a
funcionar aquí**, por más que la documentación de NVIDIA lo siga mostrando.

El motor `.engine` ya existe. Si se borra, DeepStream lo reconstruye desde el
ONNX en el primer arranque, y eso **tarda varios minutos**. Nunca interrumpir esa
primera construcción: deja el archivo corrupto.

### 4.2bis YOLO26: retirado, no borrado

**Decisión del 29-ago-2026: el proyecto deja de usar YOLO y todo lo de
ultralytics.** Nada se borró —los archivos, el parser compilado y el repositorio
siguen en disco— pero ya no se ejecutan.

Qué cambió exactamente, por si hay que revertirlo:

| Archivo | Antes | Ahora |
|---|---|---|
| `contador_neto.py` | `MODELO` por omisión `yolo26` | `peoplenet` |
| `variables.py` | Banner decía `yolo26` | `peoplenet` |
| `contador_p2p_v11.py` | `CONFIG_INFER` fijo a YOLO26 | PeopleNet (sección 9.1) |
| `deepstream_app_config.txt` | `config-file=...yolo26.txt` | `...peoplenet.txt` |

El selector sigue existiendo, así que se puede cargar YOLO26 a mano **solo para
comparar contra las mediciones históricas**:

```bash
python3 contador_neto.py                 # peoplenet, lo normal
MODELO=yolo26 python3 contador_neto.py   # retirado; solo para comparar
```

Funciona porque el pipeline **no sabe qué modelo corre**: todo lo específico
(parser, `.so`, número de clases, umbrales) vive en el `.txt` de nvinfer, y el
código solo elige cuál cargar. **Y la clase 0 es `person` en los dos** —COCO y
PeopleNet coinciden— así que `config_nvdsanalytics.txt`, las líneas de conteo y
el tracker **no se tocan** al cambiar de modelo. Eso es lo que hizo barato el
cambio.

| | PeopleNet (en uso) | YOLO26s (retirado) |
|---|---|---|
| Formato | ONNX → INT8 | ONNX → FP16 |
| Clases | 3 (person, bag, face) | 80 (COCO) |
| Entrada | 960×544 **fija** | 640×480 |
| Parser | nativo de DeepStream | propio (`libnvdsinfer_custom_impl_Yolo.so`) |
| Motor | 89 MB | 22 MB |
| Origen del modelo | NGC (NVIDIA TAO) | Exportado con `ultralytics/export_yolo26.py` |

Lo que queda en disco sin usarse, y que **no hay que borrar**: `yolo26s.onnx` +
`.data`, `model_b1_gpu0_fp16.engine`, `labels.txt` (80 clases COCO),
`nvdsinfer_custom_impl_Yolo/` con su `.so` y sus fuentes, `config_infer_primary_yolo26.txt`,
el repositorio `cit/ultralytics/` y el entorno `cit/yolo_env/`.

### 4.2ter Medir el rendimiento

El contador mide sus propios FPS sobre los buffers que llegan al OSD —el final
del pipeline—, así que refleja lo que sale por pantalla, no lo que produce la
cámara. Se ve arriba a la derecha y se resume al salir:

```
Rendimiento: peoplenet :: 20.0 FPS de promedio, 0.51 detecciones por frame (867 frames)
```

Las **detecciones por frame** acompañan al FPS a propósito: comparar dos modelos
solo por FPS engaña, porque uno puede ir rapidísimo justamente porque no detecta
nada.

| Variable | Default | Para qué |
|---|---|---|
| `MODELO` | `peoplenet` | `yolo26` carga el detector retirado (solo comparar) |
| `ETIQUETAS` | `0` | `1` muestra clase, ID y confianza sobre cada caja. Para **evaluar** un modelo |
| `FPS_EN_PANTALLA` | `1` | `0` lo oculta |
| `MOSTRAR_LINEA` | `1` | Dibuja la línea de conteo en verde CIT sobre el video |

**Medición del 25-ago-2026** (video de prueba, 1642 frames). Es la comparativa
que llevó a adoptar PeopleNet; se conserva como registro, no como guía de
configuración:

| Configuración | FPS | Detecciones/frame |
|---|---|---|
| YOLO26 @0.30 | 20.0 | **1.18** |
| PeopleNet @0.40, oficial NVIDIA | 19.9 | 0.32 |
| PeopleNet @0.30 | 20.0 | 0.41 |
| PeopleNet @0.30 + aspecto preservado | 20.0 | 0.51 |
| PeopleNet @0.20 + aspecto preservado | 20.0 | 0.67 |

Dos lecturas importantes de esta tabla:

1. **El rendimiento es idéntico.** Los 20 FPS no los limita el modelo, sino algo
   más adelante en el pipeline. Cambiar de red no cuesta velocidad.
2. **Más detecciones no significa mejor.** Sin *ground truth*, esta columna mide
   sensibilidad, no acierto: YOLO26 podría estar detectando de más. El juicio
   definitivo es visual, con `ETIQUETAS=1` y gente real delante de la cámara.
   Además el video de prueba es 1254×632 y el muxer lo deforma a 640×480, cosa
   que no pasa con la cámara real.

### 4.2quater La línea de conteo en pantalla

La línea se dibuja sobre el video **leyendo sus coordenadas de
`config_nvdsanalytics.txt`**, no escritas a mano en el código. Así no pueden
quedar en desacuerdo: si alguien mueve la línea en el config, la pantalla lo
refleja sin tocar nada.

Detalle que importa: con `extended=1` nvdsanalytics trata la línea como
**infinita**, así que dibujar solo el segmento del config (y=100 a y=260)
engañaría — se cuenta también fuera de él. Por eso se dibuja la recta completa
de borde a borde.

El **sentido** (las etiquetas `entrada`/`salida` con su flecha) no se dibuja
desde Python: ya lo pone `nvdsanalytics` con `osd-mode=1`, y mejor situado,
pegado a la línea. Duplicarlo solo ensuciaba la imagen.

Lo que se ve en la ventana:

```
AFORO: 0                    peoplenet  20.0 FPS  det 3
        │
        │  ←──→  entrada        ← la línea, en verde CIT
        │        salida
```

### 4.3 El seguimiento — `config_tracker_NvDCF.yml`

Librería: `/opt/nvidia/deepstream/deepstream/lib/libnvds_nvmultiobjecttracker.so`
Resolución de trabajo: 640x384 (`tracker-width` / `tracker-height`).

Valores que más afectan al conteo:

| Parámetro | Valor | Efecto |
|---|---|---|
| `minDetectorConfidence` | 0.0430 | Por debajo de esto, la detección se ignora |
| `minTrackerConfidence` | 0.4009 | Por debajo, el objeto pasa a "shadow mode" |
| `probationAge` | 2 | Frames antes de dar un track por válido |
| `maxShadowTrackingAge` | 51 | Cuánto sobrevive un track sin detección |
| `maxTargetsPerStream` | 150 | Máximo de personas simultáneas |

El tracker es lo que permite **no contar dos veces a la misma persona**: cada
una lleva un ID estable y `nvdsanalytics` deduplica por ese ID.

`display-tracking-id` se pone a `0` cuando `OCULTAR_ETIQUETAS = True`, para que
el número del track no se dibuje encima de cada persona.

### 4.4 El conteo — `config_nvdsanalytics.txt`

```ini
[property]
enable=1
config-width=640           # DEBE coincidir con el ancho del nvstreammux
config-height=480          # DEBE coincidir con el alto del nvstreammux
osd-mode=1
display-font-size=12

[line-crossing-stream-0]
enable=1
line-crossing-entrada=280;240;360;240;320;100;320;260
line-crossing-salida=360;240;280;240;320;100;320;360
class-id=0                 # solo personas
extended=1
mode=loose
```

**Cómo leer esos ocho números.** El formato de `nvdsanalytics` es:

```
line-crossing-<etiqueta> = dx1;dy1;dx2;dy2 ; lx1;ly1;lx2;ly2
                           └── dirección ──┘ └─── la línea ───┘
```

Los primeros cuatro definen un vector de **dirección**: solo se cuenta a quien
cruce en ese sentido. Los últimos cuatro son el **segmento** que hay que cruzar.

Traducido:

| Etiqueta | Dirección | Segmento | Sentido |
|---|---|---|---|
| `entrada` | (280,240) → (360,240) | vertical en x=320, de y=100 a y=**260** | izquierda → derecha |
| `salida` | (360,240) → (280,240) | vertical en x=320, de y=100 a y=**360** | derecha → izquierda |

Es decir: **una sola línea física vertical en el centro de la imagen** (x=320),
declarada dos veces con direcciones opuestas.

> ℹ️ Desde el 29-ago-2026 el contador **solo lee la línea `entrada`** y deduce
> la salida como el cruce en sentido contrario (sección 4.5bis), así que el
> aviso de abajo ya no puede afectar al aforo. Se conserva porque sigue
> aplicando a lo que `nvdsanalytics` dibuja.
>
> ⚠️ **Asimetría latente en los segmentos.** Los dos no miden lo mismo:
> `entrada` cubre de y=100 a y=**260**, y `salida` de y=100 a y=**360**.
> Ahora mismo **no causa problema** porque `extended=1` alarga ambas líneas
> hasta el infinito y las dos acaban siendo la misma recta vertical x=320.
> Pero si alguien pone `extended=0`, la diferencia se vuelve real de golpe:
> quien cruce por debajo de y=260 contaría como salida pero no como entrada, y
> el aforo derivaría hacia abajo. Lo sano es igualar el último valor de ambas
> antes de tocar `extended`.

Las etiquetas `entrada` y `salida` **son la interfaz** con el código Python:
tienen que coincidir con `ETIQUETA_ENTRADA` y `ETIQUETA_SALIDA` en
`contador_neto.py`. Si se renombran aquí, hay que renombrarlas allí.

**`extended`** (según el ejemplo oficial de NVIDIA en
`/opt/nvidia/deepstream/deepstream/sources/apps/sample_apps/deepstream-nvdsanalytics-test/`):

- `0` — solo cuenta los cruces que ocurren **sobre el segmento configurado**.
- `1` — trata la línea como **infinita** y cuenta todos los cruces. Es el valor
  que usamos.

**`mode`** controla cuánta rigidez se exige en la dirección:

- `loose` — cuenta todos los cruces **sin adherencia estricta a la dirección**.
  Es el valor que usamos.
- `balanced` — más estricto que `loose`.
- `strict` — el más estricto.

`mode` ya **no influye en el aforo**: solo afecta a lo que `nvdsanalytics`
cuenta y dibuja, y su cuenta se ignora desde 4.5bis. Se deja en `loose`.

### 4.5 La sonda: de dónde sale el número

> En `contador_p2p_v15.py` la sonda se llama `sonda_analitica()` y va en el
> **src pad de `nvdsanalytics`**, antes de la válvula de video. Hace lo mismo y
> además publica cada cruce como evento `cruce`.

La función `sonda_osd()` está enganchada al **sink pad del `nvdsosd`**, así que
se ejecuta una vez por buffer, justo antes de dibujar. Hace cuatro cosas:

1. **Recorre las detecciones** una sola vez y, de esa pasada, saca tres cosas:
   cuántas hay (para la media de detecciones por frame), dónde está el centro
   de cada persona con su **ID de track**, y —si `OCULTAR_ETIQUETAS`— les borra
   el texto.

2. **Detecta los cruces**, con `_contar_cruces()`. Esta es la parte que en
   agosto de 2026 se dejó de delegar en `nvdsanalytics`; el porqué está en
   4.5bis. Cada cruce se imprime al log con su ID:

   ```
   CRUCE entrada  id=42
   CRUCE salida   id=42
   CRUCE entrada  id=42      ← el mismo ID vuelve a contar
   ```

3. **Dibuja el aforo** con `nvds_acquire_display_meta_from_pool()`, en verde CIT
   (`0.0, 1.0, 0.42`) sobre fondo negro semitransparente.

4. **Publica al bus** solo cuando el número cambia (no en cada frame).

> ⚠️ **Trampa documentada en el propio código:** no poner `display-text=0` en el
> `nvdsosd`. Esa propiedad apaga **todo** el texto, incluido el contador de
> aforo. Las etiquetas de los objetos se borran en la sonda, no ahí.

### 4.5bis El conteo propio, y el fallo que lo obligó

**El fallo.** Hasta el 29-ago-2026 el aforo salía de `objLCCumCnt`, el acumulado
que publica `nvdsanalytics`. En la unidad se observó esto:

| Acción | Aforo | |
|---|---|---|
| La persona cruza hacia dentro | +1 | correcto |
| La misma persona cruza hacia fuera | −1 | correcto |
| La misma persona vuelve a cruzar hacia dentro | **sin cambio** | ❌ |
| …y cualquier cruce posterior | **sin cambio** | ❌ |

Para que volviera a contar había que **salir del encuadre** y esperar a que el
tracker asignara un ID nuevo. En una puerta real es inaceptable: quien se asoma
y vuelve deja de existir para el sistema el resto del viaje.

**La causa.** `nvdsanalytics` cuenta cada track ID **una sola vez por línea**.
El primer cruce de `entrada` gasta el ID para `entrada`; el primer cruce de
`salida` lo gasta para `salida`. A partir de ahí ese ID no puede sumar nada más,
y como las dos líneas son la misma recta, bastan dos cruces para dejarlo mudo.

**No se puede configurar.** Verificado sobre el binario del plugin: las únicas
claves que acepta son `enable`, `line-crossing-<etiqueta>`, `class-id`,
`extended`, `mode` y `object-threshold`. Ninguna cambia ese comportamiento, y la
librería es cerrada. `mode=loose|balanced|strict` **no** tiene que ver con esto:
regula la adherencia a la dirección, no el recuento por ID.

**La solución.** El cruce se calcula en `contador_neto.py`, con el ID del tracker
y **la misma línea que ya está en `config_nvdsanalytics.txt`** — el config sigue
mandando, el código solo lo aplica. Para cada persona con ID:

1. Se calcula la **distancia con signo** del centro de su caja a la línea. El
   signo dice de qué lado está.
2. Si esa distancia es menor que `BANDA` (8 px por omisión) se considera que
   está *pisando* la línea y **no se decide nada**. Esta franja muerta es lo que
   evita que una caja que tiembla sobre la línea dispare una ristra de cruces
   falsos alternos.
3. Cuando el lado cambia respecto al frame anterior, **eso es un cruce**. Un ID
   puede cruzar tantas veces como quiera.
4. Si es entrada o salida lo decide la **flecha de dirección del config**, no un
   "izquierda/derecha" escrito en el código: se compara el sentido del cruce con
   el vector `dx1;dy1;dx2;dy2`. Si alguien invierte la flecha en el `.txt`, el
   conteo se invierte solo.
5. Con `extended=0` se comprueba además que el cruce ocurra **sobre el segmento**
   dibujado y no sobre su prolongación. Con `extended=1` (lo actual) la recta es
   infinita, igual que hacía `nvdsanalytics`.
6. Los tracks que llevan más de 5 s sin aparecer se olvidan, para que la memoria
   no crezca en una sesión de horas.

Efecto secundario que conviene conocer: **el contador solo lee la línea
`entrada`**. La `salida` se deduce como el cruce en sentido contrario, así que la
asimetría entre ambas que se documenta en 4.4 ya no puede afectar al conteo. La
línea `salida` sigue en el config porque `nvdsanalytics` la usa para dibujar su
etiqueta.

`nvdsanalytics` **sigue en el pipeline**, pero ya solo por lo que dibuja con
`osd-mode=1` (las etiquetas `entrada`/`salida` pegadas a la línea, útiles para
alinear la cámara). Su cuenta se ignora.

**Cómo comprobar que está arreglado, sin cámara:**

```bash
cd DeepStream-Yolo
python3 -c "
import importlib.util, sys
spec = importlib.util.spec_from_file_location('cn', 'contador_neto.py')
cn = importlib.util.module_from_spec(spec); sys.argv=['x']; spec.loader.exec_module(cn)
geo = cn._preparar_conteo(cn._leer_linea_de_conteo())
t = 0.0
for vuelta in range(3):
    for x in list(range(220,421,20)) + list(range(420,219,-20)):
        t += 0.033
        for ident, et in cn._contar_cruces(geo, [(42, float(x), 240.0)], t):
            print('CRUCE', et, 'id=', ident)
print('entradas', cn._entradas, 'salidas', cn._salidas)
"
```

Debe imprimir 3 entradas y 3 salidas, todas con `id=42`. Con el método anterior
habría dado una de cada.

**En vivo:** camina cruzando la línea de ida y vuelta varias veces sin salir del
encuadre. En `datos/contador.log` deben aparecer los `CRUCE` con **el mismo
`id=`** repetido, y el aforo debe subir y bajar en cada pasada.

### 4.6 Parámetros ajustables (cabecera del archivo)

| Constante | Default | Qué hace |
|---|---|---|
| `ANCHO`, `ALTO`, `FPS` | 640, 480, 30 | Deben cuadrar con `config_nvdsanalytics.txt` |
| `ETIQUETA_ENTRADA/SALIDA` | `entrada` / `salida` | Deben cuadrar con el config de analytics |
| `MOSTRAR_DESGLOSE` | `False` | Añade "E: n  S: m" bajo el aforo |
| `OCULTAR_ETIQUETAS` | `True` | Quita el texto sobre cada persona. Se invierte con `ETIQUETAS=1` |
| `MOSTRAR_FPS` | `True` | Se apaga con `FPS_EN_PANTALLA=0` |
| `MOSTRAR_LINEA` | `True` | Se apaga con `MOSTRAR_LINEA=0` |
| `MOSTRAR_CAJAS` | `True` | Dibuja o no los recuadros |
| `TAM_FUENTE_AFORO` | 28 | |
| `BANDA` (env `BANDA_CRUCE`) | 8 px | Franja muerta a cada lado de la línea. Subirla si una caja temblorosa genera cruces falsos |
| `OLVIDO` | 5.0 s | Cuánto se recuerda un track que dejó de verse |
| `PERMITIR_NEGATIVO` | `True` | Si es `False`, el aforo no baja de 0 |
| `IMPRIMIR_EN_TERMINAL` | `True` | Imprime en su log cada cambio |
| `ORIGEN` (env) | `camion01` | Nombre con el que se anuncia en el bus |

---

## 4ter. Página y video bajo demanda (`contador_p2p_v15.py`, modo de reserva)

### 4ter.1 El final del pipeline

Hasta `nvdsanalytics` es idéntico a 4.1. De ahí en adelante:

```
nvdsanalytics (analitica)
  │   ◄── SONDA sonda_analitica(): cuenta y publica. Siempre corre.
valve (valvula-video)             drop=True mientras nadie mira
  │
nvvideoconvert → nvdsosd          superposicion solo con DIAGNOSTICO=1
  │
nvvideoconvert → caps I420
  │
jpegenc (quality=CALIDAD_JPEG)    por CPU: la Orin Nano no tiene NVENC
  │
appsink (max-buffers=1, drop)     entrega cada JPEG a Python
  │
_cuadro  ◄── deposito del ULTIMO cuadro; los navegadores leen de aqui
```

La válvula es lo que hace barato el video: **con la válvula cerrada no se
dibuja, no se convierte y no se comprime**, pero el conteo no se entera porque
su sonda está antes. Al cerrar la sesión se tira el último cuadro
(`olvidar_cuadro()`), para que nadie vea luego una imagen vieja como si fuera
de ahora.

### 4ter.2 Rutas del servidor web (`:8080`)

Todas exigen la clave: `?k=CLAVE` en la URL, o la cookie `cit=` que deja la
página al abrirse (8 h). Comparación en tiempo constante.

| Método | Ruta | Qué hace |
|---|---|---|
| GET | `/` | La página (aforo grande, posición, cuota, botón *Ver video*) |
| GET | `/estado` (o `/aforo`) | JSON: `aforo`, `entradas`, `salidas`, `topes_en_cero`, `unidad`, `unidad_id`, `ts_utc`, `video{…}`, `gps{…}` |
| POST | `/video/abrir` | Abre la válvula por `VENTANA_S` (o extiende). Falla si se agotó la cuota |
| POST | `/video/cerrar` | La cierra y descuenta lo consumido |
| GET | `/video.mjpg` | Flujo MJPEG continuo (`multipart/x-mixed-replace`) a `FPS_VIDEO`. Abre la sesión si no lo estaba |
| GET | `/instantanea.jpg` | Un solo JPEG. **No** abre sesión (responde 409 si está cerrada) |

Al oprimir *Ver video*, la página hace `POST /video/abrir` y carga
`/video.mjpg` en un `<img>`. Si en **6 s** no llega ningún cuadro (algún
intermediario retiene los flujos que no terminan), pasa sola a pedir
`/instantanea.jpg` cada 400 ms. El estado (`/estado`) se consulta cada 2 s.

### 4ter.3 Ventana y cuota, contadas en la Jetson

`ControlVideo` lleva la cuenta **en la unidad**, no en el navegador: si la
llevara el navegador, cualquiera la desactivaría desde la consola, y quien mira
no es quien paga los datos celulares.

| Variable | Default | Qué hace |
|---|---|---|
| `VENTANA_S` | 60 | Duración de cada sesión. El botón *Extender* la renueva |
| `TOPE_DIARIO_S` | 1200 | Cuota diaria por unidad (20 min). Se reinicia a medianoche **UTC** |
| `BAJO_DEMANDA` | 1 | `0` deja la válvula abierta siempre |
| `FPS_VIDEO` | 8 | Cuadros/s hacia el navegador |
| `CALIDAD_JPEG` | 60 | ~18 KB por cuadro a 640×480 → ~1.2 Mbps mientras alguien mira |

La cuota **no sobrevive a un reinicio** del contador: vive en memoria.

### 4ter.4 Otras variables del contador

| Variable | Default | Qué hace |
|---|---|---|
| `UNIDAD` / `UNIDAD_ID` | `Unidad de prueba` / `cit-001` | Nombre visible / identificador |
| `CLAVE` | aleatoria | Si no se da, se genera y se imprime al arrancar |
| `PUERTO_WEB` / `ESCUCHAR` | `8080` / `0.0.0.0` | Servidor web |
| `ORIGEN` | `camion01` | Origen en el bus |
| `PUBLICAR_CRUCE` | 1 | `0` deja de publicar los eventos `cruce` |
| `SIN_GPS` | 0 | `1` no carga `gps_edge.py`. `cit.sh` siempre lo pone |
| `DIAGNOSTICO` | 0 | `1` enciende línea, número de aforo y cajas sobre el video |
| `CAJAS`, `MOSTRAR_LINEA`, `AFORO_EN_VIDEO` | 0 | Encienden cada superposición por separado |
| `MODELO` | `peoplenet` | `yolo26` retirado (4.2bis) |
| `BANDA_CRUCE` | 8 | Franja muerta en px (4.5bis) |
| argumento 1 | `0` | Número de cámara: `python3 contador_p2p_v15.py 1` → `/dev/video1` |

### 4ter.5 Por qué MJPEG y no WebRTC (historia de v1.1 a v1.5)

| Versión | Fecha | Video | Qué pasó |
|---|---|---|---|
| v1.1 (`_v11`) | 18-ago | x264 → RTSP interno → go2rtc → WebRTC | Clave obligatoria, puente a go2rtc por lista blanca, válvula |
| v1.2 (`_v12`) | 07-sep | igual | Conteo propio por track ID, PeopleNet, **publica al bus**, `SIN_GPS`, video limpio. Desde aquí es el único contador de producción |
| v1.3 (`_v13`) | 08-sep | igual, con `rtph264pay` real | go2rtc no leía el SDP incompleto |
| v1.4 (`_v14`) | 09-sep | `rtspclientsink` empuja a go2rtc `:8554` | La válvula tuvo que quedarse abierta: el codificador corría 24 h |
| **v1.5 (`_v15`)** | 09-sep | **MJPEG por `:8080`, sin go2rtc** | WebRTC necesita que el navegador alcance la Jetson en `:8555`; detrás de NAT doble del proveedor no se logra sin un servidor TURN de pago |
| v1.6 (`_v16`) | 22-sep | H.264 por SRT a MediaMTX en la VM, bajo orden | Cada orden renovaba a 60 s; cuota diaria en memoria. Sustituida por la v1.7 |
| **v1.7 (`_v17`)** | 24-sep | igual que v1.6 | **ADR-001**: sesión sin renovación, duración pedida, cuota en sesiones al mes en disco (sección 4quinquies). Contador del modo VM |

Ganancia de la v1.5: funciona en cualquier red, por el mismo túnel HTTPS, sin
costo de nube, y la válvula vuelve a apagar el codificador. Pérdida: el video
ya no es punto a punto (pasa por Cloudflare) y MJPEG gasta más datos que H264.
La v1.6 recupera H.264 y quita el túnel: la Jetson empuja por SRT a la VM, que
es quien reparte.

---

## 4quinquies. Sesión de video acotada (`contador_p2p_v17.py`, ADR-001)

Es el contador del modo VM. Cuenta **exactamente igual** que la v15 (misma
sonda antes de la válvula, mismo conteo por track ID) y el final del pipeline
es el de la v16: válvula → OSD → sonda de ritmo (8 c/s) → `x264enc` →
`mpegtsmux` → UDP local `:5600` → proceso aparte `gst-launch … srtsink` hacia
MediaMTX. No hay página web: el HTTP escucha en `127.0.0.1:8080` y solo lo usa
`agente.py`, con la clave que genera `cit.sh` en cada arranque
(`datos/sesion.env`).

Lo que cambia la v1.7 respecto de la v1.6, por el ADR-001:

| Regla | Cómo se aplica |
|---|---|
| Sesión abierta solo por acción explícita | `POST /video/abrir` que llega del comando `media.open` |
| Duración acotada | Cuerpo JSON `{"segundos","camara","solicitante","orden"}`; `segundos` ≤ `VENTANA_S` (tope duro 60) |
| **Cierre forzado por el equipo** | Temporizador propio (`_vigilar`, cada 0.5 s). Corta aunque la VM o el navegador desaparezcan |
| **Sin renovación** | Pedir con la sesión abierta → `sesion en curso, restan N s`. La v1.6 renovaba |
| Tope en sesiones al mes | `TOPE_SESIONES_MES` (100 provisional). Se cuenta **antes** de abrir y se guarda con `fsync` en `datos/cuota_video.json`; mes UTC. Sustituye a la cuota diaria en segundos, que vivía en memoria |
| Estado reportado | `/estado` → `video{abierta, restan_s, camara, solicitante, orden, sesiones_restantes, srt…}` y `camara{id, activa, edad_s}` |
| Reposo de medios | Sin sesión, válvula cerrada y **ningún** proceso `srtsink`: cero tráfico de video |

Variables propias (además de las de 4ter.4 que siguen aplicando):
`SRT_DESTINO`, `SRT_STREAMID`, `SRT_PASSPHRASE`, `SRT_LATENCIA_MS` (300),
`PUERTO_UDP_LOCAL` (5600), `BITRATE` (400), `KEYFRAME_S` (2), `VENTANA_S`
(60), `TOPE_SESIONES_MES` (100), `CAMARA_ID` (`cam0`). `cit.sh` las pasa
desde `unidad.env`.

Cada apertura y cierre queda en el bus como `tipo: video` (con solicitante y
orden). Eso es bitácora local; a la VM llega por `media_session` en la
telemetría y por el `resultado` del comando.

---

## 4bis. El GPS: `lector_gps.py` + `cit_core/`

Hermano del contador: un productor que solo publica al bus. El hardware es una
tarjeta **Waveshare SIM7600G-H 4G for Jetson Nano** (módulo SimTech `1e0e`).

### 4bis.1 Hecho clave: el GPS no necesita SIM

El GNSS del SIM7600G-H opera en **modo standalone**: demodula los datos
directamente de los satélites. No requiere tarjeta SIM, no requiere registro en
red y no consume datos. La SIM solo intervendría para A-GNSS, que únicamente
acelera el primer fix y **no mejora la precisión**.

Aviso de hardware que cuesta horas si no se sabe: **la tarjeta no se alimenta
por el micro USB**. El micro USB es solo datos; la energía entra por el header
de 40 pines. Si el LED PWR está apagado, no hay nada que depurar en software.

### 4bis.2 Reparto en cuatro archivos

| Archivo | Responsabilidad |
|---|---|
| `cit_core/serie.py` | Abre un `/dev/ttyUSB*` y lee líneas. Sustituye a `pyserial` |
| `cit_core/nmea.py` | Traduce las sentencias NMEA a números. Funciones puras |
| `cit_core/modulo_gps.py` | **Encuentra** el módulo y enciende el GNSS |
| `lector_gps.py` | El bucle: leer, decidir si publicar, publicar |

**Por qué no `pyserial`:** el sistema no tiene ninguna dependencia de `pip`
obligatoria y se quiere que siga así. Todo lo que hace `pyserial` para este caso
son cuatro llamadas a `termios`, que está en la librería estándar.

### 4bis.3 La detección del puerto, que es el corazón del asunto

El módulo entrega cinco puertos `/dev/ttyUSB0..4` y los que importan son dos.
**Su número no es fijo**: depende del orden de arranque, de si hay otro USB
serie conectado y del puerto físico donde se enchufe. Dar por hecho que el GPS
está en `/dev/ttyUSB1` funciona en el banco y falla en la unidad.

Lo que **no** cambia es el número de interfaz USB dentro del módulo:

| Interfaz USB | Función |
|---|---|
| **03** | Flujo NMEA continuo — de aquí salen las coordenadas |
| **04** | Comandos AT — por aquí se enciende el GNSS |

Por eso se busca **por interfaz, nunca por nombre de dispositivo**. Tres
niveles, cada uno cubriendo el fallo del anterior:

1. **sysfs** — recorre `/sys/bus/usb-serial/devices/*`, sube al padre USB y
   empareja `idVendor` con `bInterfaceNumber`. Instantáneo. Es el caso normal.
2. **sondeo** — abre cada `/dev/ttyUSB*` y `/dev/ttyACM*` y pregunta: ¿sale
   NMEA por aquí? ¿responde `OK` a un `AT`? Cubre un módulo con otro VID.
3. **entorno** — `CIT_GPS_NMEA` / `CIT_GPS_AT` saltan toda la detección.

No se filtra por PID a propósito. El módulo aparece como `9011` (composición
RNDIS) o `9001` (estándar) según cómo esté configurado, y **en ambas las
funciones serie caen en las mismas interfaces**. Filtrar por PID sería volver a
atarse a algo que el módulo puede cambiar solo.

Comprobar la detección sin arrancar el sistema entero:

```bash
python3 -m cit_core.modulo_gps      # dice qué puertos encontró y por qué nivel
python3 -m cit_core.nmea            # prueba la conversión de coordenadas
```

### 4bis.4 ModemManager ocupa el puerto AT

En esta Jetson, **`ModemManager` está activo** y toma el SIM7600 al detectarlo,
dejando `/dev/ttyUSB2` (AT) y `/dev/ttyUSB3` (PPP) con *Device or resource busy*.

**No estorba**, y por eso no se desactiva: el flujo NMEA sale por `ttyUSB1`, que
sí queda libre, y el módulo ya tiene `AT+CGPSAUTO=1` grabado, así que el GNSS
arranca solo. **El puerto AT es un lujo, no un requisito.** El lector avisa en
su log y sigue leyendo.

Solo si hiciera falta mandar comandos AT (por ejemplo, si el GNSS apareciera
apagado):

```bash
sudo systemctl disable --now ModemManager
# o excluir el modulo con una regla de udev:  ENV{ID_MM_DEVICE_IGNORE}="1"
```

**Ventana al reconectar.** Cuando el módulo se reenumera, ModemManager lo sondea
y durante unos segundos ocupa **también el puerto NMEA**, no solo el AT. El
lector lo ve como *Device or resource busy*, reintenta y se recupera solo. Está
observado y probado; no hay que hacer nada, pero explica un par de líneas de
error en el log justo después de una reconexión.

### 4bis.4bis El módulo se reinicia solo. Riesgo de alimentación

Observado el 19-ago-2026 durante las pruebas: el módulo **desapareció del USB y
volvió unos 60 s después** por sí solo (el número de dispositivo pasó de 6 a 9 y
el hub Terminus se reenumeró también, señal de que fue la tarjeta entera la que
perdió alimentación, no solo el módulo).

La causa probable es la conocida del fabricante: el SIM7600 pide **hasta 2 A de
pico** y colgarlo del riel de 5 V del header del Jetson no siempre da abasto.
Mitigación pendiente en hardware: **riel dedicado de 5 V con capacidad de 2 A**.

El software ya está preparado para esto —el lector se recuperó solo, sin tocar
nada, y el conteo de aforo no se enteró— pero **es un problema físico sin
resolver**, no algo que el software deba tapar. En marcha, una unidad puede
perder posición durante ese minuto.

### 4bis.5 Conversión de coordenadas

El NMEA **no da grados decimales**: da grados pegados a minutos
(`ddmm.mmmm`). La conversión vive en `cit_core/nmea.py`:

```
grados_decimales = grados + (minutos / 60)      negativo si el hemisferio es S o W

1901.722343 N   ->  19 + (1.722343 / 60)   =   19.0287057
09814.892331 W  ->  98 + (14.892331 / 60)  =  -98.2482055
```

De todo el flujo solo interesan dos sentencias: **GGA** (posición, altitud,
satélites, HDOP, calidad) y **RMC** (velocidad y rumbo). El resto —`GSV` con los
satélites a la vista, `PQXFI` propietaria de Qualcomm— se descarta.

Dos detalles que evitan publicar datos falsos:

- **Se valida el checksum `*XX` de cada sentencia.** Una línea corrupta por
  ruido en el USB puede traer coordenadas con un dígito cambiado, y eso da un
  punto en mitad del mar sin que nada más lo delate.
- **Calidad `0` = sin fix.** Los campos de posición pueden seguir trayendo el
  último valor conocido, así que se descartan explícitamente.

El prefijo de la sentencia cambia con la constelación (`$GP` GPS, `$GL` GLONASS,
`$BD` BeiDou, `$GN` combinada), por eso se compara **siempre por las tres letras
finales**, nunca por la sentencia entera.

### 4bis.6 Cada cuánto se publica

El módulo da 1 Hz, pero publicar cada segundo llenaría `eventos.jsonl` con la
misma coordenada (~86 000 líneas al día estando parado). La regla es:

- cada **5 s** (`GPS_INTERVALO`), o
- **al momento** si la unidad se movió más de **10 m** (`GPS_METROS`), o
- **al momento** si se gana o se pierde el fix, que es un cambio de estado.

Eso es lo que va al **bus** (y a AWS, si se enciende). Aparte, desde el
24-sep-2026, **cada lectura** (≈1 Hz) se sobrescribe en
`/dev/shm/cit/gps_actual.json` (`CIT_GPS_ACTUAL`), un archivo en memoria que
no gasta disco ni infla el historial. De ahí lee `agente.py` para mandar la
posición a 1 Hz con la unidad en marcha, como pide el ADR-001.

### 4bis.7 Blindajes: por qué el GPS no puede tumbar el sistema

`variables.py` apaga todo si un hijo muere. Si el lector de GPS se cayera al
desenchufar un cable, **se llevaría por delante el conteo de aforo**, que es lo
importante. De ahí estas cuatro decisiones:

1. **El lector nunca muere por un fallo de hardware.** Sin módulo, sin fix o con
   el USB desenchufado, publica `{"fix": false, "motivo": "..."}`, espera con
   reintento creciente (2 s → 30 s) y **vuelve a ejecutar la detección completa**
   — porque al reconectar puede aparecer con otro `ttyUSB`.
2. **El GPS está marcado `"critico": False`** en `PROGRAMAS`. Si aun así muriera,
   `variables.py` avisa en pantalla y **el resto sigue funcionando**.
3. **Candado `datos/gps.lock`** (`flock`). Dos procesos leyendo el mismo puerto
   serie no dan error: se reparten los bytes al azar y ambos ven sentencias
   partidas por la mitad. El síntoma —posiciones que aparecen y desaparecen
   solas— no se parece en nada a la causa, así que se hace imposible.
4. **Puerto AT opcional.** Que esté ocupado no impide leer la posición.

Si la unidad no lleva módulo, `SIN_GPS=1` evita lanzar el lector.

> **En producción nadie vigila al lector.** Lo anterior describe `variables.py`.
> Con `cit.sh` cada proceso vive en su ventana de tmux y, si uno muere, **nadie
> lo reinicia** ni avisa: la ventana se queda en el prompt. Por eso la regla 1
> (el lector no muere nunca) importa todavía más allí.

### 4bis.8 El tiempo va en UTC. Regla obligatoria

El módulo reporta en **UTC** y el bus almacena en UTC (`bus.ahora()`). La
conversión a hora local (América/Ciudad de México) se hace **únicamente en la
capa de presentación**. Mezclar husos horarios en la ingesta produce reportes de
puntualidad incorrectos para el concesionario y para gobierno, y es una falla
muy difícil de detectar una vez que los datos están acumulados. Es una decisión
de arquitectura, no una preferencia de estilo.

### 4bis.9 Límites del hardware, a tener en cuenta al diseñar

| Aspecto | Valor real | Implicación |
|---|---|---|
| Tasa de actualización | 1 Hz | Más resolución temporal hay que predecirla en la nube |
| Precisión | 2.5 m CEP50 en cielo abierto | En cañón urbano se degrada. Validar en ruta real |
| TTFF en frío | ~35 s ideal, puede pasar de 1 min | Relevante en el arranque diario de la unidad |
| Antena | Solo banda L1 (1575.42 MHz) | Multiconstelación no rinde lo esperado con esta antena |
| Túneles y pasos a desnivel | Pérdida total de señal | El hueco se resuelve por software, no por antena |

Para saber si la búsqueda avanza, mirar el número de satélites que acompaña al
mensaje `sin posicion`. Si sube (0 → 3 → 6) el receptor está trabajando y solo
hay que esperar; si se queda clavado en 0 con cielo despejado, el problema está
en la antena, no en el tiempo.

Si no hay fix tras 5 minutos al aire libre, **el problema no es software**: el
cable debe estar en el u.FL rotulado **GNSS** (no en MAIN ni AUX, que son de
LTE) y la antena es **activa**, así que necesita 2.7–3.3 V DC de bias por el
mismo coaxial.

---

## 5. El bus: `cit_core/bus.py`

Capa intermedia que desacopla a quien produce datos de quien los consume.
**Es el único archivo que sabe cómo viajan los datos.** Cambiar el transporte
no obliga a tocar ni la visión ni el monitor.

### 5.1 La API, que es todo lo que usan los demás programas

```python
from cit_core.bus import Publicador, suscribir, descripcion

# Lado que produce
bus = Publicador("camion01")
bus.publicar("aforo", {"aforo": 7, "entradas": 9, "salidas": 2})
bus.cerrar()

# Lado que consume (BLOQUEA)
suscribir("cit/+/#", al_recibir)      # al_recibir(tema, datos)
```

**Regla de oro, respetada en todo el archivo:** si el bus falla, `publicar()`
devuelve `False` y sigue adelante. Nunca lanza excepción ni bloquea. El sistema
de visión jamás debe caerse porque nadie esté escuchando.

### 5.2 Temas

Nomenclatura estilo MQTT, idéntica en los dos transportes:

```
cit/<origen>/aforo      estado del aforo (solo cuando cambia)
cit/<origen>/cruce      cada cruce individual: direccion, id del track, aforo
cit/<origen>/gps        posicion de la unidad
cit/<origen>/estado     "en linea" / "desconectado"
```

Estos son los temas **del bus local**. Los de la nube son otros
(`cit/v1/<unidad>/<tipo>`) y los arma el reenviador (5bis).

`<origen>` identifica al programa o unidad. Los comodines `+` (un nivel) y `#`
(el resto) funcionan en `suscribir()`.

**Cada productor necesita su propio `<origen>`.** El contador usa `camion01` y
el GPS `gps01` (variable `ORIGEN_GPS`). No es cosmético: el archivo de estado es
`datos/estado/<origen>.json`, así que dos productores con el mismo origen se
sobrescribirían el estado el uno al otro.

Forma de un mensaje de GPS:

```json
{"ts":"2026-08-20T03:38:10Z","origen":"gps01","tipo":"gps","lat":19.0287402,
 "lon":-98.2481995,"alt":2124.3,"sats":8,"hdop":0.8,"vel":0.0,"rumbo":null,"fix":true}
```

Sin fix el mensaje se reduce, pero **conserva los satélites a la vista**:

```json
{"ts":"...","origen":"gps01","tipo":"gps","fix":false,"sats":3}
{"ts":"...","origen":"gps01","tipo":"gps","fix":false,"motivo":"sin modulo"}
```

Se publica igualmente **a propósito**: quien escuche tiene que poder distinguir
"no sé dónde está" de "está parado en el sitio".

Dos reglas sobre este mensaje:

- **`sats` sin `fix` no es un descuido, es el dato de diagnóstico.** Es lo único
  que separa "faltan segundos para enganchar" de "la antena no recibe nada", y
  sin él la única forma de decidir si esperar o ir a revisar el cable es
  adivinar. `variables.py` lo muestra: `sin posicion (buscando satelites: 3 a la vista)`.
- **Al perder el fix NO se arrastra la última coordenada.** Publicar una
  posición vieja junto a `fix:false` invita a que un consumidor la tome por
  buena. (Hasta el 29-ago-2026 el código sí la arrastraba, en contra de lo que
  decía su propio comentario.)

### 5.3 Transporte por archivo (el que está en uso)

Se elige con `CIT_TRANSPORTE=archivo`, que es **el valor por defecto**. No
necesita instalar nada.

**`datos/eventos.jsonl`** — registro que solo crece, una línea JSON por evento:

```json
{"ts":"2026-08-20T02:51:25Z","origen":"camion01","tipo":"aforo","aforo":0,"entradas":0,"salidas":0}
```

Se escribe abriendo en modo `a` y volcando la línea. En Linux, una escritura
corta en modo append es atómica, así que varios productores a la vez no se
pisan. **Efecto secundario útil: esto es también el historial en disco.**

**`datos/estado/<origen>.json`** — se **sobrescribe** cada 5 segundos
(`LATIDO`) mediante escritura atómica (`.tmp` + `os.replace`):

```json
{"ts":"2026-08-26T03:38:11Z","origen":"camion01","estado":"en linea",
 "aforo":1,"entradas":6,"salidas":5,"tipo":"aforo"}
```

Sirve para dos cosas:

- **Latido.** Si el archivo lleva más de 15 s (`PACIENCIA`) sin actualizarse,
  quien escucha da al productor por muerto aunque el archivo siga diciendo
  "en linea". Así se detecta un `SIGKILL`, que no deja despedirse.
- **Último valor conocido.** Un monitor que arranca tarde lee este archivo y
  muestra el aforo y la posición actuales de inmediato, sin esperar al siguiente
  cambio. Por eso el estado guarda también el campo `tipo` (`aforo`, `gps`): sin
  él habría que adivinar de qué variable es mirando los campos, y eso se rompe
  al añadir la tercera.

El heartbeat va aparte a propósito: si se escribiera en `eventos.jsonl`,
inflaría el historial con miles de líneas sin información.

**Cómo lee quien escucha:** se coloca al final de `eventos.jsonl` y lo sigue al
estilo `tail -f`, revisando cada 200 ms (`INTERVALO`). Maneja que el archivo aún
no exista, que lo hayan vaciado (si encoge, vuelve a empezar) y que una línea
llegue a medio escribir (JSON inválido → se ignora). Los archivos de estado se
repasan una vez por segundo.

### 5.4 Transporte MQTT (disponible, no en uso)

Se activa con `CIT_TRANSPORTE=mqtt`. Requiere:

```bash
sudo apt install -y mosquitto mosquitto-clients
sudo systemctl enable --now mosquitto
python3 -m pip install --user paho-mqtt
```

Variables: `CIT_BROKER` (default `127.0.0.1`), `CIT_PUERTO` (default `1883`).

Usa mensajes retenidos (equivalente al archivo de estado) y *last will* para
detectar caídas al instante, en vez de esperar los 15 s del latido.

**Cuándo hace falta:** solo si algún programa va a correr en **otra máquina**.
El archivo compartido no cruza el límite del equipo. Mientras todo viva en la
misma Jetson, el transporte por archivo es más simple y deja historial.

### 5.5 Constantes ajustables

| Constante | Default | Qué hace |
|---|---|---|
| `LATIDO` | 5.0 s | Cada cuánto el productor dice "sigo vivo" |
| `PACIENCIA` | 15.0 s | Sin latido durante esto, se le da por caído |
| `INTERVALO` | 0.2 s | Cada cuánto se revisa el archivo |
| `DIR_DATOS` | `cit/datos` | Se puede mover con `CIT_DATOS` |

---

## 5bis. El envío a AWS: `reenviador.py` (reserva, sin cambios)

> **De reserva desde el 24-sep-2026.** No se usa en el modo VM y no se toca.
> Su modo de órdenes (`CIT_ORDENES=1`, v1.1 del 22-sep) quedó **sustituido**
> por `agente.py` (5ter); no usarlo para la VM: renueva el video y no aplica
> las reglas de comandos del ADR.

Consumidor del bus. Sigue `datos/eventos.jsonl` y publica cada línea en un
broker MQTT; en producción, **AWS IoT Core** (`us-east-1`), puerto 8883, TLS
mutuo con el certificado de la unidad. Cambiar a Mosquitto o EMQX es cambiar
variables de entorno, no código.

### 5bis.1 Contrato con la nube

| Tema | Contenido |
|---|---|
| `cit/v1/<unidad>/aforo` | El evento tal cual salió del bus, más `"unidad"` |
| `cit/v1/<unidad>/cruce` | Ídem |
| `cit/v1/<unidad>/gps` | Ídem |
| `cit/v1/<unidad>/estado` | `en linea` al conectar / `desconectada` al salir o como *last will*. **Retenido** |

- El tema sale del campo `tipo` del evento. Cualquier tipo que cumpla
  `^[a-z][a-z0-9_]{0,19}$` se reenvía solo, sin tocar este archivo; los demás
  se descartan. Así una variable nueva llega a la nube sin cambios aquí.
- `client_id` = `<unidad>` (`cit-001`). En AWS IoT la política del *thing*
  tiene que permitir ese client ID y esos temas.
- Mensajes de más de 120 KB se descartan (el límite de AWS es 128 KB).
- Los eventos llevan el campo `origen` del bus (`camion01`, `gps01`), así que
  en la nube se puede distinguir el productor.

### 5bis.2 Garantía de entrega: al menos una vez

1. Lee líneas **completas** desde la posición guardada en `datos/reenvio.pos`
   (`{"inodo":…, "pos":…}`); una línea a medio escribir se deja para la
   siguiente vuelta.
2. Publica con QoS 1 y **espera el PUBACK** (hasta 15 s).
3. **Solo entonces** avanza y guarda la posición (escritura atómica + `fsync`).
4. Si no hay confirmación, no avanza: reintenta en la próxima vuelta.

Consecuencias:

- **Sin internet no se pierde nada.** Los productores siguen escribiendo en el
  archivo y, al reconectar, el reenviador vacía el atraso en orden.
- **Puede haber duplicados**: si muere entre el PUBACK y el guardado, reenvía
  ese mensaje al arrancar. Quien consuma en la nube debe tolerarlo (por `ts` +
  `origen` + `tipo`, o por `id` en los cruces).
- Si el archivo cambia de inodo (se rotó) o encoge (se vació), vuelve a leer
  desde el principio.
- `--desde-cero` borra la posición y reenvía **todo** el historial.

Reconexión con espera creciente: 2, 5, 10, 20, 30, 60 s. Cada 60 s imprime un
resumen: `enviados=… descartados=… reintentos=… posicion=…`.

### 5bis.3 Variables y opciones

| Variable | Opción | Default | Qué hace |
|---|---|---|---|
| `CIT_UNIDAD` | `--unidad` | `cit-001` | Identidad en la nube y client ID |
| `CIT_BROKER` | `--broker` | `127.0.0.1` | `cit.sh` pone el endpoint de AWS |
| `CIT_PUERTO` | `--puerto` | `8883` | |
| `CIT_TLS` | `--sin-tls` | `1` | Con TLS exige `CIT_CA`, `CIT_CERT` y `CIT_LLAVE` |
| `CIT_EVENTOS` | `--eventos` | `datos/eventos.jsonl` | |
| `CIT_DESPLAZAMIENTO` | `--posicion` | `datos/reenvio.pos` | |

Prueba local, sin AWS:

```bash
CIT_UNIDAD=cit-001 CIT_BROKER=127.0.0.1 CIT_PUERTO=1883 CIT_TLS=0 python3 reenviador.py
mosquitto_sub -t 'cit/v1/#' -v      # en otra terminal
```

> Nota: no confundir `CIT_BROKER` del reenviador con el `CIT_BROKER` del bus
> (5.4). Son el mismo nombre de variable, pero el del bus solo se lee con
> `CIT_TRANSPORTE=mqtt`, que no se usa. `cit.sh` solo se lo pasa al
> reenviador.

---

## 5ter. El agente publicador: `agente.py` (ADR-001)

Es el lado de a bordo de la interfaz del ADR-001: el **agente publicador
único**. Atiende la telemetría (subida) y el control (bajada); el video lo
mueve el contador y aquí solo se ordena. **El contrato completo (campos,
temas, estados, ejemplos) está en [PLAN_VM.md](PLAN_VM.md)**; esta sección
explica cómo funciona por dentro.

### 5ter.1 De dónde saca cada dato

| Campo de la telemetría | Fuente |
|---|---|
| `lat`, `lon`, `speed_kph`, `heading_deg`, `gnss_fix` | `/dev/shm/cit/gps_actual.json`, que `lector_gps.py` reescribe a cada sentencia. Si tiene más de 5 s, se da por perdido (`gnss_fix: none`) |
| `pax_in`, `pax_out`, `pax_onboard` | Suma de los eventos `cruce` de `datos/eventos.jsonl`, leída al estilo `tail -f` desde una posición guardada |
| `health.cameras_active`, `media_session` | `GET 127.0.0.1:8080/estado` del contador, una vez por segundo |
| `health.temp_c`, `disk_free_gb`, `uptime_s` | `/sys/class/thermal`, `datos/`, `/proc/uptime` |
| `ignition`, `health.watchdog_resets` | `null`: no hay hardware (R08, R32) |
| `last_command` | Su propio estado en disco |

**Por qué `pax_*` salen de los cruces y no del aforo del contador.** El ADR
pide acumulados "desde el inicio del servicio" para que una muestra perdida no
corrompa el total. Los contadores del contador vuelven a 0 cada vez que se
reinicia (trampa 5). El agente guarda su propia suma en
`datos/agente_estado.json` junto con la posición de lectura, así que un
`app.restart` o una caída del contador **no** la reinician. Solo se pone a 0
cuando cambia el `boot_id` del kernel, o sea en cada arranque del módulo, que
es lo que marca la ignición (R08). La primera vez empieza a leer por el
**final** de `eventos.jsonl`: los cruces viejos no son de este servicio.

### 5ter.2 Diario de salida y garantía de entrega

Todo lo que sube (telemetría y resultados de comandos) se escribe primero en
`datos/salida_vm.jsonl` con `fsync`. Un hilo lo publica en lotes de hasta 50
con QoS 1 y solo avanza `datos/salida_vm.pos` hasta el último mensaje
**contiguo** confirmado por PUBACK. Mismo principio que 5bis.2, con dos
diferencias: publica en lote (vacía un atraso de 45 muestras en 0.02 s) y
**rota su propio diario** cuando pasa de 20 MB y está todo confirmado (se
conserva el anterior como `.1`). Puede rotar sin riesgo porque es el único que
lo escribe y lo lee.

La sesión MQTT es **persistente** (`clean_session=False`, `client_id` =
`device_id`): un comando mandado mientras la unidad no tenía señal le llega al
reconectar, y es su `ttl_s` el que decide si todavía vale. La reconexión espera
de 2 a 20 s. `estado` se publica retenido, con *last will* `desconectada`.

### 5ter.3 Comandos

Flujo de cada mensaje en `cit/v1/<device>/cmd` (los retenidos se ignoran):

1. JSON válido, si no → `rechazada`.
2. `command_id` ya visto → **no se ejecuta**; se reenvía su resultado con
   `duplicate: true`.
3. Validación: los siete campos de la tabla 4, `device_id` propio, `type` en
   la lista cerrada, `ts_issued` no más de 120 s en el futuro, no caducado
   (`caducada`), y no más de 3 reinicios en la última hora.
4. **Se marca consumido en disco** (`status: en_curso`) **antes** de ejecutar.
5. Se ejecuta y se registra el resultado final.

Cada resultado se guarda en `datos/agente_estado.json` (últimos 500
`command_id`), en la bitácora local `datos/ordenes.jsonl` y en el diario de
salida.

Los reinicios se lanzan desacoplados (`start_new_session`, sin `TMUX` en el
entorno, salida a `datos/reinicio.log`) porque matan al propio agente:

- `app.restart` → `cit.sh reiniciar`. Antes de lanzarlo espera hasta 10 s a
  que el `en_curso` esté confirmado por la VM. El agente nuevo encuentra ese
  `en_curso` al arrancar y lo cierra como `aplicada`. Medido: 9.6 s.
- `module.restart` → `sudo -n /usr/bin/systemctl reboot`. Al volver, si el
  `boot_id` cambió → `aplicada`; si no → `fallida`. Si falla al instante (hoy,
  por falta de permiso) → `fallida` con el mensaje de `sudo`. Si en 120 s el
  proceso sigue vivo → `fallida`.
- `module.shutdown` (nuevo, 20-oct-2026) → `sudo -n /usr/bin/systemctl
  poweroff`. **Apaga y no vuelve sola**: hace falta cortar y devolver la
  energía (o la ignición, cuando exista R08). Mismo cierre que el reinicio:
  queda `en_curso` y, en el siguiente encendido, `aplicada` (`el equipo se
  apago y volvio a encender`); si sigue vivo → `fallida`. Cuenta para el
  límite de 3 por hora.

Las tres órdenes se pueden cambiar con `CIT_CMD_REINICIO_APP`,
`CIT_CMD_REINICIO_SO` y `CIT_CMD_APAGADO_SO` (útil en pruebas).

### 5ter.4 Variables

| Variable | Default | Qué hace |
|---|---|---|
| `CIT_UNIDAD` / `CIT_VEHICULO` | `cit-001` / vacío | `device_id` / `vehicle_id` |
| `CIT_BROKER`, `CIT_PUERTO` | `127.0.0.1`, `8883` | Broker de la VM (`cit.sh` pone `VM_HOST`) |
| `CIT_TLS` | `1` | `0` solo en banco; avisa en pantalla |
| `CIT_CA`, `CIT_CERT`, `CIT_LLAVE` | vacíos | CA propia; cert/llave solo con TLS mutuo |
| `CIT_USUARIO`, `CIT_CONTRASENA` | vacíos | Credencial del equipo |
| `CIT_CONTADOR`, `CIT_CLAVE_LOCAL` | `http://127.0.0.1:8080`, vacía | Control local del contador |
| `CIT_GPS_ACTUAL` | `/dev/shm/cit/gps_actual.json` | Debe coincidir con el de `lector_gps.py` |
| `CIT_VEL_MARCHA` | `3` km/h | Umbral de "en marcha" |
| `CIT_PERIODO_MARCHA` / `_DETENIDA` | `1` / `10` s | Ritmo de muestras |
| `CIT_MAX_REINICIOS_HORA` | `3` | |
| `CIT_DATOS` | `cit/datos` | Mueve todos sus archivos (lo respetan también el bus y el contador) |

Prueba rápida contra un broker local sin TLS:

```bash
CIT_BROKER=127.0.0.1 CIT_PUERTO=1883 CIT_TLS=0 python3 agente.py
mosquitto_sub -t 'cit/v1/#' -v                                  # en otra terminal
mosquitto_pub -t cit/v1/cit-001/cmd -q 1 -m '{"command_id":"t1","device_id":"cit-001",
  "type":"diagnostics.request","issued_by":"yo","ts_issued":"<AHORA en UTC>Z","ttl_s":60,"reason":"prueba"}'
```

---

## 6. Modo de banco: `variables.py`

> **No es el arranque de producción.** Producción es `cit.sh` (sección 0.1).
> `variables.py` sirve para trabajar en el banco con ventana local de video y
> el aforo y el GPS pintados en la terminal. Lanza `contador_neto.py`, no
> `contador_p2p_v15.py`, **no** lanza el reenviador y **no** sirve página.
> Como ambos contadores abren `/dev/video0`, **no se puede correr a la vez que
> `cit.sh`** (trampa 13).

Es a la vez **orquestador** y **receptor**. Un solo comando levanta el sistema.
(Antes se llamaba `monitor_aforo.py`; cambió de nombre al dejar de medir solo el
aforo.)

### 6.1 Qué hace al arrancar

1. Recorre la lista `PROGRAMAS` y lanza cada uno con `subprocess.Popen`.
2. Arranca un hilo `vigilar()` que comprueba cada 0.5 s si algún hijo murió.
3. Llama a `bus.suscribir()`, que **bloquea** el hilo principal escuchando.

```python
PROGRAMAS = [
    {
        "nombre": "contador",
        "ruta": os.path.join(AQUI, "DeepStream-Yolo", "contador_neto.py"),
        "args": sys.argv[1:],          # p.ej. el número de cámara
    },
]

if os.environ.get("SIN_GPS") != "1":
    PROGRAMAS.append({
        "nombre": "gps",
        "ruta": os.path.join(AQUI, "lector_gps.py"),
        "args": [],
        "critico": False,      # si se cae, el aforo debe seguir contando
    })
```

**Añadir un programa nuevo al sistema = añadir una entrada a esta lista.** Casi
no hay que tocar nada más: el bus ya escucha con comodín. Solo si la variable es
de un tipo nuevo hay que añadir su rama en `al_recibir()` para saber pintarla.

Dos detalles de la entrada del GPS:

- **`args` va vacío.** `sys.argv[1:]` es el número de cámara y solo le sirve al
  contador.
- **`"critico": False`.** Ver 4bis.7: un fallo del GPS no debe apagar el conteo.
  Por omisión un programa **sí** es crítico.

### 6.2 Cómo se lanza cada hijo

```python
subprocess.Popen(
    [sys.executable, prog["ruta"]] + list(prog["args"]),
    cwd=os.path.dirname(prog["ruta"]),   # el hijo resuelve sus configs relativas
    stdout=registro,                     # datos/<nombre>.log
    stderr=subprocess.STDOUT,
    env=entorno,
    start_new_session=True,              # grupo de procesos propio
)
```

Decisiones y por qué:

- **`start_new_session=True`** — le da al hijo su propio grupo de procesos, para
  poder detener *todo su árbol* con `killpg` y no dejar nietos huérfanos.
- **`stdout` a un archivo** — DeepStream escupe muchísimo ruido (TensorRT,
  nvinfer, avisos del tracker). Si fuera a la terminal, taparía el aforo. Va a
  `datos/contador.log`.
- **`DISPLAY` se pone a `:0`** si no viene ya definido: la visión abre ventana.
- **`PYTHONUNBUFFERED=1`** — para que el log se escriba al momento y sirva
  cuando algo falla.
- **`cwd`** en la carpeta del hijo, porque `contador_neto.py` da por hecho que
  sus configs están junto a él.

### 6.3 Cómo se detiene

`detener_todos()` escala por tres niveles, con espera entre cada uno:

| Señal | Espera | Por qué |
|---|---|---|
| `SIGINT` | 8 s | Equivale a Ctrl+C: deja que el hijo cierre el pipeline de GStreamer en condiciones (`[NvMultiObjectTracker] De-initialized`) |
| `SIGTERM` | 3 s | Si no hizo caso |
| `SIGKILL` | 3 s | A la fuerza |

Siempre con `os.killpg(os.getpgid(pid), señal)`, para alcanzar al árbol entero.
Se llama desde el `finally` del `main()`, así que se ejecuta salga por donde
salga.

### 6.4 Qué pasa si un hijo se cae

El hilo `vigilar()` lo detecta e imprime las últimas 15 líneas de su log. A
partir de ahí depende de si el programa es **crítico**:

**Crítico** (la visión): marca `_fallo = True` y despierta al hilo principal con
`_thread.interrupt_main()` (que provoca un `KeyboardInterrupt` desde otro hilo),
apagando el sistema entero.

```
ERROR: contador termino solo (codigo 1).
--- ultimas lineas de datos/contador.log ---
  ERROR: no existe /dev/video0

Termino por un fallo en un programa critico.
```

**No crítico** (el GPS): avisa y el resto sigue.

```
AVISO: gps termino solo (codigo 1).
--- ultimas lineas de datos/gps.log ---
  ...
El sistema sigue sin gps.
```

**Códigos de salida:** `0` si se cerró con Ctrl+C o si solo cayó un programa no
crítico, `1` si falló uno crítico. Importa para poder meterlo en `systemd` o en
otro script más adelante.

### 6.5 Qué se ve en pantalla

```
20:51:24  camion01  CONECTADO
20:51:25  camion01      AFORO    3   entradas 5  salidas 2
20:51:29  gps01       GPS   19.028733, -98.248182   alt 2124 m  sat 10  hdop 0.5
20:51:31  camion01    ▲ AFORO    4   entradas 6  salidas 2
20:51:38  camion01    ▼ AFORO    3   entradas 6  salidas 3
20:51:44  gps01       GPS   sin posicion (buscando satelites: 6 a la vista)
```

▲ verde si sube, ▼ rojo si baja. Solo se imprime cuando el valor **cambia**; un
aforo estable o una unidad parada no generan ruido. La posición se muestra con 6
decimales (~0.1 m), muy por debajo del error real del receptor (2.5 m CEP50):
más cifras solo serían ruido en pantalla. Los colores se desactivan solos si la
salida no es una terminal, o con `SIN_COLOR=1`.

---

## 7. Cómo se ejecuta

**Modo VM (ADR-001):** `cd ~/Desktop/cit && ./cit.sh arrancar` (sección
0.1). Para depurar una pieza suelta, con `cit.sh` parado:

```bash
cd ~/Desktop/cit/DeepStream-Yolo
SIN_GPS=1 CLAVE=prueba python3 contador_p2p_v17.py     # sin SRT_DESTINO: cuenta, sin video
curl 'http://127.0.0.1:8080/estado?k=prueba'
curl -X POST -d '{"segundos":10}' 'http://127.0.0.1:8080/video/abrir?k=prueba'
cd ~/Desktop/cit
python3 lector_gps.py --verbose
cat /dev/shm/cit/gps_actual.json                         # la lectura al segundo
CIT_BROKER=127.0.0.1 CIT_PUERTO=1883 CIT_TLS=0 CIT_CLAVE_LOCAL=prueba python3 agente.py
tail -f datos/eventos.jsonl datos/ordenes.jsonl           # bus y bitacora de comandos
```

**Modo de reserva (AWS):** `~/cit.sh arrancar` (sección 0.2). Piezas sueltas:

```bash
cd ~/Desktop/cit/DeepStream-Yolo
SIN_GPS=1 CLAVE=prueba python3 contador_p2p_v15.py     # http://localhost:8080/?k=prueba
cd ~/Desktop/cit
CIT_UNIDAD=cit-001 CIT_BROKER=a3g1b4rvksirbn-ats.iot.us-east-1.amazonaws.com \
  CIT_CA=certs/AmazonRootCA1.pem CIT_CERT=certs/cit-001.cert.pem \
  CIT_LLAVE=certs/cit-001.private.key python3 reenviador.py
```

**En el banco** (ventana local, terminal con colores):

```bash
cd ~/Desktop/cit
python3 variables.py              # cámara /dev/video0
python3 variables.py 1            # cámara /dev/video1
```

`Ctrl+C` detiene todo. No hace falta `export DISPLAY=:0`: lo pone el monitor.

Variables de entorno:

| Variable | Default | Para qué |
|---|---|---|
| `LANZAR` | `1` | `LANZAR=0` solo escucha, sin arrancar nada (depurar) |
| `SIN_GPS` | — | `1` no lanza el lector de GPS (unidad sin módulo) |
| `MODELO` | `peoplenet` | Detector. `yolo26` está retirado; solo para comparar. `variables.py` lo pasa al contador |
| `ORIGEN` | `+` (todos) | Escuchar un solo origen |
| `SIN_COLOR` | — | `1` desactiva los colores ANSI |
| `CIT_TRANSPORTE` | `archivo` | `mqtt` para usar broker |
| `CIT_DATOS` | `cit/datos` | Mover la carpeta de datos |
| `SINK` | `nveglglessink` | Sink de video del contador |
| `ETIQUETAS` | `0` | `1` muestra clase, ID y confianza sobre cada caja (para evaluar un modelo) |
| `FPS_EN_PANTALLA` | `1` | `0` oculta el contador de FPS |
| `MOSTRAR_LINEA` | `1` | `0` no dibuja la línea de conteo (se sigue contando igual) |
| `BANDA_CRUCE` | `8` | Píxeles de franja muerta a cada lado de la línea |
| `CIT_BROKER` | `127.0.0.1` | Solo con `CIT_TRANSPORTE=mqtt` |
| `CIT_PUERTO` | `1883` | Solo con `CIT_TRANSPORTE=mqtt` |
| `ORIGEN_GPS` | `gps01` | Nombre con el que el GPS se anuncia en el bus |
| `GPS_INTERVALO` | `5` | Segundos entre publicaciones estando quieto |
| `GPS_METROS` | `10` | Si se mueve más que esto, publica sin esperar |
| `CIT_GPS_NMEA` | — | Forzar el puerto NMEA (salta la detección) |
| `CIT_GPS_AT` | — | Forzar el puerto de comandos AT |

Para depurar con toda la salida a la vista, en terminales aparte:

```bash
# Terminal 1
LANZAR=0 python3 variables.py
# Terminal 2 -- la vision
cd DeepStream-Yolo && export DISPLAY=:0 && python3 contador_neto.py
# Terminal 3 -- el GPS
python3 lector_gps.py --verbose
```

Requisito de permisos para el GPS: el usuario debe estar en el grupo `dialout`
(`sudo usermod -aG dialout $USER`, y volver a entrar). Ya está hecho en esta
Jetson.

---

## 8. Archivos que SÍ forman parte del sistema

```
~/cit.sh                          modo de RESERVA AWS (v3). Fuera del repo. No tocar
~/tunel.log                       salida de cloudflared; de aqui sale la liga (reserva)
/dev/shm/cit/gps_actual.json      ultima lectura GNSS (lector_gps -> agente). En RAM
cit/
├── AGENTS.md                     este documento
├── .gitignore                    ignora datos/, __pycache__/, yolo_env/, el video
├── cit.sh                        ARRANQUE DEL MODO VM (v5, ADR-001). Ver PLAN_VM.md
├── unidad.env.ejemplo            plantilla de configuracion de cit.sh v5
├── unidad.env                    la configuracion real. NO va a git (contrasenas)
├── PLAN_VM.md                    contrato v1 con la VM, pruebas y pendientes
├── agente.py                     agente publicador unico: telemetria + comandos (5ter)
├── lector_gps.py                 la posicion (productor)
├── reenviador.py                 bus -> AWS IoT Core (RESERVA, sin cambios)
├── variables.py                  modo de banco: orquestador + receptor (6)
├── vm_ca/ca.crt                  CA publica de la VM (TLS del broker). No es secreto
├── certs/                        certificados de AWS IoT (reserva). En .gitignore
│   ├── AmazonRootCA1.pem
│   ├── cit-001.cert.pem
│   └── cit-001.private.key       secreto
├── cit_core/
│   ├── __init__.py
│   ├── bus.py                    transporte de datos
│   ├── serie.py                  puerto serie con termios (sin pyserial)
│   ├── nmea.py                   traduce las sentencias del GNSS
│   └── modulo_gps.py             encuentra el modulo y enciende el GNSS
├── datos/                        se genera solo (está en .gitignore)
│   ├── eventos.jsonl             historial de aforo, cruces y posicion. Crece sin limite
│   ├── reenvio.pos               hasta donde confirmo AWS (reserva)
│   ├── salida_vm.jsonl (+ .1)    diario de lo que sube a la VM (agente). Rota a 20 MB
│   ├── salida_vm.pos             hasta donde confirmo la VM
│   ├── agente_estado.json        pax_in/pax_out, boot_id, comandos vistos, reinicios
│   ├── ordenes.jsonl             bitacora local de comandos recibidos y su resultado
│   ├── cuota_video.json          sesiones de video usadas este mes (contador v17)
│   ├── sesion.env                clave del control local, nueva en cada arranque
│   ├── emisor_srt.log            salida del envio SRT
│   ├── reinicio.log              salida del ultimo app.restart / module.restart
│   ├── contador.log / gps.log    solo en modo de banco (variables.py)
│   ├── gps.lock                  candado: un solo lector a la vez
│   └── estado/
│       ├── camion01.json         latido + ultimo aforo
│       └── gps01.json            latido + ultima posicion
└── DeepStream-Yolo/
    ├── contador_p2p_v17.py       LA VISION DEL MODO VM: conteo + sesion de video acotada
    ├── contador_p2p_v15.py       vision del modo de reserva: conteo + pagina MJPEG
    ├── contador_neto.py          la vision en modo de banco
    ├── config_nvdsanalytics.txt  la linea de conteo (la leen todos los contadores)
    ├── config_tracker_NvDCF.yml  el tracker
    │
    ├── config_infer_primary_peoplenet.txt   EL MODELO DEL PROYECTO
    └── peoplenet/                           89 MB, no está en .gitignore
        ├── resnet34_peoplenet.onnx
        ├── resnet34_peoplenet.onnx_b1_gpu0_int8.engine
        ├── resnet34_peoplenet_int8.txt      tabla de calibración del fabricante
        ├── labels.txt                       3 clases: person, bag, face
        └── nvinfer_config.txt               el config oficial de NVIDIA, de referencia
```

Y lo de YOLO26, **retirado pero que no se borra** (sección 4.2bis):

```
DeepStream-Yolo/
    ├── config_infer_primary_yolo26.txt      solo con MODELO=yolo26
    ├── labels.txt                           80 clases COCO
    ├── yolo26s.onnx  +  yolo26s.onnx.data
    ├── model_b1_gpu0_fp16.engine            motor TensorRT ya construido
    └── nvdsinfer_custom_impl_Yolo/          parser propio, con sus fuentes C++/CUDA
        └── libnvdsinfer_custom_impl_Yolo.so
```

Dependencias externas, fuera del repositorio:

- `/opt/nvidia/deepstream/deepstream/lib/libnvds_nvmultiobjecttracker.so`
- Tarjeta Waveshare SIM7600G-H conectada por USB (módulo SimTech `1e0e`)
- `/usr/local/bin/cloudflared`, `tmux`, `paho-mqtt`
- El *thing* `cit-001` en AWS IoT Core (`us-east-1`), con su política

---

## 9. Archivos que NO forman parte del sistema

Todo lo de abajo puede ignorarse al trabajar en la unidad. **No modificar
esperando que afecte a producción: no lo hará.**

### 9.1 Versiones anteriores del contador y lo que quedó de go2rtc

Antes del 07-sep-2026 había **dos pilas** de conteo separadas: la del bus
(`variables.py` + `contador_neto.py`) y la de video P2P (`arranque.py` +
`contador_p2p_v11.py` + go2rtc). Desde la v1.2 el contador P2P publica al bus,
así que **se unieron** en `contador_p2p_v15.py`. Lo que queda de aquella
historia:

| Archivo | Qué es |
|---|---|
| `DeepStream-Yolo/contador_p2p_v16.py` | v1.6 (22-sep). H.264 por SRT, pero **renueva** la sesión con cada orden y lleva la cuota en memoria. Sustituida por la v1.7 (4quinquies) |
| `reenviador.py` con `CIT_ORDENES=1` | Modo de órdenes de video de la v1.1 (22-sep). Sustituido por `agente.py`. El archivo sigue vigente para AWS (reserva) |
| `DeepStream-Yolo/contador_p2p_v14.py` | v1.4. Empuja H264 a go2rtc |
| `DeepStream-Yolo/contador_p2p_v13.py`, `_v12.py` | v1.3 y v1.2 (4ter.5). Historia |
| `DeepStream-Yolo/contador_p2p_v11.py` | v1.1. **Todavía cuenta con `objLCCumCnt`** (fallo del recruce, 4.5bis) y no publica al bus |
| `DeepStream-Yolo/contador_p2p.py`, `contador_p2p_ORIGINAL.py` | Versión inicial, **con el fallo de seguridad del puente `/api/*`** (permitía ejecutar comandos en la Jetson vía go2rtc). No ejecutar |
| `DeepStream-Yolo/arranque.py` | Lanzador supervisado de la pila vieja: busca `contador_p2p_v11.py` y lanza go2rtc. **Obsoleto**; lo sustituye `cit.sh` |
| `DeepStream-Yolo/gps_edge.py` | Lector GNSS con `pyserial` y puertos fijos, para vivir dentro del contador. Antepasado de `cit_core/`. En producción no se carga (`SIN_GPS=1`) |
| `DeepStream-Yolo/logs/` | Registros de `arranque.py` (`contador.log`, `go2rtc.log`, `tunel.log`). No está en `.gitignore` |
| `~/go2rtc`, `~/go2rtc.yaml` | Puente WebRTC retirado el 09-sep-2026. `cit.sh parar` todavía lo mata por si quedó vivo |

> ⚠️ **Todas las versiones del contador se pelean por la cámara.**
> `/dev/video0` solo admite un proceso. Antes de probar cualquiera a mano,
> `~/cit.sh parar`.

### 9.1bis Otros experimentos

| Archivo | Qué es |
|---|---|
| `DeepStream-Yolo/dead_zones.py` | Experimento con OpenCV: recibe vídeo por UDP/MPEG-TS desde un portátil, pinta dos rectángulos negros (zonas muertas) y lo reenvía. Nada que ver con el conteo. |
| `cit/ultralytics/` | Repositorio de Ultralytics. Se usó **una vez** para exportar `yolo26s.onnx` (ver `export_yolo26.py`). Nunca participó en tiempo de ejecución —ningún archivo del proyecto lo importa— y desde el 29-ago-2026 tampoco se usa como herramienta: el proyecto no exporta más modelos YOLO. Se conserva sin tocar. |

### 9.2 Configuraciones de modelos que no se usan

Los ~25 archivos `config_infer_primary_*.txt` **excepto**
`config_infer_primary_peoplenet.txt` (el que se usa) y
`config_infer_primary_yolo26.txt` (retirado, se conserva): vienen del repositorio original DeepStream-Yolo
y cubren otros modelos (YOLOv2 a YOLOv13, YOLO-NAS, YOLOX, RT-DETR, D-FINE,
PPYOLOE, DAMO-YOLO, RTMDet, Gold-YOLO, RF-DETR, CO-DETR…). Solo son plantillas.

También sobran:

| Archivo | Por qué no se usa |
|---|---|
| `deepstream_app_config.txt` (+ `.bak_20260814_211534`) | Para el ejecutable `deepstream-app` de línea de comandos. `contador_neto.py` construye su pipeline en Python y **no lee este archivo**. |
| `config_msgconv.txt` | Config de `nvmsgconv` (publicar a Kafka/MQTT desde DeepStream). Quedó preparado pero **ningún programa lo carga**. |
| `labels_rfdetr.txt` | Etiquetas del modelo RF-DETR. |
| `model_b1_gpu0_fp32.engine` | Motor FP32. La config apunta al **fp16**. 40 MB que se pueden borrar. |

### 9.3 Copias de seguridad manuales

Se pueden borrar; el historial está en git:

- `config_nvdsanalytics.txt.bak_20260814_211534`
- `config_nvdsanalytics.txt.bak_texto`
- `config_nvdsanalytics.txt.bak_una_linea`
- `deepstream_app_config.txt.bak_20260814_211534`

### 9.4 Material del repositorio original y basura

| Ruta | Qué es |
|---|---|
| `DeepStream-Yolo/README.md`, `LICENSE.md`, `docs/`, `.github/`, `utils/` | Del proyecto de origen [DeepStream-Yolo](https://github.com/marcoslucianops/DeepStream-Yolo) de Marcos Luciano (MIT). Referencia útil, código muerto para nosotros. |
| `DeepStream-Yolo/agora_env/`, `cit/yolo_env/` | Entornos virtuales de Python de pruebas anteriores. El sistema usa el Python del sistema. `yolo_env/` está en `.gitignore`; `agora_env/` **no**. |
| `DeepStream-Yolo/logs/` | Registros de `arranque.py`, obsoleto (sección 9.1). No confundir con `datos/`, que es donde escribe el sistema. |
| `DeepStream-Yolo/ffmpeg`, `DeepStream-Yolo/sudo` | Archivos de **0 bytes** creados por error al teclear un comando. Se pueden borrar. |
| `cit/video_camion.mp4` | Video de prueba (24 MB). En `.gitignore`. |
| `__pycache__/` | Caché de Python. En `.gitignore`. |

---

## 10. Convenciones del código

Al escribir código en este proyecto, seguir lo que ya hay:

- **Todo en español**: nombres de variables, funciones y comentarios
  (`crear`, `sonda_osd`, `_ultimo_aforo`, `entradas`, `salidas`).
- **Comentarios sin acentos** (ASCII puro) dentro del código, para evitar
  problemas de codificación. El texto que ve el usuario sí puede llevarlos.
- **Cabecera de bloque** en cada archivo: nombre, propósito, diagrama del flujo,
  modo de uso y variables de entorno.
- **Secciones marcadas** con `# --- Nombre ---------------` o con bloques
  `# ====...`.
- Comentar **el porqué**, no el qué. Los comentarios existentes explican
  decisiones (por qué no usar `display-text=0`, por qué `start_new_session`).

---

## 11. Trampas conocidas

1. **`display-text=0` en el `nvdsosd` apaga también el aforo.** Las etiquetas de
   objetos se quitan en la sonda, nunca con esa propiedad.
2. **Las líneas de `entrada` y `salida` no miden lo mismo** (sección 4.4). Ya
   no afecta al aforo, porque el contador solo lee la de `entrada`, pero sigue
   afectando a lo que `nvdsanalytics` dibuja.
3. **`config-width/height` de analytics debe cuadrar** con el ancho/alto del
   `nvstreammux`. Si no, las líneas quedan donde no toca.
4. **Las etiquetas `entrada`/`salida` son un contrato** entre el `.txt` de
   analytics y las constantes de Python. Renombrar una sin la otra deja el
   aforo clavado en 0.
5. **Los acumulados van desde que arrancó el proceso.** Al reiniciar vuelven
   a 0. Quien consuma estos datos a futuro debe contemplarlo: un reinicio
   parece "todo el mundo salió de golpe".
5bis. **No volver a `objLCCumCnt`.** Parece la vía natural —la hace el plugin,
   son dos líneas de Python— y por eso conviene dejarlo escrito: cuenta cada
   track ID una sola vez por línea y rompe el conteo de quien recruza. Es el
   fallo que se corrigió en 4.5bis.
6. **Borrar el `.engine` cuesta minutos** de reconstrucción en el siguiente
   arranque. No interrumpirla.
7. **`interval=2`** significa que se infiere 1 de cada 3 frames. Si alguien
   cruza muy rápido y muy cerca del borde, el tracker puede perderlo.
8. **Nunca dar por fijo `/dev/ttyUSB1` para el GPS.** El número cambia con el
   orden de arranque y con el puerto físico. Se busca por interfaz USB (ver
   4bis.3). Es el error que más probablemente rompa el GPS en la unidad real.
9. **Dos procesos leyendo el mismo puerto serie no dan error**, se roban los
   bytes. De ahí el candado `datos/gps.lock`. Si se depura a mano, parar antes
   el lector que lanzó `variables.py`.
10. **La tarjeta del SIM7600 no se alimenta por el micro USB**, sino por el
    header de 40 pines. LED PWR apagado = no hay nada que depurar en software.
11. **Cada productor necesita su propio `ORIGEN`.** Compartirlo hace que dos
    programas se sobrescriban el archivo de estado.
12. **`ModemManager` ocupa el puerto AT** del módulo. Es inofensivo para leer la
    posición (ver 4bis.4); no hay que desactivarlo por costumbre.
13. **Todas las versiones del contador se pelean por `/dev/video0`.**
    `cit.sh` (con `contador_p2p_v15.py`) y `variables.py` (con
    `contador_neto.py`) no pueden correr a la vez. Un cambio en uno de los dos
    contadores **no** se refleja en el otro: la lógica de conteo está copiada,
    no compartida. Antes de editar, confirmar cuál corre en producción (0.1).
14. **Los `.txt` de nvinfer y analytics sí son compartidos.** Mover la línea de
    conteo o cambiar un umbral toca **los dos contadores** a la vez (y todas las
    versiones viejas). Es lo que más fácil se rompe sin darse cuenta.
15. **`config_infer_primary_peoplenet.txt` lleva rutas absolutas**
    (`/home/nelsonkanzela/Desktop/cit/...`), igual que el de YOLO26. Mover el
    repositorio de sitio rompe el arranque hasta que se reescriban. Lo mismo
    `cit.sh`, que da por hecho `~/Desktop/cit`.
16. **YOLO26 está retirado, no borrado.** Sigue siendo cargable con
    `MODELO=yolo26` y sus archivos siguen ocupando disco a propósito. Si algo
    del proyecto vuelve a apuntar a YOLO sin que nadie lo pidiera, es un
    descuido, no la configuración esperada (sección 4.2bis).
17. **La clave de la página es fija y conocida** (`citdemo2026`, default de
    `cit.sh`) y la liga la lleva en claro. Cualquiera con la liga ve el video y
    consume la cuota. Para una unidad real, arrancar con `CLAVE=<aleatoria>`.
18. **La página dice "modulo gps no cargado" aunque el GPS funcione.** Con
    `SIN_GPS=1` el contador no lee el GPS, y la página toma la posición de la
    memoria del contador, no del bus. La posición **sí** llega a la nube. Para
    verla en la página habría que leer `datos/estado/gps01.json` en
    `estado_completo()`.
19. **`certs/` contiene `cit-001.private.key`.** Desde el 22-sep-2026 está en
    `.gitignore`, igual que `unidad.env`. Si alguna vez se sube al
    repositorio, el certificado se da por comprometido y hay que revocarlo en
    AWS IoT.
20. **Hay dos `cit.sh`**: `~/cit.sh` (v3, modo AWS + túnel, reserva) y
    `cit/cit.sh` (v5, modo VM del ADR-001). Cambios hechos en una no están en
    la otra, y las dos usan la sesión de tmux `cit`.
24. **`pkill -f` sin ancla mata de más.** `cit.sh` v3 hace `pkill -f
    contador_p2p`, que también alcanza a cualquier proceso que solo mencione
    ese nombre (un `tail -f`, un editor, un `grep`). La v4 y la v5 anclan los patrones
    (`'^python3 (.*/)?contador_p2p'`).
25. **`videorate` no sirve para bajar cuadros en este pipeline.** Con los
    búferes NVMM de `nvstreammux` duplica en vez de descartar y deja pasar
    ~17 c/s aunque se le pida `drop-only`. La v16 descarta en una sonda propia
    (`sonda_ritmo`) y declara el ritmo al codificador con `capssetter`.
21. **`datos/eventos.jsonl` crece sin límite** (≈1.2 MB con unas 6 500 líneas,
    casi todas de GPS cada 5 s). Si se rota o se vacía, el reenviador lo
    detecta por inodo/tamaño y vuelve a empezar, pero **no** hay que hacerlo
    con eventos aún sin confirmar: se perderían.
22. **La liga cambia en cada arranque** (Quick Tunnel de Cloudflare, sin cuenta).
    Hay que volver a mandarla tras cada `cit.sh arrancar`. Un túnel con nombre
    daría una dirección fija.
23. **Los acumulados de la nube también vuelven a 0** al reiniciar el contador
    (trampa 5). En la nube, un salto de `entradas` hacia abajo significa
    reinicio, no que se bajó la gente. Los eventos `cruce` no tienen ese
    problema. En el modo VM, `pax_in`/`pax_out` del agente **no** tienen este
    problema dentro de un mismo arranque del módulo (5ter.1).
26. **No reenviar `media.open` para prolongar el video.** La v17 rechaza
    abrir con una sesión en curso (ADR-001: sin renovación). El plan viejo de
    la VM ("re-pedir cada 45 s") ya no vale.
27. **La cuota de video vive en `datos/cuota_video.json`.** Borrarlo
    devuelve todas las sesiones del mes. En pruebas conviene bajar
    `TOPE_SESIONES_MES` o usar `CIT_DATOS` aparte, para no gastar la del mes.
28. **`datos/agente_estado.json` guarda los comandos ya consumidos.**
    Borrarlo (o el archivo entero de `datos/`) permite que un comando
    repetido se vuelva a ejecutar, y pone a 0 `pax_*` y el límite de
    reinicios. No borrarlo con la unidad en servicio.
29. **`module.restart` y `module.shutdown` necesitan permiso de root sin
    contraseña** para `systemctl reboot` y `systemctl poweroff`. El de reboot
    se instaló el 30-sep en `/etc/sudoers.d/cit-reinicio`; el de poweroff hay
    que añadirlo a la misma regla (PLAN_VM.md, fase 4). Si se borra, el comando vuelve a quedar `fallida`
    con `sudo: a password is required`, que es lo correcto. El arranque al encender ya
    está instalado (crontab `@reboot`); `./cit.sh quitar-arranque` lo quita.
    **Mientras no exista `unidad.env`, al encender arranca sin VM** (solo
    conteo y GPS, sin telemetría ni comandos).
30. **`gps_actual.json` vive en `/dev/shm`** (RAM): desaparece al apagar y
    solo existe mientras `lector_gps.py` corre. Si el agente dice
    `gps_alive: false`, mirar primero la ventana `gps`. `lector_gps.py` y
    `agente.py` deben usar el mismo `CIT_GPS_ACTUAL`.
31. **El servicio de ingesta de la VM debe suscribirse con sesión
    persistente.** Con sesión limpia pierde lo que el broker entregue mientras
    se reinicia (visto en banco). La Jetson no lo puede compensar.
32. **El reloj de la Jetson importa.** `ts_device` y la vigencia de los
    comandos (`ts_issued + ttl_s`) dependen de NTP. Una orden con
    `ts_issued` más de 120 s en el futuro se rechaza; con el reloj atrasado,
    órdenes vencidas parecerían vigentes. Hoy NTP está activo y sincronizado.

---

## 12. Próximos pasos previstos

Pensados pero **no implementados** todavía:

- **La VM (ADR-001, fases 2 y 3 de PLAN_VM.md).** Broker con TLS, MediaMTX,
  ingesta con base de datos propia de Edge, API y ticket de video. Es de
  Software/TI según el ADR; la Jetson ya está lista y probada en banco.
- **Pendientes de hardware del ADR**: señal de ignición (R08) para
  `ignition`, conteo de reinicios del watchdog (R32) para
  `health.watchdog_resets`, cámaras IP por RTSP en lugar de la USB.
- **Persistencia consultable (AWS, reserva).** Este repositorio no dice qué
  regla de IoT recibe los datos de AWS ni dónde se guardan.
- ~~Eventos por cruce~~ **Hecho** en la v1.2: `tipo: cruce` con dirección, ID
  del track y aforo, publicado al bus y a la nube.
- ~~Panel web~~ **Hecho**: la página del contador (4ter). Falta mostrar el GPS
  (trampa 18).
- **Arranque al encender**: **instalado el 27-sep-2026** con
  `./cit.sh instalar-arranque` (crontab `@reboot`, 30 s de espera, salida en
  `datos/arranque.log`). Probado en el entorno reducido de cron (sin
  `.bashrc`, con `/bin/sh`): arranca contador y GPS igual. Falta que algo
  reinicie un proceso caído: hoy, si uno muere, la ventana se queda en el
  prompt. `systemd` resolvería ambas cosas, pero pide sudo.
- **Rotar `eventos.jsonl`** de forma segura: solo la parte ya confirmada por el
  reenviador (`reenvio.pos`).
- **Segundo programa de visión.** Se integra añadiendo una entrada a `PROGRAMAS`
  en `variables.py` y usando `Publicador` con otro `ORIGEN`.
- **Cruzar aforo con posición.** Ahora que ambos van al mismo `eventos.jsonl`
  con marca de tiempo UTC, se puede responder "cuánta gente subió en cada
  parada". Hoy nada lo hace: las dos variables se publican, pero no se
  correlacionan.
- **Validación formal del GNSS.** La guía de hardware define un criterio de
  aceptación (mínimo 6 satélites, HDOP < 2, cero pérdidas de fix, TTFF en frío
  < 90 s) que aún **no se ha medido en una ventana de 30 minutos**. Lo hecho
  aquí obtiene el dato; no acredita el subsistema.
