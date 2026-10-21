# Plan de trabajo — Edge Mobility hacia la VM (ADR-001)

Implementación del **ADR-001 v1.6** (*Interfaz de datos entre Edge Mobility y
las plataformas RuTi*, 24-sep-2026). El ADR separa la interfaz en **tres
planos**: telemetría, medios (video) y control (comandos). Este documento dice
qué hay ya en la Jetson, cuál es el contrato exacto que la VM tiene que
implementar y qué queda pendiente.

Última actualización: **24-sep-2026**.

> **AWS IoT Core queda de reserva, sin cambios.** `reenviador.py`,
> `~/cit.sh` v3, `certs/` y la ventana `aws` de `cit.sh` no se tocaron. Nada
> de este plan depende de ellos. Se vuelven a encender con `ENVIAR_AWS=1`.

| Fase | Qué | Responsable (ADR §2.1) | Estado |
|---|---|---|---|
| 1 | Jetson: agente publicador, contrato v1, comandos, sesión de video acotada | Hardware | **Hecha y probada en banco (24-sep)** |
| 2 | VM: broker MQTT con TLS + MediaMTX + firewall | Software/TI | Pendiente |
| 3 | VM: servicio de ingesta, base de datos propia, API de Edge, ticket de video | Software/TI | Pendiente |
| 4 | Endurecer: arranque automático, permiso de reinicio, R08/R32 en la carrier board | Hardware | Parcial (ver pendientes) |

---

## Arquitectura

```
                    JETSON (a bordo)                                   VM (nube)
 ┌──────────────────────────────────────────────────────┐   ┌──────────────────────────────┐
 │ lector_gps.py ──► /dev/shm/cit/gps_actual.json ──┐   │   │                              │
 │ contador_p2p_v17.py ─► datos/eventos.jsonl ──────┤   │   │ Broker MQTT :8883 (TLS)      │
 │        (cruces)                                  ▼   │   │                              │
 │                     agente.py ─ muestra tabla 3 ─────┼──►│ cit/v1/<dev>/telemetria ─► ingesta ─► BD Edge
 │                        │  datos/salida_vm.jsonl      │   │ cit/v1/<dev>/resultado       │      └► tiempo real
 │  TELEMETRIA            │  (diario, QoS 1, PUBACK)    │   │ cit/v1/<dev>/estado (ret.)   │
 │ ───────────────────────┼─────────────────────────────│   │                              │
 │  CONTROL               ◄──────────────────────────────┼───│ cit/v1/<dev>/cmd  ◄── API ◄── RuTi for Business
 │   lista cerrada, ttl, dedup, consumido antes de ejecutar  │                              │
 │                        │ POST 127.0.0.1:8080/video/* │   │                              │
 │ ───────────────────────┼─────────────────────────────│   │                              │
 │  MEDIOS                ▼                             │   │                              │
 │ contador: válvula → H.264 8 c/s → UDP local :5600    │SRT│ MediaMTX :8890 (SRT)         │
 │   → gst-launch srtsink (solo con sesión abierta) ────┼──►│   → WebRTC / HLS ─► ticket ─► RuTi
 └──────────────────────────────────────────────────────┘   └──────────────────────────────┘
```

La Jetson **solo abre conexiones hacia afuera** (CGNAT celular). No necesita
IP pública, puertos abiertos ni túnel.

Los tres planos son independientes a propósito (tabla 1 del ADR,
"independencia de planos"): si MediaMTX cae, la telemetría sigue; si el
broker cae, la telemetría se acumula en disco y el conteo no se entera.

---

## Fase 1 — Jetson (hecha)

### Qué cambió el 24-sep-2026

| Archivo | Cambio |
|---|---|
| `agente.py` | **Nuevo.** El "agente publicador único" del ADR. Arma la muestra de la tabla 3, la escribe en un diario en disco y la sube con QoS 1; recibe y ejecuta los comandos de la lista cerrada |
| `DeepStream-Yolo/contador_p2p_v17.py` | **Nuevo** (copia de la v16). Sesión de video sin renovación, duración pedida, cuota en **sesiones al mes** guardada en disco, datos de la sesión y cámara viva en `/estado` |
| `lector_gps.py` | Además del bus, deja la última lectura en `/dev/shm/cit/gps_actual.json` a cada sentencia (≈1 Hz). **Lo que va al bus y a AWS no cambia** |
| `cit.sh` (v5) | La ventana `vm` corre `agente.py`. Nuevos `reiniciar`, `instalar-arranque`, `quitar-arranque`. TLS hacia la VM por omisión. La ventana `gps` también carga `unidad.env` |
| `unidad.env.ejemplo` | `VEHICULO_ID`, `VM_MQTT_CA/CERT/LLAVE`, puerto 8883 y TLS por omisión, `VENTANA_S`, `TOPE_SESIONES_MES` |

Lo que **ya no se usa** en el modo VM, pero sigue en disco: la v16 del
contador y el modo de órdenes de `reenviador.py` (`CIT_ORDENES=1`). El agente
los sustituye. `reenviador.py` sigue siendo el de AWS.

### Cómo se arranca

```bash
cd ~/Desktop/cit
cp unidad.env.ejemplo unidad.env && chmod 600 unidad.env
nano unidad.env          # VM_HOST, VEHICULO_ID, VM_MQTT_CONTRASENA, VM_SRT_PASSPHRASE
./cit.sh arrancar
./cit.sh estado
./cit.sh instalar-arranque   # ya instalado el 27-sep-2026 (crontab @reboot)
```

### Reglas impuestas en la Jetson

**Telemetría**

- **Ritmo**: 1 muestra por segundo con la unidad en marcha (velocidad GNSS
  ≥ 3 km/h, y 5 s más tras bajar de ahí), 1 cada 10 s detenida. Además sale una
  muestra **al instante** (con 1 s de separación mínima) si cambia `pax_*`,
  se abre o cierra una sesión de video, se gana o pierde el fix, cae el
  contador o cambia el estado del último comando.
- **Nada se pierde**: cada muestra se escribe en `datos/salida_vm.jsonl`
  (con `fsync`) antes de publicarse; la posición confirmada
  (`datos/salida_vm.pos`) solo avanza con el PUBACK. Se publica en lotes de
  hasta 50 y se confirma solo lo contiguo. El diario se rota solo cuando
  pasa de 20 MB **y** todo está confirmado.
- **Sin duplicados en la VM**: cada muestra lleva `sample_id` (uuid4) que se
  conserva en el reenvío. Puede llegar dos veces; la VM deduplica por él.
- `buffered: true` si la muestra sale más de 5 s después de armarse.
- Reconexión al broker con espera de 2 s a 20 s como máximo.

**Video (medios)**

- La sesión se abre **solo** con un comando `media.open` válido.
- **Duración**: la que pida el comando (`params.duration_s`), nunca más de
  `VENTANA_S` (tope duro 60 s). La cuenta la lleva la Jetson y **la Jetson
  corta**, aunque el navegador desaparezca.
- **Sin renovación**: un `media.open` con la sesión abierta se rechaza
  (`sesion en curso, restan N s`). Para seguir viendo, la plataforma espera
  al cierre y pide otra sesión, que cuenta como otra.
- **Cuota**: `TOPE_SESIONES_MES` sesiones por unidad al mes (UTC), guardada
  en `datos/cuota_video.json`; sobrevive a reinicios. **100 es provisional**:
  la cifra la fijan Dirección general y Finanzas (tabla 5 del ADR).
- Sin sesión, la válvula está cerrada y **no existe proceso de envío SRT**:
  cero tráfico de video. El conteo no se entera en ningún caso.

**Comandos (control)**

- Lista cerrada: `media.open`, `media.close`, `app.restart`,
  `module.restart`, `diagnostics.request`. Cualquier otro tipo se rechaza y
  queda en la bitácora.
- **Consumido antes de ejecutar**: el `command_id` se guarda con `fsync` en
  `datos/agente_estado.json` antes de actuar. Tras un reinicio, la orden no
  se vuelve a ejecutar.
- **Repetido** (mismo `command_id`): no se ejecuta; se reenvía su resultado
  con `"duplicate": true`.
- **Caducado** (`ts_issued + ttl_s` ya pasó al llegar): `caducada`.
- `ts_issued` más de 120 s en el futuro: se rechaza (relojes desfasados).
- `device_id` ajeno o campo obligatorio vacío: se rechaza.
- **Máximo 3 reinicios** (`app.restart` + `module.restart`) por hora, contados
  en disco.
- Bitácora local en `datos/ordenes.jsonl`; el resultado también sube a la VM.

---

## Contrato MQTT, versión 1

Prefijo `cit/v1/<device_id>/`. JSON en UTF-8, marcas de tiempo UTC en
ISO 8601 terminadas en `Z`. QoS 1 en todo.

### Temas

| Tema | Sentido | Retenido | Qué lleva |
|---|---|---|---|
| `telemetria` | Jetson → VM | no | Muestra de la tabla 3 |
| `resultado` | Jetson → VM | no | Estado reportado de cada comando |
| `estado` | Jetson → VM | **sí** | `en linea` al conectar; `desconectada` al salir o como *last will* |
| `cmd` | VM → Jetson | **nunca** | Comando de la tabla 4 |

`telemetria` y `resultado` pasan por el diario en disco (llegan aunque haya
cortes); `estado` no.

### Telemetría (tabla 3)

```json
{"schema_version":1,"device_id":"cit-001","vehicle_id":"A-01",
 "sample_id":"2ca82368c0bb4e2cb38b1fe00250dcf1","ts_device":"2026-09-25T04:32:54.455Z",
 "lat":19.0298421,"lon":-98.2481535,"speed_kph":0.0,"heading_deg":null,"gnss_fix":"gps",
 "pax_in":3,"pax_out":1,"pax_onboard":2,"ignition":null,
 "health":{"temp_c":53.8,"disk_free_gb":385.8,"cameras_active":1,"uptime_s":1413,
           "watchdog_resets":null,"counter_alive":true,"gps_alive":true},
 "last_command":{"command_id":"49955b99","type":"app.restart","status":"aplicada",
                 "ts_applied":"2026-09-25T04:45:21.803Z"},
 "media_session":{"camera":"cam0","requested_by":"ana@flota","command_id":"843be819","remaining_s":19},
 "buffered":false}
```

| Campo | Cómo lo llena la Jetson |
|---|---|
| `device_id` | `UNIDAD_ID` (el módulo) |
| `vehicle_id` | `VEHICULO_ID`; `"sin-asignar"` si está vacío |
| `sample_id` | uuid4 hexadecimal, fijo desde que se arma la muestra |
| `ts_device` | Reloj de la Jetson (sincronizado por NTP), milisegundos |
| `lat`, `lon` | Tal cual del GGA (7 decimales, como sale de `cit_core/nmea.py`). `null` sin fix |
| `speed_kph` | Del RMC (Doppler del receptor), **nunca** derivada de coordenadas. `null` sin fix |
| `heading_deg` | Del RMC. `null` parado o sin fix |
| `gnss_fix` | Calidad del GGA: `none`, `gps`, `dgps`, `pps`, `rtk_fixed`, `rtk_float`, `estimated`, `manual`, `simulated`. `none` también si `lector_gps.py` lleva más de 5 s sin escribir |
| `pax_in`, `pax_out` | Suma de los eventos `cruce` del bus **desde el arranque del módulo** (ver abajo). Sobreviven a reiniciar programas |
| `pax_onboard` | `max(0, pax_in − pax_out)`, la misma regla que el contador |
| `ignition` | **`null`**: el hardware actual no tiene la señal (R08 de la carrier board) |
| `health.temp_c` | La zona térmica más caliente |
| `health.disk_free_gb` | Espacio libre del disco de `datos/` (aún no hay disco de grabación aparte) |
| `health.cameras_active` | 1 si el contador recibió un cuadro en los últimos 5 s |
| `health.uptime_s` | `/proc/uptime`. Si baja, hubo reinicio del módulo |
| `health.watchdog_resets` | **`null`**: R32 propuesto, sin hardware todavía |
| `health.counter_alive`, `gps_alive` | Extras: el contador responde / hay lectura GNSS reciente |
| `last_command` | Último comando tratado, con su estado |
| `media_session` | `{}` sin sesión (el estado normal) |
| `buffered` | Ver reglas |

**"Inicio del servicio"** (`pax_*`): el ADR lo define para acumular, pero no
dice cuándo empieza. En la Jetson se tomó **el arranque del módulo**
(`boot_id` del kernel), porque la ignición enciende el módulo (R08). Así un
`app.restart` o una caída del contador **no** reinician la cuenta, y un
reinicio del módulo sí (y se nota en `health.uptime_s`).

### Comando (tabla 4)

```json
{"command_id":"843be819","device_id":"cit-001","type":"media.open",
 "issued_by":"ana@flota","ts_issued":"2026-09-25T04:44:22Z","ttl_s":60,
 "reason":"revisar incidente reportado por el operador",
 "params":{"camera":"cam0","duration_s":60}}
```

Todos los campos de la tabla 4 son obligatorios. `params` es **extensión
propuesta** (ver diferencias con el ADR) y solo la usa `media.open`:
`camera` (hoy solo `cam0`) y `duration_s` (≤ 60; por omisión 60).

| `type` | Qué hace | `data` en el resultado |
|---|---|---|
| `media.open` | Abre sesión en el contador | `camera`, `duration_s`, `stream` (nombre en MediaMTX), `sessions_left` |
| `media.close` | Cierra la sesión abierta | — |
| `app.restart` | `cit.sh reiniciar` (≈10 s medido) | — |
| `module.restart` | `sudo -n systemctl reboot` | — |
| `module.shutdown` | `sudo -n systemctl poweroff`. **No vuelve a encender sola** | — |
| `diagnostics.request` | Foto del equipo | procesos, contador, gps, temperaturas, disco, pendiente por subir, reinicios de la última hora, cola del log SRT (≈1.2 KB) |

### Resultado (estado reportado)

```json
{"schema_version":1,"device_id":"cit-001","command_id":"843be819","type":"media.open",
 "issued_by":"ana@flota","ts_received":"2026-09-25T04:44:22.604Z","status":"aplicada",
 "detail":null,"ts_applied":"2026-09-25T04:44:23.702Z",
 "data":{"camera":"cam0","duration_s":60,"stream":"cit-001","sessions_left":99}}
```

| `status` | Significado |
|---|---|
| `en_curso` | Recibido, marcado como consumido, ejecutándose. Los reinicios se quedan aquí hasta que el equipo vuelve |
| `aplicada` | Hecho. En reinicios: el equipo volvió (`detail: "el equipo volvio en linea"`) |
| `rechazada` | No se ejecuta: fuera de lista, otro `device_id`, faltan campos, límite de reinicios, sesión en curso, cuota agotada, cámara inexistente |
| `caducada` | Llegó después de `ts_issued + ttl_s` |
| `fallida` | Se intentó y falló (contador sin respuesta, sin permiso de reinicio, el equipo no se reinició en 120 s) |

`pendiente` y `unidad sin conexión` del ADR son estados **de la
plataforma** (el equipo no los puede reportar): pendiente = sin `resultado`
todavía; sin conexión = `estado` retenido en `desconectada`.

---

## Pruebas en banco (24-sep-2026)

Con la cámara USB real, el módulo GNSS real (sin fix, bajo techo) o un GPS
simulado, **Mosquitto 2.0.11 con TLS, usuario por equipo, ACL y persistencia**
en lugar de la VM, y un receptor SRT local en lugar de MediaMTX.

| Criterio (tabla 1 del ADR) | Prueba | Resultado |
|---|---|---|
| Pérdida de telemetría tras corte | Broker apagado 50 s con la unidad a 1 Hz y cruces a mitad del corte | ✅ 99/99 muestras, en orden, sin huecos, 45 con `buffered:true`; atraso vaciado en 0.02 s |
| Ritmo | GPS simulado 0 → 30 → 0 km/h | ✅ 10 s detenida, 1 s en marcha, vuelve a 10 s |
| Reinicio remoto en línea (< 90 s, telemetría < 3 min) | `app.restart` real (`cit.sh reiniciar`) | ✅ `aplicada` y telemetría de vuelta en **9.6 s** |
| Reinicio remoto sin señal | Comando con `ttl_s=5` mandado con el agente apagado | ✅ Llegó al volver (sesión persistente) y quedó `caducada`; uno con `ttl_s=120` sí se ejecutó |
| Ausencia de bucle de reinicio | 4 `app.restart` seguidos; mismo `command_id` repetido | ✅ El 4º `rechazada (limite de 3 reinicios por hora)`; el repetido no se ejecuta y reenvía su resultado |
| Frontera del comando | `vehicle.power_off`, otro `device_id`, sin `reason`, JSON inválido, `ts_issued` +10 min | ✅ Todos `rechazada` y anotados |
| Trazabilidad | — | ✅ Cada comando en `datos/ordenes.jsonl` y en `resultado` con `issued_by`, `ts_received`, `ts_applied` |
| Apertura de video (< 5 s) | `media.open` 20 s | ✅ `aplicada` en ~1 s; H.264 640×480 a 8 c/s decodificable |
| Cierre forzado por el equipo | Sesiones de 20 y 12 s | ✅ `VIDEO CERRADO (se agoto la ventana)` al segundo exacto; el proceso SRT desaparece |
| Sin renovación | `media.open` con sesión abierta | ✅ `rechazada (sesion en curso, restan 16 s)` |
| Cierre anticipado | `media.close` a los 4 s | ✅ |
| Tope mensual | Tope 3 en la prueba, 4ª sesión | ✅ `rechazada (cuota mensual de sesiones agotada)`; contador en disco |
| Reposo de medios | Sin sesión | ✅ Ningún proceso `srtsink` vivo |
| `module.restart` sin permiso | `sudo` pide contraseña en esta Jetson | ✅ `fallida (sudo: a password is required)`, sin reiniciar |
| Estado retenido | Salida ordenada y corte | ✅ `desconectada` retenido; `en linea` al volver |

**No probado todavía**: contra la VM real, por internet ni por red celular;
`module.restart` con permiso; con cámaras IP (hoy es una USB).

### Consumo de datos medido

Una muestra pesa **≈ 490 bytes** de JSON (más ≈ 70 de MQTT/TLS). El ADR
estimó 150 bytes. En el peor caso, 16 h diarias en marcha a 1 Hz, son
**≈ 32 MB al día y ≈ 1 GB al mes** por unidad (el ADR decía 260 MB). Detenida
baja a la décima parte. Sigue por debajo de 100 sesiones de video
(≈ 1.1 GB según el ADR), pero ya no es despreciable. Si hace falta, las
opciones son mandar `health` cada 60 s o comprimir; son cambios de contrato
y los decide el ADR.

---

## Fase 2 — VM (Software/TI)

VM pequeña con IP fija. Lo que la Jetson necesita de ella:

1. **Firewall**: `8883/tcp` (MQTT TLS), `8890/udp` (SRT), los de WebRTC/HLS de
   MediaMTX hacia los clientes, `22/tcp`.
2. **Mosquitto con TLS, usuario por equipo, ACL y persistencia**. Es la
   configuración con la que se probó:
   ```
   # /etc/mosquitto/conf.d/cit.conf
   persistence true
   allow_anonymous false
   password_file /etc/mosquitto/passwd
   acl_file /etc/mosquitto/acl
   max_queued_messages 1000
   listener 8883
   certfile /etc/letsencrypt/live/<dominio>/fullchain.pem
   keyfile  /etc/letsencrypt/live/<dominio>/privkey.pem
   ```
   ```
   # /etc/mosquitto/acl
   user cit-001
   topic write cit/v1/cit-001/telemetria
   topic write cit/v1/cit-001/resultado
   topic write cit/v1/cit-001/estado
   topic read  cit/v1/cit-001/cmd

   user ingesta
   topic read  cit/v1/+/telemetria
   topic read  cit/v1/+/resultado
   topic read  cit/v1/+/estado

   user api
   topic write cit/v1/+/cmd
   ```
   - **`persistence true` es obligatorio**: el agente usa sesión persistente
     para que un comando mandado sin señal le llegue al volver.
   - Con Let's Encrypt, `VM_MQTT_CA` se deja vacío en la Jetson.
3. **MediaMTX** recibiendo SRT:
   ```yaml
   srt: yes
   srtAddress: :8890
   paths:
     cit-001:
       srtPublishPassphrase: <la misma VM_SRT_PASSPHRASE>
   ```
   Con `authInternalUsers`, solo `cit-001` publica y los clientes leen con el
   ticket que emite la API.
4. **Prueba de humo** desde la Jetson: llenar `unidad.env`, `./cit.sh arrancar`,
   en la VM `mosquitto_sub -u ingesta -P … -t 'cit/v1/#' -v`, mandar un
   `media.open` con los siete campos y abrir el flujo en MediaMTX.

## Fase 3 — Ingesta y API de Edge (Software/TI)

Lo que el contrato exige del lado de la VM:

- **El servicio de ingesta se suscribe con sesión persistente**
  (`clean_session=false`, `client_id` fijo, QoS 1). Con sesión limpia se
  pierde lo que el broker entregue mientras la ingesta se reinicia (se vio en
  banco).
- **Deduplicar por `sample_id`** (telemetría) y por `command_id` + `status`
  (resultados).
- Tomar el tiempo de `ts_device`, **nunca** el de llegada. Con
  `buffered:true` la muestra es vieja a propósito.
- **No recalcular `speed_kph`** (regla del ADR 1.6) y graficar distinto
  según `gnss_fix`.
- `pax_in`/`pax_out` son acumulados por arranque del módulo: un salto hacia
  abajo junto con `uptime_s` pequeño es un servicio nuevo, no gente que bajó.
- **Comandos**: la API genera `command_id` único, pone `issued_by`,
  `ts_issued` (UTC, reloj sincronizado), `ttl_s` y `reason`, y publica en
  `cmd` **sin retener**. El estado de la orden se lee de `resultado` y de
  `last_command`.
- **Video**: la API pide la sesión con `media.open` y, al recibir `aplicada`,
  emite el ticket de corta vigencia para MediaMTX, con caducidad ≤
  `duration_s`. **No reenviar `media.open` para prolongar**: la Jetson lo
  rechaza. El plan anterior (re-enviar cada 45 s) queda **anulado** por el ADR.
- Consultar la API de MediaMTX (`/v3/paths/get/cit-001`) para dar la URL
  solo cuando el flujo ya esté listo.

## Fase 4 — Endurecer

| Pendiente | Quién | Nota |
|---|---|---|
| Arranque automático al encender | Hardware | **Instalado 27-sep** (crontab `@reboot`, sin sudo). Probado con el entorno de cron |
| Permiso para `module.restart` | Hardware | **Instalado 30-sep** (`/etc/sudoers.d/cit-reinicio`, solo `systemctl reboot`) |
| Señal de ignición (`ignition`) | Hardware (R08) | Hoy `null` |
| Reinicios por watchdog (`watchdog_resets`) | Hardware (R25/R32) | Hoy `null` |
| Tope de sesiones al mes | Dirección general / Finanzas | 100 provisional en `unidad.env` |
| Cámaras IP por RTSP (Meriva MC1IP) | Hardware | Hoy una USB; `CAMARA_ID=cam0` |
| Rotar `datos/eventos.jsonl` | Hardware | Sigue creciendo sin límite (trampa 21 de AGENTS.md) |

Regla de sudoers para `module.restart` y `module.shutdown` (la pone un administrador):

```bash
echo 'nelsonkanzela ALL=(root) NOPASSWD: /usr/bin/systemctl reboot, /usr/bin/systemctl poweroff' | sudo tee /etc/sudoers.d/cit-reinicio
sudo chmod 440 /etc/sudoers.d/cit-reinicio && sudo visudo -c
```

---

## Diferencias con el ADR-001 v1.6 (para su versión 1.7)

1. **Número de comandos.** La decisión 6 lista cinco; la tabla 4 ("uno de
   los tres valores") y la conclusión 5 dicen tres. Se implementaron los cinco.
2. **Parámetros del comando.** El ciclo de vida dice que la orden de apertura
   nombra cámara y duración, pero la tabla 4 no tiene dónde. Se añadió
   `params` (opcional, solo `media.open`).
3. **Transporte del video.** El ADR dice que el equipo publica por WebRTC.
   La Jetson publica por **SRT** a MediaMTX y MediaMTX entrega por WebRTC. Se
   cumple lo que importa (solo conexiones salientes, servidor de medios
   aparte, ticket por la API); lo que cambia es el protocolo del primer tramo.
   Ventaja: sin TURN desde la Jetson y ya probado. Si se quisiera WebRTC de
   punta a punta, MediaMTX acepta WHIP.
4. **Estados del comando.** Se añadieron `en_curso` y `rechazada`, que el ADR
   no nombra.
5. **`ignition` y `watchdog_resets`** son obligatorios en la tabla 3 pero no
   existen en el hardware actual: van `null`.
6. **Inicio del servicio** para `pax_*`: se definió como el arranque del módulo.
7. **Tamaño de la muestra**: ≈ 490 B medidos contra 150 B estimados.
8. **Frecuencia con la unidad apagada** (una muestra cada 5 min): no aplica a
   la Jetson, que está apagada con la ignición apagada. Corresponde al
   elemento siempre disponible, cuando exista.
9. **Cruces individuales** (`cruce` con ID de track) ya no suben a la VM: no
   están en la tabla 3. Siguen en `datos/eventos.jsonl` (y en AWS si se
   enciende). Si se quieren en la plataforma, van en una versión 2 del contrato.
