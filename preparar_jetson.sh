#!/usr/bin/env bash
# =============================================================================
# CIT Mobility :: preparar_jetson.sh
#
#   Deja la Jetson lista para cit.sh arrancar despues de copiar el codigo a
#   ~/Desktop/cit. Se puede correr las veces que haga falta: solo hace lo
#   que falta.
#
#     1. Revisa DeepStream, pyds y los plugins de GStreamer que usa el contador
#     2. Descarga PeopleNet de NGC a DeepStream-Yolo/peoplenet/ (89 MB)
#     3. Crea config_tracker_NvDCF.yml a partir del de muestra de DeepStream
#     4. Crea datos/ y, si no existe, unidad.env desde la plantilla
#     5. (--motor) construye el motor TensorRT de PeopleNet. Tarda minutos;
#        NO interrumpir: deja el archivo corrupto.
#
#   Uso, en la Jetson:
#     cd ~/Desktop/cit && ./preparar_jetson.sh            # revisa y descarga
#     cd ~/Desktop/cit && ./preparar_jetson.sh --motor    # ademas el motor
# =============================================================================
set -u

CIT="$(cd "$(dirname "$0")" && pwd)"
DS="$CIT/DeepStream-Yolo"
PN="$DS/peoplenet"
DSROOT=/opt/nvidia/deepstream/deepstream
URL_PEOPLENET="https://api.ngc.nvidia.com/v2/models/nvidia/tao/peoplenet/versions/deployable_quantized_onnx_v2.6.3/zip"
FALTA=0

ok()    { echo "  [ok]    $*"; }
mal()   { echo "  [FALTA] $*"; FALTA=1; }
aviso() { echo "  [aviso] $*"; }

echo "== 1. Dependencias =================================================="
[ -d "$DSROOT" ] && ok "DeepStream en $DSROOT ($(cat $DSROOT/version 2>/dev/null | head -1))" \
                 || mal "DeepStream no esta en $DSROOT"
python3 -c "import pyds" 2>/dev/null && ok "pyds" \
    || mal "pyds  ->  pip3 install ~/pyds-1.1.11-py3-none-linux_aarch64.whl"
python3 -c "import gi; gi.require_version('Gst','1.0'); gi.require_version('GstVideo','1.0')" 2>/dev/null \
    && ok "PyGObject (gi)" || mal "PyGObject  ->  sudo apt install -y python3-gi python3-gst-1.0 gir1.2-gst-plugins-base-1.0"
python3 -c "import paho.mqtt" 2>/dev/null && ok "paho-mqtt" || mal "paho-mqtt  ->  pip3 install paho-mqtt"
command -v tmux >/dev/null && ok "tmux" || mal "tmux  ->  sudo apt install -y tmux"
for p in v4l2src videoconvert nvvideoconvert nvstreammux nvinfer nvtracker nvdsanalytics \
         valve capssetter x264enc h264parse mpegtsmux udpsink udpsrc srtsink; do
    if gst-inspect-1.0 "$p" >/dev/null 2>&1; then ok "gst: $p"; else
        case "$p" in
            x264enc)          mal "gst: $p  ->  sudo apt install -y gstreamer1.0-plugins-ugly" ;;
            srtsink|mpegtsmux|h264parse) mal "gst: $p  ->  sudo apt install -y gstreamer1.0-plugins-bad" ;;
            capssetter|valve|udpsink|udpsrc|v4l2src) mal "gst: $p  ->  sudo apt install -y gstreamer1.0-plugins-good" ;;
            *)                mal "gst: $p  (parte de DeepStream)" ;;
        esac
    fi
done
ls /dev/video* >/dev/null 2>&1 && ok "camara: $(ls /dev/video* | tr '\n' ' ')" || aviso "no hay /dev/video*: conecta la camara"

echo
echo "== 2. PeopleNet ======================================================"
mkdir -p "$PN"
if [ -s "$PN/resnet34_peoplenet.onnx" ]; then
    ok "ya esta: $PN/resnet34_peoplenet.onnx"
else
    tmp="$(mktemp -d)"
    echo "  descargando de NGC (89 MB)..."
    if wget -q --show-progress -O "$tmp/peoplenet.zip" "$URL_PEOPLENET" \
       || curl -fL -o "$tmp/peoplenet.zip" "$URL_PEOPLENET"; then
        if command -v unzip >/dev/null; then unzip -o -q "$tmp/peoplenet.zip" -d "$tmp/x"
        else python3 -c "import zipfile,sys; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])" "$tmp/peoplenet.zip" "$tmp/x"; fi
        onnx="$(find "$tmp/x" -name '*.onnx' | head -1)"
        calib="$(find "$tmp/x" -iname '*int8*.txt' | head -1)"
        etiq="$(find "$tmp/x" -name 'labels.txt' | head -1)"
        conf="$(find "$tmp/x" -name 'nvinfer_config.txt' | head -1)"
        [ -n "$onnx" ]  && cp "$onnx"  "$PN/resnet34_peoplenet.onnx"
        [ -n "$calib" ] && cp "$calib" "$PN/resnet34_peoplenet_int8.txt"
        [ -n "$etiq" ]  && cp "$etiq"  "$PN/labels.txt"
        [ -n "$conf" ]  && cp "$conf"  "$PN/nvinfer_config.txt"
        echo "  contenido del zip:"; find "$tmp/x" -type f -printf '    %P\n'
    else
        mal "no se pudo descargar PeopleNet (¿internet en la Jetson?). El contador usara YOLO26 si esta."
    fi
    rm -rf "$tmp"
fi
[ -s "$PN/labels.txt" ] || printf 'person\nbag\nface\n' > "$PN/labels.txt"
[ -s "$PN/resnet34_peoplenet.onnx" ]     && ok "onnx"            || mal "peoplenet/resnet34_peoplenet.onnx"
[ -s "$PN/resnet34_peoplenet_int8.txt" ] && ok "calibracion INT8" || mal "peoplenet/resnet34_peoplenet_int8.txt (sin ella INT8 falla)"

echo
echo "== 3. Tracker NvDCF ==================================================="
MUESTRA="$DSROOT/samples/configs/deepstream-app/config_tracker_NvDCF_perf.yml"
if [ -s "$DS/config_tracker_NvDCF.yml" ]; then
    ok "ya esta: config_tracker_NvDCF.yml"
elif [ -s "$MUESTRA" ]; then
    cp "$MUESTRA" "$DS/config_tracker_NvDCF.yml"
    # Valores de AGENTS.md 4.3
    sed -i -E \
        -e 's/^(\s*minDetectorConfidence:\s*).*/\10.0430/' \
        -e 's/^(\s*minTrackerConfidence:\s*).*/\10.4009/' \
        -e 's/^(\s*probationAge:\s*).*/\12/' \
        -e 's/^(\s*maxShadowTrackingAge:\s*).*/\151/' \
        -e 's/^(\s*maxTargetsPerStream:\s*).*/\1150/' \
        "$DS/config_tracker_NvDCF.yml"
    ok "creado desde $MUESTRA"
else
    mal "no encuentro $MUESTRA"
fi

echo
echo "== 4. Carpetas y configuracion ========================================"
mkdir -p "$CIT/datos" && ok "datos/"
chmod +x "$CIT/cit.sh" 2>/dev/null
if [ -f "$CIT/unidad.env" ]; then
    ok "unidad.env existe"
else
    cp "$CIT/unidad.env.ejemplo" "$CIT/unidad.env" && chmod 600 "$CIT/unidad.env"
    aviso "unidad.env creado desde la plantilla: LLENA VM_HOST y contrasenas (nano unidad.env)"
fi

if [ "${1:-}" = "--motor" ]; then
    echo
    echo "== 5. Motor TensorRT de PeopleNet (varios minutos, NO interrumpir) ==="
    if [ -s "$PN/resnet34_peoplenet.onnx_b1_gpu0_int8.engine" ]; then
        ok "el motor ya existe"
    else
        cd "$DS" && gst-launch-1.0 -e videotestsrc num-buffers=3 ! nvvideoconvert ! \
            'video/x-raw(memory:NVMM),format=NV12,width=640,height=480' ! m.sink_0 \
            nvstreammux name=m batch-size=1 width=640 height=480 ! \
            nvinfer config-file-path="$DS/config_infer_primary_peoplenet.txt" ! fakesink
        ls -la "$PN"/*.engine 2>/dev/null
        [ -s "$PN/resnet34_peoplenet.onnx_b1_gpu0_int8.engine" ] && ok "motor listo" \
            || aviso "el motor no quedo con el nombre esperado; revisa 'ls $PN $DS' y ajusta model-engine-file"
    fi
fi

echo
if [ "$FALTA" = 0 ]; then
    echo "Todo listo. Siguiente: ./cit.sh arrancar   (y ./cit.sh estado)"
else
    echo "Hay cosas en [FALTA]. Instala lo indicado y vuelve a correr ./preparar_jetson.sh"
fi
