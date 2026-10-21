#!/usr/bin/env bash
# =============================================================================
# subir.sh :: manda el codigo de la laptop a la Jetson y reinicia la unidad
#
#   ./subir.sh            sincroniza y reinicia (cit.sh reiniciar)
#   ./subir.sh --prueba   solo muestra que se mandaria, sin tocar nada
#   ./subir.sh --traer    trae a la laptop el codigo actual de la Jetson
#
# Nunca borra nada en la Jetson (no usa --delete) y nunca toca datos/,
# unidad.env ni certs/: esos viven solo en la Jetson.
# =============================================================================
set -euo pipefail

JETSON="${JETSON:-jetson}"
DESTINO="Desktop/cit/"
AQUI="$(cd "$(dirname "$0")" && pwd)/"

EXCLUIR=(
  --exclude .git/ --exclude datos/ --exclude unidad.env --exclude certs/
  --exclude __pycache__/ --exclude '*.engine' --exclude '*.onnx*' --exclude '*.pt'
  --exclude '*.mp4' --exclude '*.avi' --exclude agora_env/ --exclude yolo_env/
  --exclude ultralytics/ --exclude subir.sh
)

case "${1:-}" in
  --traer)
    rsync -av "${EXCLUIR[@]}" "$JETSON:$DESTINO" "$AQUI"
    echo; echo "Listo. Revisa con: git status"
    ;;
  --prueba)
    rsync -avn "${EXCLUIR[@]}" "$AQUI" "$JETSON:$DESTINO"
    ;;
  "")
    rsync -av "${EXCLUIR[@]}" "$AQUI" "$JETSON:$DESTINO"
    echo; echo "Reiniciando la unidad en la Jetson..."
    ssh "$JETSON" 'cd ~/Desktop/cit && ./cit.sh reiniciar && sleep 5 && ./cit.sh estado'
    ;;
  *)
    echo "Uso: ./subir.sh [--prueba | --traer]"; exit 1 ;;
esac
