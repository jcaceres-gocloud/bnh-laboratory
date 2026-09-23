#!/usr/bin/env bash

set -euo pipefail

TOTAL="${1:-10}"
JURISDICCION="${JURISDICCION:-ARG-B}"
LOTE_ID="BENCH-GRPC-${TOTAL}-$(date +%Y%m%d-%H%M%S)"

PREFIX="personas/${JURISDICCION}/"

echo
echo "======================================================"
echo "BNH - Benchmark gRPC -> Bronze"
echo "======================================================"
echo "Personas:      $TOTAL"
echo "Jurisdicción:  $JURISDICCION"
echo "Lote:          $LOTE_ID"
echo "Destino:       s3://bnh-bronze/$PREFIX"
echo

T0=$(date +%s%3N)

echo "[1/2] Enviando por gRPC..."

TOTAL_PERSONAS="$TOTAL" \
GRPC_CASE=valid \
LOTE_ID="$LOTE_ID" \
docker compose -f docker-compose-demo.yml \
  run --rm grpc-client

T1=$(date +%s%3N)

GRPC_MS=$((T1 - T0))

echo
echo "gRPC finalizó en $GRPC_MS ms"
echo
echo "[2/2] Esperando que las $TOTAL Personas estén disponibles"
echo "      en archivos part-* definitivos de Bronze..."

while true; do

    COUNT=$(
        docker exec bnh-demo-ozone-scm \
          ozone sh key list /s3v/bnh-bronze \
          --prefix "$PREFIX" 2>/dev/null \
        | python3 -c '
import sys, json

try:
    data = json.load(sys.stdin)
    for obj in data:
        name = obj.get("name", "")
        base = name.rsplit("/", 1)[-1]
        if base.startswith("part-"):
            print(name)
except Exception:
    pass
' \
        | while read -r KEY; do
            docker exec bnh-demo-ozone-scm \
              ozone sh key cat "/s3v/bnh-bronze/$KEY" 2>/dev/null || true
          done \
        | grep -c "\"lote_id\": \"$LOTE_ID\"" || true
    )

    NOW=$(date +%s%3N)
    ELAPSED=$((NOW - T0))

    printf "\rBronze: %s/%s registros | %.3f s" \
        "$COUNT" "$TOTAL" "$(awk "BEGIN {print $ELAPSED/1000}")"

    if [ "$COUNT" -ge "$TOTAL" ]; then
        break
    fi

    sleep 1
done

T2=$(date +%s%3N)

TOTAL_MS=$((T2 - T0))
BRONZE_MS=$((T2 - T1))

TOTAL_S=$(awk "BEGIN {printf \"%.3f\", $TOTAL_MS/1000}")
GRPC_S=$(awk "BEGIN {printf \"%.3f\", $GRPC_MS/1000}")
BRONZE_S=$(awk "BEGIN {printf \"%.3f\", $BRONZE_MS/1000}")
RATE=$(awk "BEGIN {printf \"%.2f\", $TOTAL / ($TOTAL_MS/1000)}")

echo
echo
echo "======================================================"
echo "RESULTADO"
echo "======================================================"
echo "Personas:                  $TOTAL"
echo "Tiempo envío gRPC:         $GRPC_S s"
echo "Espera post-gRPC Bronze:   $BRONZE_S s"
echo "Tiempo total E2E:          $TOTAL_S s"
echo "Throughput E2E:            $RATE personas/s"
echo "Lote:                      $LOTE_ID"
echo "======================================================"
