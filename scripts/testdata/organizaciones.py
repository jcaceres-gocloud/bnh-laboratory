import argparse
import csv
import io
import json


CASE_NAMES = (
    "valid",
    "missing-id",
    "missing-name",
    "missing-type",
    "missing-start-date",
    "numeric-identifiers",
    "numeric-type-code",
    "invalid-start-date",
    "invalid-end-date",
    "invalid-name-type",
    "invalid-description-type",
    "invalid-root",
    "invalid-envelope",
    "invalid-flat",
)


CASOS_ESTRUCTURA = {
    "invalid-root": ["esto", "no", "es", "un", "objeto"],
    "invalid-envelope": {
        "metadata": "metadata-invalida",
        "registro": "registro-invalido",
    },
    "invalid-flat": {
        "id_organizacion": "ORG000001",
        "nombre": "Hospital Central",
        "jurisdiccion": "ARG-B",
    },
}


CASOS_SIN_CAMPO = {
    "missing-id": "id_organizacion",
    "missing-name": "nombre",
    "missing-type": "c_organizacion",
    "missing-start-date": "fecha_alta",
}


REGISTRO_OVERRIDES = {
    "numeric-identifiers": {
        "id_organizacion": 1001,
    },
    "numeric-type-code": {
        "c_organizacion": 1,
    },
    "invalid-start-date": {
        "fecha_alta": "2020-99-99",
    },
    "invalid-end-date": {
        "fecha_baja": "2025-99-99",
    },
    "invalid-name-type": {
        "nombre": 12345,
    },
    "invalid-description-type": {
        "descripcion": 12345,
    },
}


def organizacion_base():
    return {
        "metadata": {
            "jurisdiccion": "ARG-B",
            "dominio": "organizacion",
            "lote_id": "TEST-ORGANIZACIONES-001",
        },
        "registro": {
            "id_organizacion": "ORG000001",
            "nombre": "Hospital Central",
            "descripcion": "Organizacion de prueba del laboratorio",
            "c_organizacion": "01",
            "fecha_alta": "2020-01-15",
            "fecha_baja": None,
        },
    }


CAMPOS_METADATA_CSV = (
    "jurisdiccion",
    "dominio",
    "lote_id",
)

CAMPOS_REGISTRO_CSV = (
    "id_organizacion",
    "nombre",
    "descripcion",
    "c_organizacion",
    "fecha_alta",
    "fecha_baja",
)


def generar_caso(nombre):
    if nombre in CASOS_ESTRUCTURA:
        return CASOS_ESTRUCTURA[nombre]

    payload = organizacion_base()

    campo = CASOS_SIN_CAMPO.get(nombre)
    if campo:
        payload["registro"].pop(campo)
        return payload

    cambios = REGISTRO_OVERRIDES.get(nombre)
    if cambios:
        payload["registro"].update(cambios)
        return payload

    if nombre == "valid":
        return payload

    raise ValueError(f"Caso desconocido: {nombre}")


def _valor_csv(valor):
    if valor is None:
        return ""
    return valor


def payload_a_fila_csv(payload):
    metadata = payload["metadata"]
    registro = payload["registro"]

    fila = {
        campo: _valor_csv(metadata.get(campo))
        for campo in CAMPOS_METADATA_CSV
    }
    fila.update(
        {
            campo: _valor_csv(registro.get(campo))
            for campo in CAMPOS_REGISTRO_CSV
        }
    )
    return fila


def payloads_a_csv(payloads):
    filas = [payload_a_fila_csv(payload) for payload in payloads]
    columnas = [
        campo
        for campo in CAMPOS_METADATA_CSV + CAMPOS_REGISTRO_CSV
        if any(fila.get(campo) != "" for fila in filas)
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=columnas,
        lineterminator="\n",
        extrasaction="ignore",
    )
    writer.writeheader()
    for fila in filas:
        writer.writerow(fila)
    return buffer.getvalue()


def organizaciones_del_lote(payload, cantidad):
    organizaciones = []

    for indice in range(1, cantidad + 1):
        organizacion = {
            "metadata": dict(payload["metadata"]),
            "registro": dict(payload["registro"]),
        }
        organizacion["registro"]["id_organizacion"] = f"ORG{indice:06d}"
        organizaciones.append(organizacion)

    return organizaciones


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--case",
        choices=CASE_NAMES,
        default="valid",
    )
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
    )
    parser.add_argument(
        "--lote-id",
        dest="lote_id",
        default=None,
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Cantidad de Organizaciones del lote (ORG000001, ORG000002, ...)",
    )
    args = parser.parse_args()

    if args.count < 1:
        raise SystemExit("--count debe ser >= 1")

    payload = generar_caso(args.case)

    if args.lote_id:
        if not isinstance(payload, dict) or "metadata" not in payload:
            raise SystemExit(
                "El caso seleccionado no admite override de lote_id"
            )
        payload["metadata"]["lote_id"] = args.lote_id

    if args.count > 1:
        if not isinstance(payload, dict) or "registro" not in payload:
            raise SystemExit(
                "El caso seleccionado no admite múltiples Organizaciones"
            )
        organizaciones = organizaciones_del_lote(payload, args.count)
    else:
        organizaciones = (
            [payload]
            if isinstance(payload, dict) and "registro" in payload
            else None
        )

    if args.format == "csv":
        if not organizaciones:
            raise SystemExit("El caso seleccionado no se puede emitir como CSV")
        print(payloads_a_csv(organizaciones), end="")
        return

    if args.count == 1:
        salida = payload
    else:
        salida = {
            "metadata": payload["metadata"],
            "registros": [
                organizacion["registro"] for organizacion in organizaciones
            ],
        }

    print(
        json.dumps(
            salida,
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
