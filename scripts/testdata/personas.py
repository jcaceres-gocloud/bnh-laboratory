import argparse
import csv
import io
import json


CASE_NAMES = (
    "valid",
    "missing-id",
    "missing-birth-date",
    "missing-document-type",
    "missing-country",
    "missing-indigenous",
    "numeric-identifiers",
    "numeric-country-code",
    "numeric-codes",
    "invalid-birth-date",
    "invalid-death-date",
    "invalid-cuit-length",
    "invalid-cuit-nondigit",
    "invalid-root",
    "invalid-envelope",
    "invalid-flat",
    "invalid-place-type",
)


CASOS_ESTRUCTURA = {
    "invalid-root": ["esto", "no", "es", "un", "objeto"],
    "invalid-envelope": {
        "metadata": "metadata-invalida",
        "registro": "registro-invalido",
    },
    "invalid-flat": {
        "id": "P101",
        "nombre": "Carlos",
        "jurisdiccion": "ARG-B",
    },
}


CASOS_SIN_CAMPO = {
    "missing-id": "id_persona",
    "missing-birth-date": "fecha_nacimiento",
    "missing-document-type": "c_documento",
    "missing-country": "c_pais_nacimiento",
    "missing-indigenous": "c_es_indigena",
}


REGISTRO_OVERRIDES = {
    "numeric-identifiers": {
        "id_persona": 1001,
        "cuit": 20123456789,
        "nro_documento": 12345678,
    },
    "invalid-birth-date": {
        "fecha_nacimiento": "1990-99-99",
    },
    "invalid-death-date": {
        "fecha_fallecido": "2025-99-99",
    },
    "numeric-country-code": {
        "c_pais_nacimiento": 32,
    },
    "numeric-codes": {
        "id_persona": 1001,
        "cuit": 20123456789,
        "c_documento": 1,
        "nro_documento": 12345678,
        "c_pais_nacimiento": 32,
        "c_provincia_nacimiento": 6,
        "c_departamento_nacimiento": 1003,
        "c_localidad_nacimiento": 1004,
        "c_municipio_nacimiento": 1005,
        "c_fallecido": 0,
        "c_es_indigena": 1,
    },
    "invalid-cuit-length": {
        "cuit": "2012345678",
    },
    "invalid-cuit-nondigit": {
        "cuit": "2012345678A",
    },
    "invalid-place-type": {
        "lugar_nacimiento": 12345,
    },
}


def persona_base():
    return {
        "metadata": {
            "jurisdiccion": "ARG-B",
            "dominio": "persona",
            "lote_id": "TEST-PERSONAS-001",
        },
        "registro": {
            "id_persona": "P000001",
            "fecha_nacimiento": "1990-05-10",
            "cuit": "20123456789",
            "c_documento": "DNI",
            "nro_documento": "12345678",
            "c_pais_nacimiento": "ARG",
            "c_provincia_nacimiento": "B",
            "c_departamento_nacimiento": "001",
            "c_localidad_nacimiento": "001",
            "c_municipio_nacimiento": "001",
            "lugar_nacimiento": "Buenos Aires",
            "c_fallecido": "N",
            "fecha_fallecido": None,
            "c_es_indigena": "N",
        },
    }


CAMPOS_METADATA_CSV = (
    "jurisdiccion",
    "dominio",
    "lote_id",
)

CAMPOS_REGISTRO_CSV = (
    "id_persona",
    "fecha_nacimiento",
    "cuit",
    "c_documento",
    "nro_documento",
    "c_pais_nacimiento",
    "c_provincia_nacimiento",
    "c_departamento_nacimiento",
    "c_localidad_nacimiento",
    "c_municipio_nacimiento",
    "lugar_nacimiento",
    "c_fallecido",
    "fecha_fallecido",
    "c_es_indigena",
)


def generar_caso(nombre):
    if nombre in CASOS_ESTRUCTURA:
        return CASOS_ESTRUCTURA[nombre]

    payload = persona_base()

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


def payload_a_csv(payload):
    return payloads_a_csv([payload])


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


def personas_del_lote(payload, cantidad):
    personas = []

    for indice in range(1, cantidad + 1):
        persona = {
            "metadata": dict(payload["metadata"]),
            "registro": dict(payload["registro"]),
        }
        persona["registro"]["id_persona"] = f"P{indice:06d}"
        personas.append(persona)

    return personas


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
        help="Cantidad de Personas del lote (P000001, P000002, ...)",
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
                "El caso seleccionado no admite múltiples Personas"
            )
        personas = personas_del_lote(payload, args.count)
    else:
        personas = [payload] if isinstance(payload, dict) and "registro" in payload else None

    if args.format == "csv":
        if not personas:
            raise SystemExit("El caso seleccionado no se puede emitir como CSV")
        print(payloads_a_csv(personas), end="")
        return

    if args.count == 1:
        salida = payload
    else:
        salida = {
            "metadata": payload["metadata"],
            "registros": [persona["registro"] for persona in personas],
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
