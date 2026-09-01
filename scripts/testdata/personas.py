import argparse
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
    "invalid-place-type",
)


CASOS_ESTRUCTURA = {
    "invalid-root": ["esto", "no", "es", "un", "objeto"],
    "invalid-envelope": {
        "metadata": "metadata-invalida",
        "registro": "registro-invalido",
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--case",
        choices=CASE_NAMES,
        default="valid",
    )
    args = parser.parse_args()

    print(
        json.dumps(
            generar_caso(args.case),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
