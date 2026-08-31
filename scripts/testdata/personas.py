import argparse
import json


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
    payload = persona_base()

    if nombre == "valid":
        return payload

    if nombre == "missing-id":
        payload["registro"].pop("id_persona")
        return payload

    if nombre == "numeric-identifiers":
        payload["registro"]["id_persona"] = 1001
        payload["registro"]["cuit"] = 20123456789
        payload["registro"]["nro_documento"] = 12345678
        return payload

    raise ValueError(f"Caso desconocido: {nombre}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--case",
        choices=["valid", "missing-id", "numeric-identifiers"],
        default="valid",
    )
    args = parser.parse_args()

    payload = generar_caso(args.case)

    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
