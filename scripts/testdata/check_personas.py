import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from apps.flink.jobs.personas_validate import normalizar_y_validar
from scripts.testdata.personas import generar_caso


CASOS = {
    "valid": ("VALIDO", []),
    "missing-id": (
        "INVALIDO",
        ["registro.id_persona requerido"],
    ),
    "missing-birth-date": (
        "INVALIDO",
        ["registro.fecha_nacimiento requerido"],
    ),
    "missing-document-type": (
        "INVALIDO",
        ["registro.c_documento requerido"],
    ),
    "missing-country": (
        "INVALIDO",
        ["registro.c_pais_nacimiento requerido"],
    ),
    "missing-indigenous": (
        "INVALIDO",
        ["registro.c_es_indigena requerido"],
    ),
    "numeric-identifiers": (
        "VALIDO",
        [],
    ),
    "invalid-birth-date": (
        "INVALIDO",
        ["registro.fecha_nacimiento invalida"],
    ),
    "invalid-death-date": (
        "INVALIDO",
        ["registro.fecha_fallecido invalida"],
    ),
    "numeric-country-code": (
        "VALIDO",
        [],
    ),
    "numeric-codes": (
        "VALIDO",
        [],
    ),
    "invalid-cuit-length": (
        "INVALIDO",
        ["registro.cuit debe tener 11 digitos"],
    ),
    "invalid-cuit-nondigit": (
        "INVALIDO",
        ["registro.cuit debe contener solo digitos"],
    ),
    "invalid-root": (
        "INVALIDO",
        ["payload debe ser objeto"],
    ),
    "invalid-envelope": (
        "INVALIDO",
        [
            "metadata debe ser objeto",
            "registro debe ser objeto",
        ],
    ),
    "invalid-place-type": (
        "INVALIDO",
        ["registro.lugar_nacimiento debe ser texto"],
    ),
}


NORMALIZACIONES_NUMERIC_CODES = {
    "id_persona": "1001",
    "cuit": "20123456789",
    "c_documento": "1",
    "nro_documento": "12345678",
    "c_pais_nacimiento": "32",
    "c_provincia_nacimiento": "6",
    "c_departamento_nacimiento": "1003",
    "c_localidad_nacimiento": "1004",
    "c_municipio_nacimiento": "1005",
    "c_fallecido": "0",
    "c_es_indigena": "1",
}


def ejecutar_caso(nombre, estado_esperado, errores_esperados):
    payload = generar_caso(nombre)

    resultado = json.loads(
        normalizar_y_validar(
            json.dumps(payload, ensure_ascii=False)
        )
    )

    errores = set(resultado.get("errores", []))

    correcto = (
        resultado["estado_validacion"] == estado_esperado
        and set(errores_esperados).issubset(errores)
    )

    if nombre == "numeric-codes":
        registro = resultado["registro"]

        correcto = correcto and all(
            registro.get(campo) == valor
            for campo, valor in NORMALIZACIONES_NUMERIC_CODES.items()
        )

    estado = "OK" if correcto else "FAIL"

    print(
        f"[{estado}] {nombre}: "
        f"{resultado['estado_validacion']} "
        f"{resultado.get('errores', [])}"
    )

    return correcto


def main():
    resultados = [
        ejecutar_caso(nombre, estado, errores)
        for nombre, (estado, errores) in CASOS.items()
    ]

    if not all(resultados):
        raise SystemExit(1)

    print(f"\n{len(resultados)} casos OK")


if __name__ == "__main__":
    main()
