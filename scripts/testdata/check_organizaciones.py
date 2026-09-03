import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from apps.flink.jobs.organizaciones_validate import normalizar_y_validar
from scripts.testdata.organizaciones import generar_caso


CASOS = {
    "valid": ("VALIDO", []),
    "missing-id": (
        "INVALIDO",
        ["registro.id_organizacion requerido"],
    ),
    "missing-name": (
        "INVALIDO",
        ["registro.nombre requerido"],
    ),
    "missing-type": (
        "INVALIDO",
        ["registro.c_organizacion requerido"],
    ),
    "missing-start-date": (
        "INVALIDO",
        ["registro.fecha_alta requerido"],
    ),
    "numeric-identifiers": (
        "VALIDO",
        [],
    ),
    "numeric-type-code": (
        "VALIDO",
        [],
    ),
    "invalid-start-date": (
        "INVALIDO",
        ["registro.fecha_alta invalida"],
    ),
    "invalid-end-date": (
        "INVALIDO",
        ["registro.fecha_baja invalida"],
    ),
    "invalid-name-type": (
        "INVALIDO",
        ["registro.nombre debe ser texto"],
    ),
    "invalid-description-type": (
        "INVALIDO",
        ["registro.descripcion debe ser texto"],
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
    "invalid-flat": (
        "INVALIDO",
        ["se requiere envelope {metadata, registro}"],
    ),
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

    if nombre == "numeric-identifiers":
        correcto = (
            correcto
            and resultado.get("registro", {}).get("id_organizacion") == "1001"
        )

    if nombre == "numeric-type-code":
        correcto = (
            correcto
            and resultado.get("registro", {}).get("c_organizacion") == "1"
        )

    if nombre == "valid":
        registro = resultado.get("registro", {})
        esperado = {
            "id_organizacion",
            "nombre",
            "descripcion",
            "c_organizacion",
            "fecha_alta",
            "fecha_baja",
        }
        correcto = (
            correcto
            and esperado.issubset(registro)
            and registro.get("fecha_baja") is None
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
