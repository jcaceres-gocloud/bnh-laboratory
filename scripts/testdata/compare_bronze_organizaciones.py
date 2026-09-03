import argparse
import json
from pathlib import Path


def iterar_registros(origenes):
    for origen in origenes:
        ruta = Path(origen)

        if ruta.is_dir():
            archivos = sorted(
                p
                for p in ruta.rglob("*")
                if p.is_file() and "_tmp_" not in p.name
            )
        else:
            archivos = [ruta]

        for archivo in archivos:
            texto = archivo.read_text(encoding="utf-8")
            for numero, linea in enumerate(texto.splitlines(), start=1):
                linea = linea.strip()
                if not linea:
                    continue

                try:
                    yield archivo, json.loads(linea)
                except json.JSONDecodeError as exc:
                    raise SystemExit(
                        f"JSON inválido en {archivo}:{numero}: {exc}"
                    ) from exc


def id_organizacion_de(resultado):
    registro = resultado.get("registro")
    if not isinstance(registro, dict):
        return None
    return registro.get("id_organizacion")


def comparable(resultado):
    metadata = dict(resultado.get("metadata") or {})
    metadata.pop("lote_id", None)

    return {
        "estado_validacion": resultado.get("estado_validacion"),
        "errores": resultado.get("errores"),
        "metadata": metadata,
        "registro": resultado.get("registro"),
    }


def indice_por_lote(origenes):
    encontrados = {}
    duplicados = []

    for _, resultado in iterar_registros(origenes):
        if not isinstance(resultado, dict):
            continue

        metadata = resultado.get("metadata")
        if not isinstance(metadata, dict):
            continue

        lote_id = metadata.get("lote_id")
        id_organizacion = id_organizacion_de(resultado)

        if not lote_id or not id_organizacion:
            continue

        organizaciones = encontrados.setdefault(lote_id, {})
        if id_organizacion in organizaciones:
            duplicados.append((lote_id, id_organizacion))
            continue

        organizaciones[id_organizacion] = resultado

    return encontrados, duplicados


def imprimir_lote(lote_id, organizaciones):
    print(f"{lote_id}")
    for id_organizacion in sorted(organizaciones):
        resultado = organizaciones[id_organizacion]
        print(
            f"- {id_organizacion} {resultado.get('estado_validacion')}"
        )


def diferencias_lote(referencia_id, referencia, lote_id, actual):
    fallos = []
    ids_ref = set(referencia)
    ids_act = set(actual)

    faltantes = sorted(ids_ref - ids_act)
    extras = sorted(ids_act - ids_ref)

    for id_organizacion in faltantes:
        fallos.append(f"{lote_id} organizacion faltante: {id_organizacion}")

    for id_organizacion in extras:
        fallos.append(f"{lote_id} organizacion extra: {id_organizacion}")

    for id_organizacion in sorted(ids_ref & ids_act):
        if comparable(actual[id_organizacion]) != comparable(
            referencia[id_organizacion]
        ):
            fallos.append(
                f"{lote_id} {id_organizacion} no converge con {referencia_id}"
            )

    return fallos


def comparar_lotes(por_lote, lotes, duplicados, esperadas=None):
    fallos = [
        f"duplicado {lote_id} id_organizacion={id_organizacion}"
        for lote_id, id_organizacion in duplicados
        if lote_id in lotes
    ]

    faltantes = [lote for lote in lotes if lote not in por_lote]
    if faltantes:
        presentes = ", ".join(sorted(por_lote)) or "(ninguno)"
        raise SystemExit(
            "No se encontraron en Bronze: "
            + ", ".join(faltantes)
            + f". Lotes presentes: {presentes}"
        )

    for lote_id in lotes:
        imprimir_lote(lote_id, por_lote[lote_id])
        print()

        if esperadas is not None and len(por_lote[lote_id]) != esperadas:
            fallos.append(
                f"{lote_id} tiene {len(por_lote[lote_id])} Organizaciones; "
                f"se esperaban {esperadas}"
            )

    referencia_id = lotes[0]
    referencia = por_lote[referencia_id]
    print(f"[REF] {referencia_id}")

    for lote_id in lotes[1:]:
        diffs = diferencias_lote(
            referencia_id,
            referencia,
            lote_id,
            por_lote[lote_id],
        )
        if diffs:
            fallos.extend(diffs)
            print(f"[FAIL] {lote_id} no converge con {referencia_id}")
            for diff in diffs:
                print(f"       {diff}")
            continue

        print(f"[OK]  {lote_id} converge con {referencia_id}")

    return fallos, len(referencia)


def self_check():
    import tempfile

    linea = lambda lote, organizacion, codigo: json.dumps(
        {
            "estado_validacion": "VALIDO",
            "errores": [],
            "metadata": {
                "jurisdiccion": "ARG-B",
                "dominio": "organizacion",
                "lote_id": lote,
            },
            "registro": {
                "id_organizacion": organizacion,
                "c_organizacion": codigo,
            },
        },
        ensure_ascii=False,
    )

    with tempfile.TemporaryDirectory() as tmp:
        ruta = Path(tmp) / "bronze.jsonl"
        ruta.write_text(
            "\n".join(
                [
                    linea("L-A", "ORG000001", "01"),
                    linea("L-A", "ORG000002", "01"),
                    linea("L-B", "ORG000001", "01"),
                    linea("L-B", "ORG000002", "01"),
                    linea("L-A", "ORG000001", "01"),
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        por_lote, duplicados = indice_por_lote([ruta])

        if ("L-A", "ORG000001") not in duplicados:
            raise SystemExit("self-check: no detectó duplicado")

        if set(por_lote["L-A"]) != {"ORG000001", "ORG000002"}:
            raise SystemExit("self-check: L-A no conservó ambas Organizaciones")

        ok, _ = comparar_lotes(
            {
                "L-A": {
                    "ORG000001": json.loads(linea("L-A", "ORG000001", "01")),
                    "ORG000002": json.loads(linea("L-A", "ORG000002", "01")),
                },
                "L-B": {
                    "ORG000001": json.loads(linea("L-B", "ORG000001", "01")),
                    "ORG000002": json.loads(linea("L-B", "ORG000002", "01")),
                },
            },
            ["L-A", "L-B"],
            [],
            esperadas=2,
        )
        if ok:
            raise SystemExit("self-check: convergencia válida falló")

        fail, _ = comparar_lotes(
            {
                "L-A": {
                    "ORG000001": json.loads(linea("L-A", "ORG000001", "01")),
                    "ORG000002": json.loads(linea("L-A", "ORG000002", "01")),
                },
                "L-B": {
                    "ORG000001": json.loads(linea("L-B", "ORG000001", "01")),
                    "ORG000003": json.loads(linea("L-B", "ORG000003", "01")),
                },
            },
            ["L-A", "L-B"],
            [],
        )
        texto = " ".join(fail)
        if "organizacion faltante: ORG000002" not in texto:
            raise SystemExit("self-check: no detectó organizacion faltante")
        if "organizacion extra: ORG000003" not in texto:
            raise SystemExit("self-check: no detectó organizacion extra")

        contenido, _ = comparar_lotes(
            {
                "L-A": {
                    "ORG000001": json.loads(linea("L-A", "ORG000001", "01")),
                },
                "L-B": {
                    "ORG000001": json.loads(linea("L-B", "ORG000001", "1")),
                },
            },
            ["L-A", "L-B"],
            [],
        )
        if "ORG000001 no converge" not in " ".join(contenido):
            raise SystemExit("self-check: no detectó diferencia de contenido")

    print("[OK] self-check comparador multi-organizacion")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Compara resultados Bronze de Organizacion entre canales. "
            "Agrupa por lote_id e id_organizacion. Ignora lote_id al comparar."
        )
    )
    parser.add_argument(
        "origenes",
        nargs="*",
        help="Archivos JSONL o directorios con objetos Bronze",
    )
    parser.add_argument(
        "--lotes",
        nargs="+",
        help="lote_id a comparar, por ejemplo REST-ORG-MULTI-001 GRPC-ORG-MULTI-001",
    )
    parser.add_argument(
        "--esperadas",
        type=int,
        default=None,
        help="Cantidad de Organizaciones esperadas por lote",
    )
    parser.add_argument(
        "--self-check",
        action="store_true",
        help="Verifica detección de faltantes, extras, duplicados y contenido",
    )
    args = parser.parse_args()

    if args.self_check:
        self_check()
        if not args.origenes:
            return

    if not args.origenes or not args.lotes:
        raise SystemExit("se requieren origenes y --lotes, o --self-check")

    por_lote, duplicados = indice_por_lote(args.origenes)
    fallos, cantidad = comparar_lotes(
        por_lote,
        args.lotes,
        duplicados,
        esperadas=args.esperadas,
    )

    if fallos:
        raise SystemExit(1)

    print(
        f"\n{len(args.lotes)} lotes convergen\n"
        f"{cantidad} Organizaciones por lote\n"
        "ignorando lote_id"
    )


if __name__ == "__main__":
    main()
