#!/usr/bin/env python3
import json
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FLOW_PATH = ROOT / "nifi/flows/BNH_-_Personas_File_Ingestion.json"
NIFI_URL = "https://localhost:8443"
USERNAME = "admin"
PASSWORD = "BnhLaboratory1234"
FLOW_NAME = "BNH - Personas File Ingestion"
# Solo laboratorio local: NiFi usa un certificado autofirmado.
# En un entorno real debe validarse la cadena TLS.
SSL_CTX = ssl._create_unverified_context()


def request(method, path, token=None, data=None, content_type=None, headers=None):
    url = NIFI_URL + path
    body = data if isinstance(data, (bytes, type(None))) else data.encode("utf-8")
    req = urllib.request.Request(url, data=body, method=method)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if content_type:
        req.add_header("Content-Type", content_type)
    if headers:
        for clave, valor in headers.items():
            req.add_header(clave, valor)

    try:
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=60) as resp:
            payload = resp.read()
            if not payload:
                return None
            ctype = resp.headers.get("Content-Type", "")
            if "json" in ctype or payload[:1] in (b"{", b"["):
                return json.loads(payload.decode("utf-8"))
            return payload.decode("utf-8")
    except urllib.error.HTTPError as exc:
        detalle = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"{method} {path} -> HTTP {exc.code}: {detalle}") from exc


def esperar_nifi(intentos=60):
    for _ in range(intentos):
        try:
            token = request(
                "POST",
                "/nifi-api/access/token",
                data=urllib.parse.urlencode(
                    {
                        "username": USERNAME,
                        "password": PASSWORD,
                    }
                ),
                content_type="application/x-www-form-urlencoded",
            )
            if token:
                return token
        except SystemExit:
            time.sleep(2)
        except OSError:
            time.sleep(2)
    raise SystemExit("NiFi no respondió a tiempo")


def revision(entidad):
    return entidad["revision"]


def process_group_root(token):
    return request("GET", "/nifi-api/process-groups/root", token=token)["id"]


def listar_grupos(token, parent_id):
    data = request(
        "GET",
        f"/nifi-api/process-groups/{parent_id}/process-groups",
        token=token,
    )
    return data.get("processGroups", [])


def detener_grupo(token, group_id):
    request(
        "PUT",
        f"/nifi-api/flow/process-groups/{group_id}",
        token=token,
        data=json.dumps({"id": group_id, "state": "STOPPED"}),
        content_type="application/json",
    )


def borrar_grupo(token, group_id):
    entidad = request(
        "GET",
        f"/nifi-api/process-groups/{group_id}",
        token=token,
    )
    version = entidad["revision"]["version"]
    client_id = entidad["revision"].get("clientId", "bnh-lab")
    request(
        "DELETE",
        f"/nifi-api/process-groups/{group_id}?version={version}&clientId={client_id}&disconnectedNodeAcknowledged=true",
        token=token,
    )


def importar_flujo(token, parent_id):
    boundary = "----BnhNiFiBoundary"
    contenido = FLOW_PATH.read_bytes()
    campos = [
        ("clientId", "bnh-lab"),
        ("groupName", FLOW_NAME),
        ("positionX", "0.0"),
        ("positionY", "0.0"),
        ("disconnectedNodeAcknowledged", "true"),
    ]
    partes = []
    for nombre, valor in campos:
        partes.append(
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{nombre}"\r\n\r\n'
            f"{valor}\r\n".encode("utf-8")
        )
    partes.append(
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; '
        f'filename="{FLOW_PATH.name}"\r\n'
        "Content-Type: application/json\r\n\r\n".encode("utf-8")
        + contenido
        + b"\r\n"
    )
    partes.append(f"--{boundary}--\r\n".encode("utf-8"))
    return request(
        "POST",
        f"/nifi-api/process-groups/{parent_id}/process-groups/upload",
        token=token,
        data=b"".join(partes),
        content_type=f"multipart/form-data; boundary={boundary}",
    )


def habilitar_servicios(token, group_id):
    data = request(
        "GET",
        f"/nifi-api/flow/process-groups/{group_id}/controller-services",
        token=token,
    )
    for servicio in data.get("controllerServices", []):
        componente = servicio["component"]
        if componente.get("state") == "ENABLED":
            continue
        request(
            "PUT",
            f"/nifi-api/controller-services/{componente['id']}/run-status",
            token=token,
            data=json.dumps(
                {
                    "revision": revision(servicio),
                    "state": "ENABLED",
                    "disconnectedNodeAcknowledged": True,
                }
            ),
            content_type="application/json",
        )


def esperar_servicios(token, group_id, intentos=30):
    for _ in range(intentos):
        data = request(
            "GET",
            f"/nifi-api/flow/process-groups/{group_id}/controller-services",
            token=token,
        )
        estados = [
            s["component"].get("state")
            for s in data.get("controllerServices", [])
        ]
        if estados and all(estado == "ENABLED" for estado in estados):
            return
        time.sleep(2)
    raise SystemExit("Controller Services de NiFi no quedaron ENABLED")


def iniciar_grupo(token, group_id):
    request(
        "PUT",
        f"/nifi-api/flow/process-groups/{group_id}",
        token=token,
        data=json.dumps({"id": group_id, "state": "RUNNING"}),
        content_type="application/json",
    )


def main():
    if not FLOW_PATH.exists():
        raise SystemExit(f"No existe {FLOW_PATH}")

    print("Esperando NiFi...")
    token = esperar_nifi()
    root_id = process_group_root(token)

    for grupo in listar_grupos(token, root_id):
        nombre = grupo["component"]["name"]
        if nombre != FLOW_NAME:
            continue
        group_id = grupo["id"]
        print(f"Eliminando flujo previo {group_id}")
        detener_grupo(token, group_id)
        time.sleep(2)
        borrar_grupo(token, group_id)

    print(f"Importando {FLOW_PATH.name}")
    importado = importar_flujo(token, root_id)
    group_id = importado["id"]

    print(f"Habilitando Controller Services en {group_id}")
    habilitar_servicios(token, group_id)
    esperar_servicios(token, group_id)

    print("Iniciando procesadores")
    iniciar_grupo(token, group_id)
    print(f"OK flujo NiFi importado: {group_id}")


if __name__ == "__main__":
    main()
