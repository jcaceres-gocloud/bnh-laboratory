# bnh-laboratory
Laboratory from bnh infrastructure


## Etapa 1 — Apache Kafka

El objetivo de esta etapa es validar un Kafka local con Docker Compose y comprobar:

* que el broker levanta correctamente;
* que podemos conectarnos desde un cliente;
* crear un topic;
* crear varias particiones;
* producir eventos con `key`;
* consumirlos;
* observar cómo Kafka distribuye los eventos entre particiones.

### Requisitos

Tener instalados:

```bash
docker --version
docker compose version
```

Este laboratorio fue probado inicialmente en:

* Debian 13
* Ubuntu 24.04

---

## 1. Configuración de Kafka

El archivo `compose.yaml` contiene un único broker Kafka ejecutando también como controller mediante KRaft.

```yaml
services:
  kafka:
    image: apache/kafka:4.3.0
    container_name: bnh-kafka
    hostname: kafka

    ports:
      - "9092:9092"

    environment:
      KAFKA_NODE_ID: 1

      KAFKA_PROCESS_ROLES: broker,controller

      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: >
        CONTROLLER:PLAINTEXT,
        INTERNAL:PLAINTEXT,
        EXTERNAL:PLAINTEXT

      KAFKA_LISTENERS: >
        CONTROLLER://:29093,
        INTERNAL://:19092,
        EXTERNAL://:9092

      KAFKA_ADVERTISED_LISTENERS: >
        INTERNAL://kafka:19092,
        EXTERNAL://localhost:9092

      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      KAFKA_CONTROLLER_QUORUM_VOTERS: >
        1@kafka:29093

      CLUSTER_ID: 4L6g3nShT-eMCtK--X86sw

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: 1

      KAFKA_LOG_DIRS: /var/lib/kafka/data

    volumes:
      - kafka_data:/var/lib/kafka/data

volumes:
  kafka_data:
```

Los listeners quedan preparados para:

```text
Host local             → localhost:9092
Futuros contenedores   → kafka:19092
```

---

## 2. Validar el Compose

### Para qué

Verificar que Docker Compose puede interpretar correctamente el archivo antes de levantar los servicios.

### Comando

```bash
docker compose config
```

### Resultado esperado

Debe mostrar la configuración expandida sin errores.

Entre otras cosas deberían aparecer:

```text
container_name: bnh-kafka
image: apache/kafka:4.3.0
published: "9092"
```

---

## 3. Levantar Kafka

### Para qué

Crear la red, el volumen persistente y arrancar el broker Kafka.

### Comando

```bash
docker compose up -d kafka
```

### Resultado esperado

Algo similar a:

```text
Image apache/kafka:4.3.0         Pulled
Volume ..._kafka_data            Created
Network ..._default              Created
Container bnh-kafka              Started
```

Verificar:

```bash
docker ps
```

Debe aparecer:

```text
bnh-kafka
```

con estado:

```text
Up
```

y el puerto:

```text
0.0.0.0:9092->9092/tcp
```

---

## 4. Verificar que Kafka terminó de iniciar

### Para qué

Que el contenedor esté `Up` no garantiza que Kafka internamente haya terminado de inicializar.

### Comando

```bash
docker logs --tail 100 bnh-kafka
```

### Resultado esperado

Buscar estas líneas o equivalentes:

```text
The broker has been unfenced
Transitioning from RECOVERY to RUNNING
Endpoint is now READY
Transition from STARTING to STARTED
Kafka Server started
```

Si aparece:

```text
Kafka Server started
```

el broker está operativo.

---

## 5. Comprobar conexión con Kafka

### Para qué

Validar que un cliente puede conectarse realmente al broker.

### Comando

```bash
docker exec bnh-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### Resultado esperado

La primera vez no debería mostrar ningún topic.

Una salida vacía es correcta.

---

## 6. Crear el topic `bnh.personas`

### Para qué

Crear el primer canal de eventos del laboratorio.

Vamos a utilizar tres particiones para poder observar cómo Kafka distribuye los registros.

### Comando

```bash
docker exec bnh-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic bnh.personas \
  --partitions 3 \
  --replication-factor 1
```

### Resultado esperado

```text
Created topic bnh.personas.
```

Puede aparecer además un warning relacionado con nombres de topics que utilizan `.` o `_`. Para este laboratorio no afecta.

Usamos:

```text
replication-factor = 1
```

porque actualmente tenemos un solo broker.

---

## 7. Inspeccionar el topic

### Para qué

Comprobar que existen las tres particiones.

### Comando

```bash
docker exec bnh-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic bnh.personas
```

### Resultado esperado

Debe contener:

```text
PartitionCount: 3
ReplicationFactor: 1
```

y:

```text
Partition: 0
Partition: 1
Partition: 2
```

Como tenemos un único broker, las tres particiones deberían mostrar:

```text
Leader: 1
Replicas: 1
Isr: 1
```

La situación actual es:

```text
Kafka Cluster
└── Broker 1
    ├── bnh.personas / Partition 0
    ├── bnh.personas / Partition 1
    └── bnh.personas / Partition 2
```

---

## 8. Abrir un producer Kafka

### Para qué

Enviar mensajes manualmente al topic usando una `key` por persona.

### Comando

```bash
docker exec -it bnh-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic bnh.personas \
  --property parse.key=true \
  --property key.separator=":"
```

El proceso queda esperando mensajes.

---

## 9. Publicar eventos

Ingresar estas líneas:

```text
P001:{"id":"P001","nombre":"Ana","jurisdiccion":"ARG-B"}
P002:{"id":"P002","nombre":"Juan","jurisdiccion":"ARG-B"}
P003:{"id":"P003","nombre":"Lucia","jurisdiccion":"ARG-B"}
P004:{"id":"P004","nombre":"Pedro","jurisdiccion":"ARG-B"}
P005:{"id":"P005","nombre":"Maria","jurisdiccion":"ARG-B"}
P006:{"id":"P006","nombre":"Sofia","jurisdiccion":"ARG-B"}
```

Cada línea tiene:

```text
KEY:VALUE
```

Ejemplo:

```text
P001:{"id":"P001", ...}
```

donde:

```text
P001                         → key
{"id":"P001", ...}           → value
```

Salir del producer con:

```text
Ctrl+C
```

---

## 10. Consumir los eventos

### Para qué

Leer los mensajes desde el inicio y observar:

* key;
* partición;
* offset;
* contenido.

### Comando

```bash
docker exec bnh-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic bnh.personas \
  --from-beginning \
  --max-messages 6 \
  --formatter-property print.key=true \
  --formatter-property print.partition=true \
  --formatter-property print.offset=true
```

### Resultado esperado

Los mensajes deberían estar distribuidos entre las tres particiones.

En la primera ejecución del laboratorio obtuvimos:

```text
Partition:0 Offset:0 P003 {"id":"P003","nombre":"Lucia","jurisdiccion":"ARG-B"}
Partition:0 Offset:1 P005 {"id":"P005","nombre":"Maria","jurisdiccion":"ARG-B"}

Partition:1 Offset:0 P002 {"id":"P002","nombre":"Juan","jurisdiccion":"ARG-B"}
Partition:1 Offset:1 P006 {"id":"P006","nombre":"Sofia","jurisdiccion":"ARG-B"}

Partition:2 Offset:0 P001 {"id":"P001","nombre":"Ana","jurisdiccion":"ARG-B"}
Partition:2 Offset:1 P004 {"id":"P004","nombre":"Pedro","jurisdiccion":"ARG-B"}
```

La distribución exacta debe ser consistente para las mismas keys y configuración, pero lo importante a validar es que Kafka tenga los seis mensajes distribuidos entre las particiones.

Conceptualmente:

```text
bnh.personas

Partition 0
├── offset 0
└── offset 1

Partition 1
├── offset 0
└── offset 1

Partition 2
├── offset 0
└── offset 1
```

Los offsets son independientes para cada partición.

---

## 11. Detener el laboratorio

### Mantener los datos

Para detener y eliminar los contenedores y la red, conservando el volumen Kafka:

```bash
docker compose down
```

Al volver a ejecutar:

```bash
docker compose up -d kafka
```

los datos deberían seguir disponibles.

### Reiniciar completamente

Solo cuando se quiera eliminar también toda la información almacenada por Kafka:

```bash
docker compose down -v
```

Esto elimina el volumen:

```text
kafka_data
```

y la próxima ejecución comienza desde cero.

---

## Estado de la etapa

Al completar estos pasos queda validado:

```text
Docker Compose
      ↓
Kafka 4.3 / KRaft
      ↓
Topic bnh.personas
      ↓
3 particiones
      ↓
Producer manual
      ↓
Eventos con key
      ↓
Consumer manual
      ↓
Distribución por particiones
```

Siguiente etapa:

```text
Producer Python
      ↓
Kafka
      ↓
Consumer Python
```

El `compose.yaml` continuará creciendo sobre esta misma base; no se crearán laboratorios aislados para cada tecnología.

---
---

## Etapa 2 — Producer y Consumer Python

Objetivo de esta etapa:

* publicar eventos Kafka desde Python;
* consumirlos desde Python;
* verificar particiones y offsets;
* comprobar consumer groups;
* validar reparto de particiones entre múltiples consumers;
* comprobar rebalance cuando un consumer se cae.

---

## 1. Estructura

```text
apps/
├── producer/
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
│
└── consumer/
    ├── Dockerfile
    ├── consumer.py
    └── requirements.txt
```

---

## 2. Producer Python

### `apps/producer/requirements.txt`

```txt
confluent-kafka==2.15.0
```

### `apps/producer/Dockerfile`

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "producer.py"]
```

### `apps/producer/producer.py`

```python
import json

from confluent_kafka import Producer


producer = Producer(
    {
        "bootstrap.servers": "kafka:19092",
    }
)

personas = [
    {"id": "P101", "nombre": "Carlos", "jurisdiccion": "ARG-B"},
    {"id": "P102", "nombre": "Laura", "jurisdiccion": "ARG-B"},
    {"id": "P103", "nombre": "Martin", "jurisdiccion": "ARG-B"},
]


def delivery_report(err, msg):
    if err:
        print(f"ERROR: {err}")
        return

    print(
        f"OK key={msg.key().decode()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


for persona in personas:
    producer.produce(
        topic="bnh.personas",
        key=persona["id"],
        value=json.dumps(persona),
        callback=delivery_report,
    )

producer.flush()
```

### Qué valida

Cada persona se publica como un evento independiente en:

```text
bnh.personas
```

usando el ID como `key`.

Ejemplo:

```text
P101 → Kafka
P102 → Kafka
P103 → Kafka
```

El producer usa:

```text
kafka:19092
```

porque corre dentro de la misma red Docker que Kafka.

---

## 3. Consumer Python

### `apps/consumer/requirements.txt`

```txt
confluent-kafka==2.15.0
```

### `apps/consumer/Dockerfile`

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

CMD ["python", "consumer.py"]
```

### `apps/consumer/consumer.py`

```python
import json

from confluent_kafka import Consumer


consumer = Consumer(
    {
        "bootstrap.servers": "kafka:19092",
        "group.id": "bnh-personas-consumer",
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe(["bnh.personas"])

print("Esperando eventos de bnh.personas...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"ERROR: {msg.error()}")
            continue

        persona = json.loads(msg.value().decode("utf-8"))

        print(
            f"key={msg.key().decode()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"persona={persona}"
        )

finally:
    consumer.close()
```

---

## 4. Servicios en `compose.yaml`

Agregar:

```yaml
  producer:
    build:
      context: ./apps/producer
    container_name: bnh-producer
    depends_on:
      - kafka
    restart: "no"

  consumer:
    build:
      context: ./apps/consumer
    container_name: bnh-consumer
    depends_on:
      - kafka
    restart: "no"
```

---

## 5. Validar Compose

### Para qué

Confirmar que los nuevos servicios están correctamente definidos.

```bash
docker compose config
```

### Resultado esperado

Deben aparecer:

```text
producer
consumer
kafka
```

sin errores de configuración.

---

## 6. Construir producer

```bash
docker compose build producer
```

### Resultado esperado

```text
Image bnh-laboratory-producer Built
```

---

## 7. Ejecutar producer

```bash
docker compose run --rm producer
```

### Resultado esperado

Algo similar a:

```text
OK key=P103 partition=1 offset=2
OK key=P102 partition=2 offset=2
OK key=P101 partition=0 offset=2
```

Las particiones concretas dependen de la key y configuración actual.

---

## 8. Verificar mensajes desde Kafka

```bash
docker exec bnh-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic bnh.personas \
  --from-beginning \
  --formatter-property print.key=true \
  --formatter-property print.partition=true \
  --formatter-property print.offset=true
```

### Para qué

Confirmar que los eventos producidos desde Python fueron persistidos correctamente por Kafka.

---

## 9. Construir consumer

```bash
docker compose build consumer
```

### Resultado esperado

```text
Image bnh-laboratory-consumer Built
```

---

## 10. Ejecutar consumer

```bash
docker compose run --rm consumer
```

### Resultado esperado

El consumer lee los eventos existentes y luego queda esperando nuevos mensajes.

Ejemplo:

```text
Esperando eventos de bnh.personas...

key=P001 partition=2 offset=0 persona={...}
key=P004 partition=2 offset=1 persona={...}
key=P102 partition=2 offset=2 persona={...}
```

No termina automáticamente.

Salir con:

```text
Ctrl+C
```

---

## 11. Probar consumo en vivo

Dejar el consumer ejecutándose.

En otra terminal:

```bash
docker compose run --rm producer
```

### Resultado esperado

El producer publica nuevos eventos y el consumer los muestra automáticamente.

Esto valida:

```text
Producer Python
      ↓
    Kafka
      ↓
Consumer Python
```

---

## 12. Verificar Consumer Group

Con el consumer todavía corriendo:

```bash
docker exec bnh-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group bnh-personas-consumer
```

### Resultado esperado

Ejemplo:

```text
PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
0          4               4               0
1          4               4               0
2          4               4               0
```

### Interpretación

`CURRENT-OFFSET`

Posición hasta la que avanzó el consumer group.

`LOG-END-OFFSET`

Próxima posición disponible en la partición.

`LAG`

Cantidad de mensajes pendientes.

Si:

```text
LAG = 0
```

el consumer está al día.

---

## 13. Probar múltiples consumers

Mantener el primer consumer ejecutándose y abrir un segundo:

```bash
docker compose run --rm consumer
```

Ambos utilizan:

```text
group.id = bnh-personas-consumer
```

por lo tanto Kafka los considera parte del mismo consumer group.

Volver a inspeccionar:

```bash
docker exec bnh-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group bnh-personas-consumer
```

### Resultado esperado

Con 3 particiones y 2 consumers:

```text
Consumer A
├── Partition 0
└── Partition 1

Consumer B
└── Partition 2
```

La distribución concreta puede variar.

---

## 14. Probar paralelismo

Con los dos consumers ejecutándose:

```bash
docker compose run --rm producer
```

En la prueba realizada:

```text
P101 → Partition 0
P102 → Partition 2
P103 → Partition 1
```

Por lo tanto:

```text
Consumer A
├── P101
└── P103

Consumer B
└── P102
```

Cada consumer procesa solamente las particiones que Kafka le asignó.

---

## 15. Probar rebalance por caída

Detener uno de los consumers:

```text
Ctrl+C
```

Después:

```bash
docker exec bnh-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group bnh-personas-consumer
```

### Resultado esperado

Kafka reasigna las particiones del consumer que desapareció al consumer que sigue activo.

Ejemplo:

Antes:

```text
Consumer A → Partition 0, 1
Consumer B → Partition 2
```

Después:

```text
Consumer A → Partition 0, 1, 2
```

Esto confirma el rebalance automático del consumer group.

---

## Resultado de la etapa

Quedó validado:

```text
Producer Python
      ↓
    Kafka
      ↓
Consumer Group Python
      ├── particiones
      ├── offsets
      ├── lag
      ├── paralelismo
      └── rebalance
```

También quedó comprobado que eventos con la misma `key` mantienen la misma partición mientras se conserve la configuración actual de particiones.

Ejemplo observado:

```text
P101 → Partition 0
P102 → Partition 2
P103 → Partition 1
```

al volver a publicar esas mismas keys.

---

## Detener el laboratorio

Cerrar los consumers activos con:

```text
Ctrl+C
```

Después:

```bash
docker compose down
```

Esto elimina contenedores y red, pero mantiene el volumen Kafka.

Para borrar también todos los datos:

```bash
docker compose down -v
```

Usar `-v` únicamente cuando se quiera reiniciar el laboratorio completamente.

---

## Siguiente etapa

Reemplazar el producer fijo por una API Python:

```text
POST /personas
      ↓
API Python
      ↓
publica un evento Kafka por registro
      ↓
Kafka
      ↓
Consumer
```

---
---
## Etapa 3 — API REST → Kafka

Objetivo de esta etapa:

* reemplazar el producer de prueba por una API HTTP;
* recibir un lote JSON;
* publicar un evento Kafka por cada registro;
* conservar metadata del lote;
* validar el flujo completo usando el consumer Python existente.

Flujo validado:

```text
Cliente HTTP
     ↓
API Python
     ↓
Kafka
     ↓
Consumer Python
```

---

## 1. Estructura

Agregar:

```text
apps/
├── api/
│   ├── Dockerfile
│   ├── api.py
│   └── requirements.txt
│
├── producer/
└── consumer/
```

---

## 2. API Python

### `apps/api/requirements.txt`

```txt
fastapi
uvicorn[standard]
confluent-kafka==2.15.0
```

### `apps/api/Dockerfile`

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api.py .

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
```

### `apps/api/api.py`

```python
import json

from confluent_kafka import Producer
from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI(title="BNH Laboratory API")


producer = Producer(
    {
        "bootstrap.servers": "kafka:19092",
    }
)


class Metadata(BaseModel):
    jurisdiccion: str
    dominio: str
    lote_id: str


class Persona(BaseModel):
    id: str
    nombre: str


class PersonasPayload(BaseModel):
    metadata: Metadata
    registros: list[Persona]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/personas", status_code=202)
def crear_personas(payload: PersonasPayload):
    for persona in payload.registros:
        evento = {
            "metadata": payload.metadata.model_dump(),
            "registro": persona.model_dump(),
        }

        key = f"{payload.metadata.jurisdiccion}:{persona.id}"

        producer.produce(
            topic="bnh.personas",
            key=key,
            value=json.dumps(evento),
        )

    producer.flush()

    return {
        "status": "accepted",
        "lote_id": payload.metadata.lote_id,
        "cantidad_registros": len(payload.registros),
    }
```

### Qué hace

La API recibe un lote:

```text
1 request REST
    ↓
3 personas
```

y publica:

```text
3 eventos Kafka
```

Ejemplo:

```text
ARG-B:P201 → evento 1
ARG-B:P202 → evento 2
ARG-B:P203 → evento 3
```

La `key` utilizada es:

```text
jurisdiccion:id_persona
```

por ejemplo:

```text
ARG-B:P201
```

---

## 3. Agregar API al `compose.yaml`

Dentro de `services:`:

```yaml
  api:
    build:
      context: ./apps/api
    container_name: bnh-api
    depends_on:
      - kafka
    ports:
      - "8000:8000"
    restart: "no"
```

La API se comunica con Kafka mediante:

```text
kafka:19092
```

y queda expuesta al host mediante:

```text
localhost:8000
```

---

## 4. Validar Compose

### Para qué

Confirmar que el servicio API fue agregado correctamente.

### Comando

```bash
docker compose config
```

### Resultado esperado

Deben aparecer:

```text
api
consumer
producer
kafka
```

sin errores de configuración.

---

## 5. Construir la API

### Comando

```bash
docker compose build api
```

### Resultado esperado

Algo similar a:

```text
Image bnh-laboratory-api Built
```

---

## 6. Levantar Kafka + API

### Comando

```bash
docker compose up -d kafka api
```

### Verificar

```bash
docker ps
```

### Resultado esperado

Deben aparecer:

```text
bnh-kafka
bnh-api
```

con estado:

```text
Up
```

La API debe exponer:

```text
0.0.0.0:8000->8000/tcp
```

---

## 7. Validar `/health`

### Para qué

Comprobar que la API HTTP está operativa antes de probar Kafka.

### Comando

```bash
curl http://localhost:8000/health
```

### Resultado esperado

```json
{"status":"ok"}
```

---

## 8. Levantar consumer

En otra terminal:

```bash
docker compose run --rm consumer
```

### Resultado esperado

Debe quedar escuchando:

```text
Esperando eventos de bnh.personas...
```

Puede mostrar eventos anteriores almacenados en Kafka.

Dejarlo corriendo.

---

## 9. Enviar lote de Personas por REST

En otra terminal:

```bash
curl -X POST http://localhost:8000/personas \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {
      "jurisdiccion": "ARG-B",
      "dominio": "persona",
      "lote_id": "L-REST-001"
    },
    "registros": [
      {
        "id": "P201",
        "nombre": "Julieta"
      },
      {
        "id": "P202",
        "nombre": "Nicolas"
      },
      {
        "id": "P203",
        "nombre": "Valentina"
      }
    ]
  }'
```

---

## 10. Resultado esperado de la API

```json
{
  "status": "accepted",
  "lote_id": "L-REST-001",
  "cantidad_registros": 3
}
```

Esto confirma que la API recibió un único lote con tres registros.

---

## 11. Resultado esperado en Kafka

El consumer debe recibir tres eventos independientes.

Ejemplo conceptual:

```text
key=ARG-B:P201 ... persona={...}
key=ARG-B:P202 ... persona={...}
key=ARG-B:P203 ... persona={...}
```

Cada registro del request REST se convierte en un mensaje Kafka independiente.

Flujo:

```text
POST /personas

L-REST-001
├── P201
├── P202
└── P203

        ↓

API

        ↓

Kafka

├── evento ARG-B:P201
├── evento ARG-B:P202
└── evento ARG-B:P203
```

---

## 12. Metadata conservada

Cada evento enviado a Kafka contiene:

```json
{
  "metadata": {
    "jurisdiccion": "ARG-B",
    "dominio": "persona",
    "lote_id": "L-REST-001"
  },
  "registro": {
    "id": "P201",
    "nombre": "Julieta"
  }
}
```

De esta forma cada registro conserva trazabilidad respecto del lote HTTP original.

---

## Resultado de la etapa

Quedó validado:

```text
Cliente REST
     ↓
API Python
     ↓
1 lote JSON
     ↓
N eventos Kafka
     ↓
Consumer Python
```

También quedó comprobado que:

* la API puede comunicarse con Kafka por la red interna Docker;
* un request puede generar varios eventos;
* cada registro puede tener su propia key;
* los datos mantienen metadata del lote;
* el consumer recibe los eventos en tiempo real.

---

## Detener la etapa

Cerrar el consumer:

```text
Ctrl+C
```

Luego:

```bash
docker compose down
```

Esto mantiene el volumen Kafka.

Para eliminar también los datos:

```bash
docker compose down -v
```

---

## Siguiente etapa

Agregar Apache NiFi como segundo mecanismo de ingesta:

```text
API REST ───────────────┐
                        ▼
                       Kafka
                        ▲
CSV / archivo → NiFi ───┘
```

Objetivo:

* levantar NiFi;
* validar acceso a su UI;
* procesar un CSV;
* transformar registros;
* publicar esos registros en `bnh.personas`;
* comprobar que Kafka recibe datos tanto por API como por archivo.

---
---

## Etapa 4 — Ingesta de archivos con Apache NiFi

En esta etapa se incorporó **Apache NiFi 2.11.0** al laboratorio para validar un segundo canal de ingreso de datos hacia Kafka.

Hasta esta etapa existían:

- productor Python → Kafka;
- consumer Python ← Kafka;
- API REST → Kafka.

Ahora se agregó:

```text
Archivo CSV
    ↓
Apache NiFi
    ↓
Kafka
    ↓
Consumer Python
```

El objetivo es simular la recepción de archivos de una jurisdicción y transformar cada registro recibido en un mensaje Kafka independiente.

> Esta configuración corresponde al laboratorio local. No representa todavía el contrato definitivo de intercambio ni una configuración productiva de NiFi.

---

### 4.1 Servicio NiFi

Se agregó al `compose.yaml`:

```yaml
nifi:
  image: apache/nifi:2.11.0
  container_name: bnh-nifi
  hostname: nifi

  depends_on:
    - kafka

  ports:
    - "8443:8443"

  environment:
    SINGLE_USER_CREDENTIALS_USERNAME: admin
    SINGLE_USER_CREDENTIALS_PASSWORD: BnhLaboratory1234

  volumes:
    - ./data/incoming:/data/incoming
    - nifi_conf:/opt/nifi/nifi-current/conf
    - nifi_state:/opt/nifi/nifi-current/state
    - nifi_database:/opt/nifi/nifi-current/database_repository
    - nifi_flowfile:/opt/nifi/nifi-current/flowfile_repository
    - nifi_content:/opt/nifi/nifi-current/content_repository
    - nifi_provenance:/opt/nifi/nifi-current/provenance_repository

  restart: "no"
```

Y los siguientes volúmenes:

```yaml
volumes:
  kafka_data:
  nifi_conf:
  nifi_state:
  nifi_database:
  nifi_flowfile:
  nifi_content:
  nifi_provenance:
```

Los volúmenes permiten conservar configuración, estado y repositories de NiFi entre recreaciones del contenedor.

El directorio:

```text
./data/incoming
```

se monta como:

```text
/data/incoming
```

dentro del contenedor.

Esto permite que los archivos generados desde el host sean visibles directamente por NiFi.

---

### 4.2 Inicio

Levantar Kafka y NiFi:

```bash
docker compose up -d kafka nifi
```

La interfaz web queda disponible en:

```text
https://localhost:8443/nifi
```

El certificado es autofirmado, por lo que el navegador puede mostrar una advertencia.

Credenciales del laboratorio:

```text
Usuario: admin
Password: BnhLaboratory1234
```

Estas credenciales son únicamente para desarrollo local.

---

### 4.3 Landing de archivos

Se creó la estructura:

```text
data/
└── incoming/
    └── personas/
```

Ejemplo de archivo de entrada:

```csv
id,nombre,jurisdiccion
P401,Carolina,ARG-B
P402,Diego,ARG-B
P403,Florencia,ARG-B
```

Ubicación:

```text
data/incoming/personas/personas.csv
```

Dentro de NiFi el mismo archivo queda disponible en:

```text
/data/incoming/personas/personas.csv
```

---

### 4.4 Flujo NiFi

Se construyó manualmente el siguiente flujo:

```text
                         ┌─────────────┐
                    ┌───→│  LogErrors  │
                    │    └─────────────┘
                    │
GetFile
   │ success
   ▼
SplitRecord
   │ splits
   ▼
EvaluateJsonPath
   │ matched
   ▼
PublishKafka
   │
   ▼
Kafka: bnh.personas
```

Las relaciones `failure` de los processors principales se envían a `LogErrors`.

---

### 4.5 GetFile

Processor:

```text
GetFile
```

Configuración:

```text
Input Directory: /data/incoming/personas
Recurse Subdirectories: false
Keep Source File: false
```

Su función es detectar archivos en la landing y convertirlos en FlowFiles de NiFi.

Con `Keep Source File = false`, el archivo se elimina de la landing una vez incorporado correctamente al flujo de NiFi.

---

### 4.6 SplitRecord

Processor:

```text
SplitRecord
```

Se configuró para convertir un archivo CSV con múltiples registros en un FlowFile independiente por registro.

Controller Services utilizados:

```text
CSVReader
JsonRecordSetWriter
```

#### CSVReader

Configuración principal:

```text
Schema Access Strategy:
Use String Fields From Header
```

La primera fila del CSV se interpreta como encabezado y todos los campos se manejan inicialmente como strings.

#### JsonRecordSetWriter

Configuración:

```text
Schema Access Strategy:
Inherit Record Schema

Output Grouping:
One Line Per Object

Pretty Print JSON:
false
```

#### SplitRecord

Configuración:

```text
Record Reader: CSVReader
Record Writer: JsonRecordSetWriter
Records Per Split: 1
```

De esta forma:

```text
personas.csv
    │
    ▼
SplitRecord
    ├── P401
    ├── P402
    └── P403
```

Cada persona continúa por el pipeline como un FlowFile independiente.

Relaciones:

```text
splits   → EvaluateJsonPath
failure  → LogErrors
original → terminate
```

---

### 4.7 EvaluateJsonPath

Processor:

```text
EvaluateJsonPath
```

Su función es obtener del JSON los campos necesarios para construir la key del mensaje Kafka.

Configuración:

```text
Destination:
flowfile-attribute
```

Propiedades dinámicas:

```text
persona.id
$.id
```

```text
persona.jurisdiccion
$.jurisdiccion
```

Ejemplo:

Contenido del FlowFile:

```json
{"id":"P401","nombre":"Carolina","jurisdiccion":"ARG-B"}
```

Atributos generados:

```text
persona.id = P401
persona.jurisdiccion = ARG-B
```

Relaciones:

```text
matched → PublishKafka
failure → LogErrors
```

---

### 4.8 Kafka3ConnectionService

Para que NiFi pueda publicar en Kafka se creó:

```text
Kafka3ConnectionService
```

Configuración:

```text
Bootstrap Servers:
kafka:19092

Security Protocol:
PLAINTEXT
```

Se utiliza `kafka:19092` porque NiFi y Kafka se encuentran dentro de la misma red Docker Compose.

El listener:

```text
localhost:9092
```

queda reservado para clientes que ejecutan desde el host.

---

### 4.9 PublishKafka

Processor:

```text
PublishKafka
```

Configuración:

```text
Kafka Connection Service:
Kafka3ConnectionService

Topic Name:
bnh.personas
```

Key Kafka:

```text
${persona.jurisdiccion}:${persona.id}
```

Ejemplo:

```text
ARG-B:P401
```

Esto mantiene el mismo criterio de key utilizado anteriormente por los productores Python.

Relaciones:

```text
success → terminate
failure → LogErrors
```

---

### 4.10 LogErrors

Processor:

```text
LogAttribute
```

Nombre utilizado en el flujo:

```text
LogErrors
```

Recibe los FlowFiles enviados por las relaciones `failure`.

Flujos de error:

```text
SplitRecord.failure ────────┐
                            │
EvaluateJsonPath.failure ───┼──→ LogErrors
                            │
PublishKafka.failure ───────┘
```

La relación:

```text
success
```

de `LogErrors` se encuentra configurada como `terminate`.

---

### 4.11 Prueba end-to-end

Se dejó un consumer Python escuchando el topic:

```bash
docker compose run --rm consumer
```

Salida inicial:

```text
Esperando eventos de bnh.personas...
```

Luego se creó:

```bash
cat > data/incoming/personas/personas.csv <<'EOF'
id,nombre,jurisdiccion
P401,Carolina,ARG-B
P402,Diego,ARG-B
P403,Florencia,ARG-B
EOF
```

NiFi detectó automáticamente el archivo y ejecutó:

```text
CSV
 ↓
GetFile
 ↓
SplitRecord
 ↓
EvaluateJsonPath
 ↓
PublishKafka
 ↓
Kafka
 ↓
Consumer Python
```

Resultado observado:

```text
key=ARG-B:P402 partition=0 offset=10 persona={'id': 'P402', 'nombre': 'Diego', 'jurisdiccion': 'ARG-B'}

key=ARG-B:P401 partition=1 offset=8 persona={'id': 'P401', 'nombre': 'Carolina', 'jurisdiccion': 'ARG-B'}

key=ARG-B:P403 partition=1 offset=9 persona={'id': 'P403', 'nombre': 'Florencia', 'jurisdiccion': 'ARG-B'}
```

Con esto quedó validado:

```text
Archivo CSV
    ↓
NiFi
    ↓
Kafka
    ↓
Consumer Python
```

con **un mensaje Kafka por persona** y una key formada por:

```text
jurisdiccion:id_persona
```

---

### 4.12 Consumer groups durante la prueba

Durante la primera ejecución había dos consumers activos utilizando:

```text
group.id = bnh-personas-consumer
```

Kafka repartió las particiones entre ambos consumers, por lo que la terminal utilizada para la prueba no mostraba todos los mensajes.

Se verificó mediante:

```bash
docker exec bnh-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group bnh-personas-consumer
```

Luego de dejar un único consumer activo, este tomó las tres particiones:

```text
partition 0 → consumer
partition 1 → consumer
partition 2 → consumer
```

con:

```text
LAG = 0
```

Esto confirmó también el comportamiento de rebalanceo de Kafka Consumer Groups validado en etapas anteriores.

---

### 4.13 Estado de la etapa

Validado:

```text
CSV → NiFi → Kafka → Consumer
```

También se comprobó:

- lectura de archivos desde una landing compartida;
- separación de un archivo en registros independientes;
- transformación CSV → JSON;
- extracción de atributos desde JSON;
- generación de Kafka keys;
- publicación en `bnh.personas`;
- distribución por particiones;
- manejo básico de ramas de error;
- persistencia local de NiFi mediante Docker volumes.

### Pendiente

El flujo NiFi fue creado actualmente desde la interfaz web y está persistido en los volúmenes Docker locales.

Esto significa que todavía **no es reproducible solamente clonando el repositorio**.

Próximo paso:

```text
Agrupar flujo NiFi
        ↓
Exportar definición
        ↓
Versionar en Git
        ↓
Importar desde otro entorno
```

Estructura prevista:

```text
nifi/
└── flows/
    └── personas-file-ingestion.json
```

Esto permitirá que otro integrante del equipo pueda clonar el laboratorio y reconstruir el flujo sin configurarlo manualmente desde cero.

---
---

## Etapa 5 — Ingesta gRPC con client streaming

En esta etapa se incorporó **gRPC** como segundo contrato público de integración programática de BNH.

La decisión funcional es que **REST y gRPC coexistirán como contratos públicos**. gRPC no reemplaza REST.

El objetivo del laboratorio es validar este flujo:

```text
Cliente gRPC
    │
    │ client streaming
    ▼
Servidor gRPC Python
    │
    ▼
Kafka
    │
    ▼
bnh.personas
```

El canal de archivos con NiFi continúa siendo independiente:

```text
REST ─────┐
          │
gRPC ─────┼──→ Kafka
          │
NiFi ─────┘
```

> El contrato utilizado en esta etapa es simplificado y corresponde al laboratorio. No representa todavía el contrato definitivo de Personas de BNH.

---

### 5.1 Estructura

Se agregó la siguiente estructura:

```text
contracts/
└── grpc/
    └── personas/
        └── v1/
            └── personas.proto

apps/
└── grpc/
    ├── generated/
    │   └── personas/
    │       └── v1/
    │           ├── personas_pb2.py
    │           └── personas_pb2_grpc.py
    ├── client/
    │   ├── client.py
    │   ├── Dockerfile
    │   └── requirements.txt
    └── server/
        ├── server.py
        ├── Dockerfile
        └── requirements.txt
```

---

### 5.2 Contrato Protobuf

Archivo:

```text
contracts/grpc/personas/v1/personas.proto
```

Contenido:

```protobuf
syntax = "proto3";

package bnh.personas.v1;


message Metadata {
  string jurisdiccion = 1;
  string dominio = 2;
  string lote_id = 3;
}


message Persona {
  string id = 1;
  string nombre = 2;
}


message CargaPersonaRequest {
  Metadata metadata = 1;
  Persona registro = 2;
}


message ResultadoCarga {
  string lote_id = 1;
  int32 cantidad_recibida = 2;
  string estado = 3;
}


service PersonasService {
  rpc EnviarPersonas(stream CargaPersonaRequest) returns (ResultadoCarga);
}
```

El método:

```protobuf
rpc EnviarPersonas(stream CargaPersonaRequest) returns (ResultadoCarga);
```

implementa un RPC de tipo **client streaming**:

```text
Cliente
  ├── request 1 ──→
  ├── request 2 ──→
  ├── request 3 ──→
  └── request N ──→

Cliente ←── una única respuesta final
```

Esto permite enviar múltiples registros utilizando un único stream gRPC.

---

### 5.3 Generación de código Python

El archivo `.proto` se compila utilizando `grpcio-tools`.

Para evitar instalar herramientas directamente en el host se utilizó Docker:

```bash
docker run --rm \
  -v "$PWD:/workspace" \
  -w /workspace \
  python:3.12-slim \
  sh -c "pip install --no-cache-dir grpcio-tools==1.83.0 && \
  python -m grpc_tools.protoc \
    -I contracts/grpc \
    --python_out=apps/grpc/generated \
    --grpc_python_out=apps/grpc/generated \
    contracts/grpc/personas/v1/personas.proto"
```

Esto genera:

```text
personas_pb2.py
personas_pb2_grpc.py
```

`personas_pb2.py` contiene las clases correspondientes a los mensajes Protobuf.

`personas_pb2_grpc.py` contiene las clases necesarias para cliente y servidor gRPC:

```text
PersonasServiceStub
PersonasServiceServicer
add_PersonasServiceServicer_to_server
```

El código generado no debe editarse manualmente.

---

### 5.4 Dependencias

Servidor:

```text
grpcio==1.83.0
protobuf==7.35.1
confluent-kafka==2.15.0
```

Cliente:

```text
grpcio==1.83.0
protobuf==7.35.1
```

`grpcio-tools` se utiliza únicamente para generar código desde el `.proto` y no forma parte del runtime.

---

### 5.5 Servidor gRPC

El servidor implementa:

```python
class PersonasService(
    personas_pb2_grpc.PersonasServiceServicer
):
```

El método:

```python
def EnviarPersonas(self, request_iterator, context):
```

recibe un iterador de mensajes provenientes del stream.

Conceptualmente:

```text
request_iterator
    │
    ├── CargaPersonaRequest P501
    ├── CargaPersonaRequest P502
    └── CargaPersonaRequest P503
```

El servidor recorre los mensajes a medida que llegan:

```python
for request in request_iterator:
    ...
```

y no necesita recibir el lote completo antes de empezar a procesarlo.

---

### 5.6 Integración con Kafka

Por cada registro recibido por gRPC, el servidor genera un mensaje independiente en:

```text
bnh.personas
```

La Kafka key sigue el mismo criterio utilizado por REST y NiFi:

```text
jurisdiccion:id
```

Ejemplo:

```text
ARG-B:P501
```

El evento publicado tiene la forma:

```json
{
  "metadata": {
    "jurisdiccion": "ARG-B",
    "dominio": "persona",
    "lote_id": "L-GRPC-001"
  },
  "registro": {
    "id": "P501",
    "nombre": "Lucia"
  }
}
```

El producer utiliza el listener interno de Kafka:

```text
kafka:19092
```

porque ambos servicios se encuentran dentro de la red Docker Compose.

---

### 5.7 Respuesta gRPC

Una vez finalizado el stream y publicados los mensajes en Kafka, el servidor responde:

```text
ResultadoCarga
```

Ejemplo:

```text
lote_id=L-GRPC-001
cantidad_recibida=3
estado=RECIBIDO
```

Para esta etapa del laboratorio se utiliza:

```python
producer.flush(10)
```

antes de devolver la respuesta.

Esto permite validar que el lote fue enviado hacia Kafka antes de responder al cliente.

> Este mecanismo deberá revisarse para pruebas de volumen y para un diseño productivo. El comportamiento de backpressure, acknowledgements parciales, reintentos y errores de streams largos todavía no está definido.

---

### 5.8 Cliente gRPC

El cliente genera mensajes mediante un generador Python:

```python
def generar_personas():
    ...
    yield personas_pb2.CargaPersonaRequest(...)
```

El uso de `yield` permite entregar mensajes progresivamente al stream en vez de crear necesariamente todo el lote en memoria.

La llamada gRPC se realiza mediante el Stub generado:

```python
stub = personas_pb2_grpc.PersonasServiceStub(channel)

resultado = stub.EnviarPersonas(
    generar_personas()
)
```

En Docker el cliente se conecta mediante:

```text
grpc-server:50051
```

---

### 5.9 Servicios Docker

Servidor:

```yaml
grpc-server:
  build:
    context: ./apps/grpc
    dockerfile: server/Dockerfile
  container_name: bnh-grpc-server
  hostname: grpc-server

  depends_on:
    kafka:
      condition: service_healthy

  ports:
    - "50051:50051"

  restart: "no"
```

Cliente:

```yaml
grpc-client:
  build:
    context: ./apps/grpc
    dockerfile: client/Dockerfile
  container_name: bnh-grpc-client

  depends_on:
    - grpc-server

  restart: "no"
```

---

### 5.10 Healthcheck de Kafka

Durante la primera integración se detectó una condición de carrera:

```text
Kafka container started
    ↓
grpc-server started
    ↓
Kafka todavía no aceptaba conexiones
    ↓
Connection refused
```

`depends_on` con `service_started` garantiza que el contenedor haya arrancado, pero no que Kafka esté listo para aceptar conexiones.

Se agregó un healthcheck:

```yaml
healthcheck:
  test:
    [
      "CMD-SHELL",
      "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 || exit 1",
    ]
  interval: 5s
  timeout: 5s
  retries: 12
  start_period: 10s
```

Y el servidor gRPC utiliza:

```yaml
depends_on:
  kafka:
    condition: service_healthy
```

El arranque queda:

```text
Kafka inicia
    ↓
healthcheck OK
    ↓
Kafka Healthy
    ↓
grpc-server inicia
```

La prueba en frío confirmó:

```text
Container bnh-kafka       Healthy
Container bnh-grpc-server Started
```

---

### 5.11 Prueba gRPC aislada

Primero se validó gRPC sin Kafka.

El cliente envió:

```text
P501 - Lucia
P502 - Mateo
P503 - Camila
```

por un único stream.

El servidor recibió individualmente:

```text
Recibida persona id=P501 nombre=Lucia jurisdiccion=ARG-B lote_id=L-GRPC-001
Recibida persona id=P502 nombre=Mateo jurisdiccion=ARG-B lote_id=L-GRPC-001
Recibida persona id=P503 nombre=Camila jurisdiccion=ARG-B lote_id=L-GRPC-001
```

El cliente recibió:

```text
lote_id=L-GRPC-001 cantidad_recibida=3 estado=RECIBIDO
```

Con esto se validó:

```text
grpc-client
    ↓
client streaming
    ↓
grpc-server
    ↓
ResultadoCarga
```

---

### 5.12 Prueba end-to-end gRPC → Kafka

Se dejó un consumer escuchando:

```bash
docker compose run --rm consumer
```

Luego se ejecutó:

```bash
docker compose run --rm grpc-client
```

El consumer recibió:

```text
key=ARG-B:P501 partition=2 offset=9
persona={
  'metadata': {
    'jurisdiccion': 'ARG-B',
    'dominio': 'persona',
    'lote_id': 'L-GRPC-001'
  },
  'registro': {
    'id': 'P501',
    'nombre': 'Lucia'
  }
}
```

```text
key=ARG-B:P502 partition=1 offset=12
```

```text
key=ARG-B:P503 partition=1 offset=13
```

Con esto quedó validado:

```text
gRPC client
     ↓
gRPC server
     ↓
Kafka
     ↓
bnh.personas
     ↓
Consumer Python
```

Cada persona se publica como un mensaje Kafka independiente.

---

### 5.13 Estado de la etapa

Validado:

```text
REST ─────┐
          │
gRPC ─────┼──→ Kafka
          │
NiFi ─────┘
```

La integración gRPC funciona utilizando:

```text
Protocol Buffers
HTTP/2 / gRPC
client streaming
Python
Kafka
```

El contrato gRPC y el contrato REST coexistirán como interfaces públicas de BNH.

---

### 5.14 Pruebas de volumen realizadas

Antes de incorporar Flink se ejecutaron pruebas de carga sobre el canal gRPC.

Se probaron los siguientes volúmenes:

- 10 registros.
- 1.000 registros.
- 10.000 registros.
- 100.000 registros.
- 1.000.000 de registros.

Para evitar distorsiones se eliminaron los logs por registro del servidor.

Resultados observados en el laboratorio:

```text
10 registros
duración: 0.019 s
throughput aproximado: 513 registros/s

1.000 registros
duración: 0.043 s
throughput aproximado: 23.300 registros/s

10.000 registros
duración: 0.341 s
throughput aproximado: 29.360 registros/s

100.000 registros
duración: 3.164 s
throughput aproximado: 31.600 registros/s

1.000.000 registros
duración: 33.504 s
throughput aproximado: 29.850 registros/s
```

Estas mediciones corresponden exclusivamente al laboratorio local y no deben
interpretarse como un benchmark productivo.

El objetivo de estas pruebas fue verificar que el flujo gRPC → Kafka se mantuviera
estable al aumentar el volumen antes de incorporar procesamiento con Flink.

---

## Etapa 6 — Capa Bronze con Apache Flink y MinIO

El laboratorio incorpora una primera implementación de la capa Bronze utilizando
Apache Flink para el procesamiento de eventos y MinIO como almacenamiento
compatible con S3.

El objetivo de esta etapa es validar el flujo técnico completo desde los canales
de ingesta hasta la persistencia, sin implementar todavía lógica de negocio
educativa compleja ni persistencia analítica en PostgreSQL.

### Flujo de la etapa

La arquitectura de esta etapa contempla el siguiente recorrido:

```text
gRPC / REST / NiFi
        |
        v
      Kafka
        |
        v
   Apache Flink
        |
        | normalización básica
        | validaciones técnicas
        v
      MinIO
        |
        v
  bucket bnh-bronze
```

El recorrido validado end-to-end hasta la capa Bronze corresponde actualmente a gRPC.

REST y NiFi ya fueron validados como canales de ingesta hacia Kafka, pero todavía
no se ejecutó la misma prueba completa hasta MinIO para ambos canales.

La prueba end-to-end realizada para Personas fue:

```text
gRPC
  -> Kafka
  -> PyFlink
  -> normalización y validación
  -> FileSink S3
  -> MinIO / bnh-bronze/personas/
```

### MinIO

Para el laboratorio se utiliza:

```text
minio/minio:RELEASE.2025-09-07T16-13-09Z
```

MinIO expone:

* API S3: [http://localhost:9000](http://localhost:9000)
* Consola web: [http://localhost:9001](http://localhost:9001)

El almacenamiento se persiste mediante el volumen Docker:

```text
minio_data
```

El bucket utilizado para la capa Bronze es:

```text
bnh-bronze
```

#### Inicialización automática del bucket

El servicio `minio-init` utiliza el cliente oficial de MinIO (`mc`) para crear
automáticamente el bucket necesario por la capa Bronze.

El inicializador:

1. espera hasta que MinIO acepte conexiones;
2. configura el alias interno `bnh`;
3. crea `bnh-bronze` si todavía no existe;
4. termina con código `0`.

La creación utiliza:

```text
mc mb --ignore-existing bnh/bnh-bronze
```

Por lo tanto, el proceso puede ejecutarse nuevamente sin eliminar ni recrear el
bucket existente.

Para ejecutar la inicialización manualmente:

```bash
docker compose up minio-init
```

Esto evita depender de la creación manual del bucket desde la consola web de
MinIO y mejora la reproducibilidad del laboratorio.

> La versión y las credenciales utilizadas son exclusivamente para el
> laboratorio local y no constituyen una definición para ambientes productivos.

### Integración Flink con S3

Flink utiliza el plugin oficial:

```text
flink-s3-fs-hadoop-2.1.1.jar
```

El plugin se habilita dentro de:

```text
/opt/flink/plugins/s3-fs-hadoop/
```

La configuración del JobManager y TaskManager apunta al endpoint S3-compatible
de MinIO:

```text
s3.endpoint: http://minio:9000
s3.path.style.access: true
```

Esto permite que PyFlink utilice rutas del tipo:

```text
s3://bnh-bronze/...
```

mediante `FileSink`.

### Smoke test Flink -> MinIO

Se incorporó el job:

```text
apps/flink/jobs/s3_smoke.py
```

Este job genera un registro de prueba mediante PyFlink y lo persiste
directamente en:

```text
s3://bnh-bronze/flink-smoke/
```

La prueba fue ejecutada correctamente y el objeto resultante pudo ser listado y
leído posteriormente desde MinIO.

Con esto se validó técnicamente:

```text
PyFlink
  -> FileSink
  -> plugin S3
  -> MinIO
  -> bucket Bronze
```

### Procesamiento de Personas

El job:

```text
apps/flink/jobs/personas_validate.py
```

consume eventos desde:

```text
topic: bnh.personas
bootstrap server: kafka:19092
```

Actualmente realiza una normalización técnica mínima para soportar distintos
formatos de entrada del laboratorio.

Un evento con envelope:

```json
{
  "metadata": {
    "jurisdiccion": "ARG-B",
    "dominio": "persona",
    "lote_id": "L-GRPC-3"
  },
  "registro": {
    "id": "P000001",
    "nombre": "Persona 1"
  }
}
```

se conserva bajo la estructura común utilizada por el laboratorio.

También se soportan registros planos provenientes de pruebas de ingesta mediante
NiFi, agregando la metadata mínima necesaria.

El resultado actual posee la forma:

```json
{
  "estado_validacion": "VALIDO",
  "errores": [],
  "metadata": {
    "jurisdiccion": "ARG-B",
    "dominio": "persona",
    "lote_id": "L-GRPC-3"
  },
  "registro": {
    "id": "P000001",
    "nombre": "Persona 1"
  }
}
```

Las validaciones existentes son deliberadamente simples y corresponden al
laboratorio. Actualmente permiten verificar, entre otras cosas:

* JSON válido.
* presencia de jurisdicción.
* presencia del identificador del registro.
* adaptación de distintos formatos de entrada a una estructura común.

Estas reglas no constituyen todavía el contrato definitivo de Persona ni las
reglas de negocio de BNH.

### Checkpointing

El job de streaming habilita checkpointing cada 5 segundos:

```python
env.enable_checkpointing(5000)
```

Esto es necesario para que `FileSink` pueda completar y publicar los archivos
generados durante un flujo continuo.

Además de permitir la persistencia en Bronze, el checkpointing será parte de las
próximas pruebas relacionadas con recuperación ante fallos y garantías de
procesamiento.

### Prueba end-to-end

Con el job de Flink en ejecución se enviaron tres registros mediante el cliente
gRPC:

```bash
TOTAL_PERSONAS=3 docker compose run --rm grpc-client
```

Resultado:

```text
cantidad_enviada=3
cantidad_recibida=3
lote_id=L-GRPC-3
estado=RECIBIDO
```

Luego del checkpoint de Flink se generó un objeto en:

```text
bnh-bronze/personas/
```

El contenido persistido fue:

```json
{"estado_validacion":"VALIDO","errores":[],"metadata":{"jurisdiccion":"ARG-B","dominio":"persona","lote_id":"L-GRPC-3"},"registro":{"id":"P000001","nombre":"Persona 1"}}
{"estado_validacion":"VALIDO","errores":[],"metadata":{"jurisdiccion":"ARG-B","dominio":"persona","lote_id":"L-GRPC-3"},"registro":{"id":"P000002","nombre":"Persona 2"}}
{"estado_validacion":"VALIDO","errores":[],"metadata":{"jurisdiccion":"ARG-B","dominio":"persona","lote_id":"L-GRPC-3"},"registro":{"id":"P000003","nombre":"Persona 3"}}
```

De esta forma quedó validado el recorrido:

```text
gRPC -> Kafka -> Flink -> Bronze
```

### Estado actual

Validado en el laboratorio:

* Kafka como bus de eventos.
* gRPC como canal de ingesta.
* NiFi como canal de ingesta desde archivos.
* PyFlink consumiendo Kafka.
* normalización y validación técnica básica.
* checkpointing de Flink.
* MinIO como almacenamiento S3-compatible.
* escritura desde Flink hacia Bronze.
* lectura posterior de los objetos persistidos.

### Alcance actual

El trabajo se encuentra limitado a la capa Bronze.

Por el momento quedan fuera de esta etapa:

* persistencia en PostgreSQL.
* procesamiento analítico con Spark.
* orquestación con Airflow.
* reglas educativas complejas.
* calificaciones.
* modelo definitivo de datos.

Estas etapas se retomarán cuando exista mayor definición de los contratos y de
los datos reales proporcionados por las jurisdicciones.

### Próximas investigaciones

La siguiente etapa del laboratorio estará orientada a estudiar qué capacidades
de Flink resultan útiles dentro de la capa Bronze, priorizando controles
técnicos sobre reglas educativas todavía no definidas.

Entre los casos a evaluar se encuentran:

* cálculo de checksum.
* validaciones por campo.
* validaciones por lote.
* detección de duplicados.
* separación de registros válidos e inválidos.
* preservación del evento original.
* state y ventanas.
* comportamiento ante fallos.
* recuperación mediante checkpoints.
* garantías de entrega y procesamiento.

La definición final de estas reglas dependerá de los contratos y ejemplos de
datos que proporcione el cliente.


## Validación e integración de Persona hasta Bronze

### Objetivo

El laboratorio implementa y valida el flujo de ingestión de la entidad
`Persona` hasta la capa Bronze.

La arquitectura probada actualmente es:

```text
REST ──┐
       │
gRPC ──┼──> Kafka (`bnh.personas`) ──> PyFlink ──> Bronze (S3 compatible)
       │
NiFi ──┘
````

Estado actual:

* REST → Kafka → Flink → Bronze: validado.
* gRPC → Kafka → Flink → Bronze: validado.
* REST y gRPC producen el mismo registro canónico en Bronze.
* NiFi todavía debe alinearse al contrato actual de Persona.

La validación funcional/técnica común de Persona se centraliza en Flink.
Los canales de ingreso se encargan principalmente del transporte y de
construir el envelope necesario.

---

### Modelo actual de Persona

El registro normalizado contiene siempre los siguientes campos:

```text
id_persona
fecha_nacimiento
cuit
c_documento
nro_documento
c_pais_nacimiento
c_provincia_nacimiento
c_departamento_nacimiento
c_localidad_nacimiento
c_municipio_nacimiento
lugar_nacimiento
c_fallecido
fecha_fallecido
c_es_indigena
```

Flink genera una representación canónica del registro:

* todos los campos conocidos están siempre presentes;
* los campos ausentes quedan como `null`;
* los campos adicionales recibidos se conservan;
* determinados identificadores/códigos numéricos se normalizan a `string`.

Ejemplo:

```json
{
  "estado_validacion": "VALIDO",
  "errores": [],
  "metadata": {
    "jurisdiccion": "ARG-B",
    "dominio": "persona",
    "lote_id": "REST-CANON-001"
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
    "fecha_fallecido": null,
    "c_es_indigena": "N"
  }
}
```

---

### Responsabilidades por componente

#### REST / gRPC

Los canales de entrada:

* reciben el registro;
* conservan metadata;
* construyen el envelope;
* publican en Kafka;
* generan la key Kafka a partir de `jurisdiccion:id_persona`.

No duplican las reglas de validación implementadas en Flink.

Por ejemplo, un CUIT inválido como:

```text
2012345678A
```

puede ser aceptado por REST/gRPC y transportado a Kafka.

Flink es quien posteriormente lo clasifica como `INVALIDO`.

#### Kafka

Kafka funciona como capa de transporte y desacoplamiento.

Topic actual:

```text
bnh.personas
```

El laboratorio utiliza 3 particiones y replication factor 1.

Kafka no aplica reglas de validación de Persona.

#### Flink

Flink centraliza:

* parsing;
* validación estructural;
* normalización;
* canonicalización;
* validaciones técnicas de Persona;
* generación de `estado_validacion`;
* generación de errores;
* persistencia hacia Bronze.

Job:

```text
BNH - Personas Normalize Validate and Bronze
```

Archivo:

```text
apps/flink/jobs/personas_validate.py
```

#### Bronze

Los resultados procesados por Flink se persisten actualmente en:

```text
s3://bnh-bronze/personas/
```

La implementación utiliza `FileSink`.

Durante una escritura pueden aparecer archivos temporales:

```text
_part-..._tmp_...
```

Cuando ocurre el rolling del archivo y el checkpoint correspondiente,
el objeto pasa a un archivo final:

```text
part-...
```

Esto es comportamiento normal del FileSink.

---

## Casos sintéticos de Persona

Los datos reutilizables están en:

```text
scripts/testdata/personas.py
```

Casos disponibles:

```text
valid
missing-id
missing-birth-date
missing-document-type
missing-country
missing-indigenous
numeric-identifiers
numeric-country-code
numeric-codes
invalid-birth-date
invalid-death-date
invalid-cuit-length
invalid-cuit-nondigit
invalid-root
invalid-envelope
invalid-place-type
```

También existe un smoke check rápido:

```text
scripts/testdata/check_personas.py
```

Ejecutar:

```bash
python3 scripts/testdata/check_personas.py
```

Actualmente cubre 16 casos.

Este script permite revisar rápidamente la lógica Python, pero la evidencia
principal del laboratorio continúa siendo el flujo real:

```text
canal → Kafka → Flink → Bronze
```

---

## Levantar el flujo Persona

Ejecutar desde la raíz del repositorio.

### Infraestructura mínima

```bash
docker compose up -d \
  kafka \
  minio \
  minio-init \
  flink-jobmanager \
  flink-taskmanager
```

### Crear el topic Kafka

En un entorno nuevo:

```bash
docker exec bnh-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic bnh.personas \
  --partitions 3 \
  --replication-factor 1
```

> La creación automática del topic todavía no está implementada.

### Ejecutar el job de Flink

```bash
docker exec bnh-flink-jobmanager \
  flink run -d \
  -py /opt/flink/jobs/personas_validate.py
```

Verificar:

```bash
curl -s http://localhost:8081/jobs/overview
```

Debe existir un job:

```text
BNH - Personas Normalize Validate and Bronze
```

en estado:

```text
RUNNING
```

---

## Validar REST

Construir y levantar:

```bash
docker compose build api
docker compose up -d api
```

Health check:

```bash
curl -s http://localhost:8000/health
```

### Persona válida

```bash
python3 scripts/testdata/personas.py --case valid \
  | python3 -c '
import sys,json
data=json.load(sys.stdin)
data["metadata"]["lote_id"]="REST-VALID-001"
print(json.dumps(data, ensure_ascii=False))
' \
  | curl -s -w '\nHTTP %{http_code}\n' \
      -X POST http://localhost:8000/personas \
      -H "Content-Type: application/json" \
      -d @-
```

Esperado:

```text
HTTP 202
```

y posteriormente Flink:

```text
estado_validacion = VALIDO
```

### Persona con CUIT inválido

```bash
python3 scripts/testdata/personas.py --case invalid-cuit-nondigit \
  | python3 -c '
import sys,json
data=json.load(sys.stdin)
data["metadata"]["lote_id"]="REST-INVALID-CUIT-001"
print(json.dumps(data, ensure_ascii=False))
' \
  | curl -s -w '\nHTTP %{http_code}\n' \
      -X POST http://localhost:8000/personas \
      -H "Content-Type: application/json" \
      -d @-
```

REST debe aceptar el registro (`HTTP 202`).

Flink debe generar:

```text
estado_validacion = INVALIDO
registro.cuit debe contener solo digitos
```

---

## Validar gRPC

Construir:

```bash
docker compose build grpc-server grpc-client
docker compose up -d grpc-server
```

### Persona válida

```bash
TOTAL_PERSONAS=1 \
GRPC_CASE=valid \
LOTE_ID=GRPC-VALID-001 \
docker compose run --rm grpc-client
```

### CUIT inválido

```bash
TOTAL_PERSONAS=1 \
GRPC_CASE=invalid-cuit \
LOTE_ID=GRPC-INVALID-CUIT-001 \
docker compose run --rm grpc-client
```

gRPC debe aceptar ambos registros.

Flink debe producir respectivamente:

```text
GRPC-VALID-001
→ VALIDO
```

y:

```text
GRPC-INVALID-CUIT-001
→ INVALIDO
→ registro.cuit debe contener solo digitos
```

---

## Verificar Flink

Buscar ejecuciones por lote:

```bash
docker logs --tail 1000 bnh-flink-taskmanager 2>&1 \
  | grep -E 'REST-VALID-001|REST-INVALID-CUIT-001|GRPC-VALID-001|GRPC-INVALID-CUIT-001'
```

---

## Verificar Bronze

Listar objetos:

```bash
docker compose run --rm \
  --entrypoint /bin/sh \
  minio-init \
  -c '
    mc alias set bnh http://minio:9000 bnhadmin BnhMinioLaboratory1234 >/dev/null &&
    mc ls --recursive bnh/bnh-bronze/personas
  '
```

Para inspeccionar un objeto:

```bash
docker compose run --rm \
  --entrypoint /bin/sh \
  minio-init \
  -c '
    mc alias set bnh http://minio:9000 bnhadmin BnhMinioLaboratory1234 >/dev/null &&
    mc cat bnh/bnh-bronze/personas/<RUTA_DEL_ARCHIVO>
  '
```

Se comprobó que REST y gRPC, enviando la misma Persona, generan en Bronze:

* el mismo `registro`;
* el mismo `estado_validacion`;
* los mismos `errores`.

La única diferencia esperada entre ambos mensajes es metadata de trazabilidad,
por ejemplo `lote_id`.

---

## Estado de NiFi

NiFi todavía utiliza el flujo inicial del laboratorio y debe alinearse al
modelo actual.

El objetivo pendiente es:

```text
archivo
  ↓
NiFi
  ↓
{metadata, registro}
  ↓
Kafka
  ↓
Flink
  ↓
Bronze
```

NiFi debe publicar el mismo envelope que REST y gRPC y preservar
identificadores/códigos como strings, evitando pérdida de información por
inferencia automática de tipos CSV.

La compatibilidad de Flink con mensajes planos se mantiene temporalmente
hasta completar esta migración.

---

## Reglas deliberadamente fuera de alcance actual

Todavía no se implementan como validaciones duras:

* dígito verificador de CUIT;
* DNI → CUIT obligatorio;
* DNI → número de documento únicamente numérico;
* estados "En trámite" / "No Posee";
* validación contra catálogos oficiales;
* país/provincia/departamento/localidad/municipio;
* dependencia geográfica entre códigos;
* provincia obligatoria para Argentina;
* lugar de nacimiento obligatorio para extranjeros;
* relación `c_fallecido` / `fecha_fallecido`;
* edad mínima/máxima;
* fechas futuras;
* relación nacimiento/fallecimiento;
* movimientos posteriores al fallecimiento;
* integración RENAPER / SINTyS / ANSES.

Estas reglas requieren definiciones funcionales o catálogos oficiales y no se
consideran confirmadas únicamente por aparecer en relevamientos/documentación.
