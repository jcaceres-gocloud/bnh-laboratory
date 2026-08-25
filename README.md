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
