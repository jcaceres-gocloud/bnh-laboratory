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
