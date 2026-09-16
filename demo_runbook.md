# Bajar todo:
```bash
docker compose -f docker-compose-demo.yml down

# Eliminar volumenes

docker volume rm \
  bnh-laboratory_kafka_data \
  bnh-laboratory_ozone_scm_data \
  bnh-laboratory_ozone_om_data \
  bnh-laboratory_ozone_datanode_data \
  bnh-laboratory_postgres_dw_data
```

# Levantar todo:

```bash
docker compose -f docker-compose-demo.yml up -d
```
### Verificación general

```bash
docker compose -f docker-compose-demo.yml ps -a
```

## 1. Verificar Kafka

```bash
docker exec bnh-demo-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```
Esperamos:

```text
bnh.personas
bnh.organizaciones
```

## 2. Verificar Bronze en Ozone

```bash
docker exec bnh-demo-ozone-om \
  ozone sh bucket list /s3v
```

## 3. Verificar S3WebUI

Abrir:

```text
http://localhost:8083
```

## 4. Iniciar job Flink de Personas

```bash
docker exec bnh-demo-flink-jobmanager \
  flink run -d -py /opt/flink/jobs/personas_validate.py
```

**Para qué / por qué:**
Deja corriendo el pipeline que escucha:

```text
bnh.personas
```

y procesa/valida los mensajes de Personas antes de escribirlos en Bronze.

---

## 5. Iniciar job Flink de Organizaciones

```bash
docker exec bnh-demo-flink-jobmanager \
  flink run -d -py /opt/flink/jobs/organizaciones_validate.py
```

## 6. Verificar Flink

Abrir:

```text
http://localhost:8081
```

## 7. Interfaces útiles durante la demo

```text
API / Swagger     → http://localhost:8000


Flink             → http://localhost:8081


S3WebUI / Bronze  → http://localhost:8083
USERNAME: admin@bnh.local
PASSWORD: BnhLaboratory1234


NiFi              → https://localhost:8443
USERNAME: admin
PASSWORD: BnhLaboratory1234


Portainer         → https://localhost:9443
USERNAME: admin
PASSWORD: 3~yX6tp#ga4}%7V18d!/n2WF]wh0B,Oj
```

