# Práctica resolta: CityBikes → Kafka (producer) → kafka-console-consumer

## Obxectivo

Crear un produtor en Python que consulte periodicamente a API pública de **CityBikes** e publique eventos nun topic de Kafka. Despois, verificar a chegada dos eventos usando `kafka-console-consumer`.

Entrega:
- ficheiro `bike_producer.py`
- captura de pantalla do terminal co `kafka-console-consumer` recibindo eventos

---

## 1) A API de CityBikes (que é e como se usa)

CityBikes ofrece unha API pública (sen chave) para consultar información de sistemas de bicicleta compartida en moitas cidades do mundo.

### Endpoints principais

1) Listado de redes dispoñibles (para descubrir IDs):
   - `GET https://api.citybik.es/v2/networks`

   Resposta (simplificada):
   - `networks[]`: lista de redes
     - `id`: identificador da rede (o que se usa para pedir detalles)
     - `name`: nome
     - `location`: cidade/país e coordenadas

2) Detalle dunha rede concreta (inclúe estacións):
   - `GET https://api.citybik.es/v2/networks/{network_id}`

   Resposta (simplificada):
   - `network`:
     - `id`, `name`
     - `location`
     - `stations[]`: lista de estacións
       - `id`, `name`
       - `latitude`, `longitude`
       - `free_bikes`, `empty_slots`
       - `timestamp` (da propia estación, se vén informado)
       - `extra` (campos adicionais segundo rede)

### Que redes hai? (como escoller unha)

Como hai moitas, o proceso recomendado é:
1. Consultar `GET /v2/networks`
2. Buscar unha rede coñecida e copiar o seu `id`
3. Usar ese `id` en `GET /v2/networks/{id}`

Exemplos típicos (dependen do listado actual que devolva a API):
- `bicing` (Barcelona)
- `bicimad` (Madrid)
- `valenbisi` (Valencia)
- outras redes internacionais

Nesta práctica seleccionarase unha rede e traballarase co seu `network_id`.

---

## 2) Que debe facer `bike_producer.py` (paso a paso)

O produtor vai funcionar como unha “fonte de datos” que publica eventos continuamente.

### Paso 1 — Definir configuración e rede a consultar
- Definir o `NETWORK_ID` (a rede elixida)
- Definir o `TOPIC` onde publicar (ex.: `citybikes-stations`)
- Definir cada canto tempo se consulta a API (ex.: 10 segundos)

### Paso 2 — Crear un `KafkaProducer`
- Conectarse ao broker (`kafka:9092`)
- Engadir `value_serializer` para enviar JSON en UTF-8

### Paso 3 — Nun bucle infinito, consultar a API
- Construír URL: `https://api.citybik.es/v2/networks/{NETWORK_ID}`
- `requests.get(..., timeout=10)`
- `raise_for_status()`
- `json()` para obter un dict de Python

### Paso 4 — Extraer e normalizar os datos de estacións
- Ler `network.stations` (lista)
- Para cada estación:
  - Construír un evento pequeno e “limpo”, por exemplo:
    - `network_id`, `network_name`
    - `station_id`, `station_name`
    - `latitude`, `longitude`
    - `free_bikes`, `empty_slots`
    - `api_timestamp` (se existe)
    - `local_timestamp` (momento no que se inxesta/produce)

### Paso 5 — Publicar eventos en Kafka
- Enviar un evento por estación:
  - `producer.send(TOPIC, value=event)`
- Para evitar un volume excesivo, pódese:
  - limitar a N estacións (ex.: as primeiras 20)
  - ou seleccionar unha mostra aleatoria

### Paso 6 — Manexar erros e controlar frecuencia
- `try/except` para que unha caída puntual non pare o produtor
- `time.sleep(INTERVAL_SECONDS)` ao final do ciclo

---

## 3) Código do produtor: `bike_producer.py`

Notas didácticas:
- Publica 1 evento por estación (limitado por `MAX_STATIONS`).
- Engade `local_timestamp` para que no laboratorio se vexa claramente que van chegando eventos “novos”.
- Mantén o evento pequeno (máis cómodo para Kafka e para consumir por consola).


# 4) Probar a recepción coa consola (kafka-console-consumer)

Abrir un terminal no contedor/entorno onde estea Kafka CLI e executar:

**Consumidor en tempo real (imprime as mensaxes segundo chegan):**

```bash
kafka-console-consumer --bootstrap-server kafka:9092 --topic citybikes-stations --from-beginning
```

**Notas:**

- `--from-beginning` permite ver tamén o xa publicado (útil se arrincas o consumidor despois do produtor).
- Se só queres ver o que chegue a partir de agora, quita `--from-beginning`.


---

## 5) Checklist de verificación rápida

**A API responde?**

Probar no navegador ou con `curl`:

- https://api.citybik.es/v2/networks  
- https://api.citybik.es/v2/networks/bicimad (ou a rede elixida)

**O produtor imprime “Enviado: {...}” de forma periódica?**

**O consumidor mostra JSONs entrando no topic?**


---

## 6) Que entregar

- Código de `bike_producer.py` 
- Captura do terminal executando o `kafka-console-consumer` recibindo eventos
