# 1. Kafka: que é e que problema resolve

Apache Kafka é unha plataforma distribuída para a xestión de fluxos de eventos (*event streaming*). Está deseñada para permitir a publicación, almacenamento e consumo de grandes volumes de datos en tempo case real, de forma escalable e tolerante a fallos.

Tradicionalmente, os sistemas intercambiaban datos mediante:
- chamadas directas a APIs,
- intercambio de ficheiros,
- sistemas de mensaxería punto a punto.

Estas aproximacións presentan limitacións cando o volume de datos é elevado, os produtores e consumidores son numerosos ou é necesario reprocesar información histórica.

Kafka resolve estes problemas actuando como un **log distribuído de eventos**, no que:
- os produtores publican eventos,
- os consumidores len os eventos ao seu propio ritmo,
- os datos consérvanse durante un tempo configurable, independentemente de se foron consumidos ou non.

Kafka resulta especialmente adecuado para:
- recoller datos de sensores ou dispositivos IoT,
- xestionar logs de aplicacións,
- integrar múltiples sistemas mediante eventos,
- servir como capa de inxestión en arquitecturas Big Data.

flowchart TD
    AppA(Producer A)
    AppB(Producer B)
    Broker1[Kafka Broker 1]
    Broker2[Kafka Broker 2]
    Consumer1(Consumer Group)
    DB[(Database)]

    AppA -->|eventos| Broker1
    AppB -->|eventos| Broker2
    Broker1 --> Consumer1
    Broker2 --> Consumer1
    Consumer1 -->|procesa e garda| DB

---

# 2. Arquitectura básica de Kafka

Kafka está baseado nunha arquitectura distribuída composta por varias pezas fundamentais que traballan conxuntamente para garantir escalabilidade e tolerancia a fallos.

## Broker e cluster

Un **broker** é unha instancia de Kafka que almacena datos e atende as peticións de produtores e consumidores. Un conxunto de brokers forma un **cluster Kafka**, no que os datos se distribúen entre os distintos nodos.

A existencia de varios brokers permite:
- repartir a carga de traballo,
- escalar horizontalmente,
- evitar puntos únicos de fallo.

## Topics e particións

Os datos en Kafka organízanse en **topics**, que poden entenderse como canles lóxicas de eventos.

Cada topic divídese en **particións**, que son logs ordenados e inmutables. As particións permiten:
- paralelizar a lectura e escritura de datos,
- distribuír os datos entre distintos brokers.

Dentro dunha partición, as mensaxes manteñen a orde na que foron publicadas.

## Replicación

Para garantir a tolerancia a fallos, cada partición pode estar **replicada** en varios brokers. Unha das réplicas actúa como líder, mentres que as demais funcionan como réplicas secundarias.

Se o broker líder falla, outra réplica pode asumir o seu papel, garantindo a dispoñibilidade dos datos.

## ZooKeeper e KRaft

Tradicionalmente, Kafka empregaba **ZooKeeper** para a xestión do estado do cluster. Nas versións máis recentes, Kafka introduce **KRaft**, un mecanismo propio que elimina esta dependencia externa.

A nivel conceptual, ambos sistemas cumpren a mesma función: coordinar os brokers e manter a coherencia do cluster.

---

# 3. Modelo de datos en Kafka

Kafka traballa con **eventos**, que representan feitos ocorridos nun determinado momento. Cada evento publícase como unha mensaxe inmutable dentro dun topic.

## Estrutura dunha mensaxe

Un evento en Kafka pode conter os seguintes elementos:
- **key**: valor opcional empregado para decidir en que partición se almacena a mensaxe.
- **value**: contido principal do evento (habitualmente en formato JSON).
- **headers**: metadatos adicionais en formato clave-valor.
- **timestamp**: instante temporal asociado ao evento.

Kafka non impón un formato concreto para o `value`, polo que é responsabilidade do produtor definir a estrutura dos datos.

## Importancia da key

A *key* permite controlar a distribución dos eventos entre particións. As mensaxes coa mesma key sempre se almacenan na mesma partición, o que garante a orde relativa entre eventos relacionados.

Un uso adecuado da key é fundamental para:
- manter coherencia nos datos,
- facilitar o procesamento posterior,
- evitar desequilibrios entre particións.

## Inmutabilidade e orde

Unha vez escrita, unha mensaxe non pode ser modificada nin eliminada individualmente. Kafka conserva os eventos segundo a política de retención configurada para o topic.

A orde dos eventos só está garantida **dentro de cada partición**, non a nivel global do topic.

## Boas prácticas no deseño de eventos

Ao definir eventos para Kafka, recoméndase:
- empregar eventos pequenos e autocontenidos,
- incluír información temporal clara,
- evitar estruturas excesivamente complexas,
- manter un esquema consistente ao longo do tempo.
