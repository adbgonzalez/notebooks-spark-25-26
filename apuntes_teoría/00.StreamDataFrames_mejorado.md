# Procesamiento en Streaming
En la actualidad se están generando datos en tiempo real continuamente. Por ello, es deseable procesar estos datos también en tiempo real y generar unos nuevos datos correctamente estructurados, limpios y que generen valor para las empresas.
Tradicionalmente el procesamiento se realizaba por lotes (batch). En determinado momento, se obtienen los datos de una fuente de datos, se procesan empleando una herramienta diseñada para ello (Hadoop Map Reduce, Spark, etc.) y el almacenan o se envían a un destino.
## Tipos de procesamiento:
- **Procesamiento por lotes**: Es el origen del Big Data, formado por procesos que se basan en el paradigma map-reduce. Se procesan de forma periódica grandes volúmenes de datos. El mayor ejemplo de este paradigma es Apache Hadoop, que emplea el framework Hadoop MapReduce y el planificador Yarn. Los tiempos de respuesta suelen ser bastante altos (segundos, minutos, horas…). En los últimos años ha ido ganando peso Apache Spark, que reduce los tiempos de respuesta al no necesitar realizar escrituras intermedias en disco, pero que necesita una cantidad de memoria RAM sensiblemente mayor.
- **tiempo real**: Es el modelo de procesamiento con menor latencia, con tiempos de respuesta entorno al milisegundo o incluso inferiores. En este modelo las aplicaciones normalmente lanza una petición y se bloquean esperando respuesta. Se usa en puntos de venta, procesamiento de tarjetas de crédito, etc.
- **procesamiento en streaming**: Surge para dar respuesta a la necesidad de las empresas de procesar los datos que se generan continuamente. Está a medio camino entre el procesamiento por lotes y el procesamiento en tiempo real: no es necesario obtener respuestas en unos pocos milisegundos pero tampoco es admisible esperar varias horas o, incluso, más de un día. Los datos se procesan nada más ser “ingestados” y, por lo general, se puede afirmar que la cantidad de datos procesada es “pequeña” en comparación con el procesamiento batch. Además, se procesan sin bloqueo, diferenciándose así del procesamiento en tiempo real.
## Spark Structured Streaming
Motor de procesamiento en streaming escalable y tolerante a fallos construido sobre el motor Spark SQL. Los cálculos en streaming se expresan de forma muy simular a como se haría en computación por lotes sobre datos estáticos. El motor Spark SQL se encarga de ejecutarlos de forma incremental actualizando los resultados finales continuamente mientras el flujo de datos siga llegando. Está permitido usar la API de Dataframes/Datasets en Scala, Java, Python o R para expresar agregaciones en streaming, ventanas de tiempo-eventos, joins de streaming a batch, etc. Por otra parte, el sistema asegura tolerancia a fallos de extremos a extremo de tipo exactamente una vez (exactly-once) a través de checkpoints y logs write-ahead.
Internamente, por defecto, las consultas de Structured Streaming  se procesan usando un motor de procesamiento micro-batch, que procesa flujos de datos como series de pequeños trabajos por lotes alcanzando latencias de entorno a 100 milisegundos y tolerancia a fallos exactly-once. Sin embargo, a partir de Spark 2.3, está disponible un nuevo modo de procesamiento de baja latencia denominado Procesamiento continuo, que proporciona latencias de 1 milisegundo con garantía at-least-once. Independientemente del modo usado, las operaciones sobre Dataframes y Datasets siguen siendo las mismas.
### Modelo de programación
La idea clave en Structured Streaming es tratar un flujo de datos activo como una tabla a la que se le están añadiendo filas continuamente. La forma de especificar las operaciones es muy similar a cuando se usa procesamiento por lotes: expresamos la computación en Streaming de forma muy similar a como lo haríamos con una consulta batch  en una tabla estática y Spark la ejecuta de forma incremental sobre la tabla ilimitada ( unbounded) de entrada.
#### Conceptos básicos
Consideremos el flujo de entrada de datos como una “tabla de entrada”. Cada ítem de datos que llega a través del flujo es una nueva fila que se añade a dicha tabla:
![ubounded table](./images/unbounded_table.png)
Una consulta sobre la tabla de entrada genera la “tabla de resultados”. Cada cierto intervalo de tiempo (ejemplo: 1 segundo) se añaden nuevas filas a la tabla de entrada lo que eventualmente actualiza la Tabla de Resultados. Cada vez que esta última cambia, lo ideal sería escribir las filas que han cambiado a través de un sumidero externo (sink).
![modelo de programación](./images/programming_model.png)
La salida (Output) define lo que se escribe al almacenamiento externo. Existen diferentes modos:
- **Modo completo** (complete mode): La Tabla de Resultados al completo se escribe en el almacenamiento externo
- **Modo añadir** (append mode): Sólo se escriben al almacenamiento externo las filas de la Tabla de Resultados que se han añadido durante el último intervalo.
- **Modo actualización** (Update Mode): Sólo se escriben al almacenamiento externo las filas de la Tabla de Resultados que han cambiado durante el último intervalo.
El modo usado depende en gran medida del tipo de consulta a realizar, cada modo es adecuado para determinados tipos de consultas. Más adelante se tratará este tema en más profundidad


# Uso de DataFrames en Structured Streaming
Structured Streaming permite realizar operaciones con Stream DataFrames/DataSets muy similares a aquellas permitidas para los DataFrames/DataSets estáticos, con algunas restricciones. En general, si queremos realizar procesamiento en streaming usando DataFrames/DataSets el procedimiento es el siguiente
  1. Crear un objeto SparkSession. Para utilizar determinados orígenes/destinos de datos puede ser necesario usar alguna opción especial. Abajo podemos observar un ejemplo general y otro específico para usar Kafka como fuente/destino de datos.
  2. Crear el Stream DataFrame/DataSet empleando alguna de las fuentes de datos disponibles
  3. Realizar operaciones de transformación sobre el DataFrame/DataSet hasta dar a los datos la forma que se necesite.
  4. Iniciar el procesamiento en Streaming, especificando un *sink* (destino de los datos)
![Flujo de trabajo en Spark Structured Streaming](./images/flujo_sss.png)



```python
# Inicialización del objeto SparkSession para socket y archivo

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import string


spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("StreamDataFrames") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "hdfs:///spark/logs/history") \
    .config("spark.history.fs.logDirectory", "hdfs:///spark/logs/history") \
    .getOrCreate()
```


```python
# Inicialización del objeto SparkSession para Kafka

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import os
import pyspark

scala_version = '2.12'  # TODO: Ensure this is correct
spark_version = pyspark.__version__

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.5.1'
]

args = os.environ.get('PYSPARK_SUBMIT_ARGS', '')
if not args:
    args = f'--packages {",".join(packages)}'
    print('Using packages', packages)
    os.environ['PYSPARK_SUBMIT_ARGS'] = f'{args} pyspark-shell'
else:
    print(f'Found existing args: {args}') 


```


## 1. Creación de Streaming DataFrames 
Los Streaming DataFrames pueden crearse a través de la interfaz DataStreamReader que se obtiene a través de la función readStream() del objeto SparkSession.
Existen diversas fuentes de entrada:
- **Archivo**: Lee los ficheros de un directorio como un flujo de datos.Los archivos se procesan en orden según la hora de modificación del archivo. La opción latestFirst permite invertir el orden. Se soportan varios formatos, como texto, CSV, JSON, ORC o Parquet. Los archivos deben estar colocados directamente en el directorio indicado.
- **Kafka**: Lee datos desde Kafka. Compatible con la versión 0.10.0 o superiores.
- **Socket (pruebas)**: Lee texto en formato UTF8 a través de una conexión tipo socket. Esta opción solo es recomendada para pruebas
- **Rate source (pruebas)**: Genera datos a un número especificado de filas por segundo, cada una de ellas con un *timestamp* y un *valor* de tipo *Long* que contiene el conteo de mensajes (empezando en 0). Esta fuente se utiliza para pruebas y *benchmarks*
- **Rate per Micro-Batch source (pruebas)**:

### Archivo
Al utilizar la fuente de entrada *archivo* tenemos varias opciones que configurar:
- **path**: Ruta al directorio de entrada
- **maxFilesPerTrigger**: Máximo número de archivos nuevos para considerar en cada intervalo (por defecto: no hay)
- **latestFirst**: Indica que los archivos más recientes se han de procesar primero. (por defecto: *falso*).
- **fileNameOnly**: Indica si comprobamos los nuevos archivo en base a su nombre en vez de a la ruta absoluta. Así, varios ficheros con el mismo nombre pero ubicados en rutas distintas serían considerados el mismo archivo.
- **maxFileAge**: Antigüedad máxima de archivo, los que sobrepasen dicha antigüedad serán ignorados. Si las opciones *latestFirst* y *maxFilesPerTrigger* tienen valores ésta será ignorada. La antigüedad máxima see considera respecto al *timestamp* del archivo más nuevo, no al actual (por defecto: 1 semana).
- **cleanSource**: Opción de "limpiar" los archivos completados después de su procesamiento.


```python

```


```python
# Ejemplo de file source

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("DEST_COUNTRY_NAME", StringType(), True),
    StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
    StructField("count", IntegerType(), True)
])
streaming_file_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .csv("/home/jovyan/work/data/flight-data/csv")



```

### Socket
Al utilizar la fuente **socket** (sólo pruebas) deben especificarse las siguientes opciones:
- **host**: Nombre de host al que conectarse, obligatorio.
- **port**: Número de puerto al que conectarse, obligatorio.


```python
# Ejemplo de socket source

socket_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()



```

### Kafka
Ver ejemplos específicos


```python
# ejemplo de kafka

# Suscribirse a un topic (entrada)
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-1:9092") \
  .option("subscribe", "entrada") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

```

## 2. Operaciones con Streaming DataFrames
Se pueden aplicar distintos tipos de operaciones sobre *Streaming DataFrames*, desde operacions estilo SQL (select, where, groupBy...) a operaciones tipadas estilo RDD (map, filter, flatMap...).
### Operaciones básicas
La mayoría de las operaciones comunes sobre *DataFrames* están soportadas también en Streaming. Más adealente se hablará de las que no lo están


```python
# Ejemplo operaciones básicas

df = ...  # streaming DataFrame with IOT device data with schema { device: string, deviceType: string, signal: double, time: DateType }

# Select the devices which have signal more than 10
df.select("device").where("signal > 10")

# Running count of the number of updates for each device type
df.groupBy("deviceType").count()



```


```python
# Realiza operaciones de streaming, por ejemplo, contar los registros por cada grupo
counts = streaming_file_df.groupBy("columna_a_agrupar").count()
```

### Ventanas
Las operaciones de agregación sobre una ventana deslizante de tiempo de evento son directas en Structured Streaming y muy similares a las agregaciones agrupadas. La diferencia es que en las **agregaciones basadas en ventanas** los valores agregados se mantienen para cada ventana de tiempo de evento a la que pertenece una determinada fila.
Imaginemos que en el ejemplo de **wordCounts**, además de los datos, cada fila contiene el instante en que fue generada. En vez de realizar un conteo total de las ocurrencias de cada palabra queremos contar dichas ocurrencias en intervalos de 10 minutos, actualizando los resultados cada 5. Las tablas de resultados tendrían un aspecto muy similar al siguiente: 

![ventanas deslizantes](./images/structured-streaming-window.png)

Ya que las agrupaciones y las ventanas son operaciones similares, pueden usarse las operaciones *groupBy()* y *window()* para expresar agregaciones con ventanas. Se distinguen tres tipos de ventanas:
- **Ventanas fijas**: duración determinada.
- **Ventanas deslizantes**: duración determinada y con solapamiento.
- **ventanas de sesión**: duración indeterminada, dependiente de la sesión.

![tipos de ventanas](./images/structured-streaming-time-window-types.jpg)



```python
# Ejemplo ventanas 

words = ...  # streaming DataFrame of schema { timestamp: Timestamp, word: String }

# Group the data by window and word and compute the count of each group
windowedCounts = words.groupBy(
    window(words.timestamp, "10 minutes", "5 minutes"),
    words.word
).count()
```

### Manejo de datos tardíos y marcas de agua (Watermarking)
¿Qué pasa si algún evento llega tarde a la aplicación? Es posible que el tiempo de evento de los datos sea anterior al intervalo de la ventana. Structured Streaming puede mantener el estado intermedio para agregaciones parciales por un largo periodo de tiempo, consiguiendo así que los datos tardíos actualicen agregaciones de ventanas antiguas, como se muestra en la siguiente ilustración:
![datos tardíos](./images/structured-streaming-late-data.png)
Para ejecutar esta consulta durante largos periodos de tiempo es necesario especificar la cantidad de memoria que reserva el sistema para almacenar estados intermedios. Es decir, el sistema necesita saber cuando puede  eliminar de memoria una agregación antigua debido a que la aplicación no va a recibir más datos para esa ventana. Debido a esto, en la versión 2.1 de Spark se introduce el concepto de **watermarking**. Podemos definir la marca de agua (*watermark*) de una consulta especificando la columna de *tiempo de evento* y el umbral de cómo de tarde se espera que puedan llegar los datos a la aplicación. Es decir, los datos tardíos que lleguen durante ese umbral serán añadidos y los que lleguen más tarde descartados. Para definir una marca de agua basta con usar la función *withWatermark()*:


```python
# Ejemplo Watermark:
words = ...  # streaming DataFrame of schema { timestamp: Timestamp, word: String }

# Group the data by window and word and compute the count of each group
windowedCounts = words \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(words.timestamp, "10 minutes", "5 minutes"),
        words.word) \
    .count()
```

EJEMPLOS


```python
# 1
grouped_streaming_file_df = streaming_file_df.groupBy("DEST_COUNTRY_NAME").count()

#windowed_file_df = streaming_file_df \
#   .withWaterMark("timestamp", "10 minutes") \
#    .grooupBy(
#       window(
```


```python
# 2
# Split the lines into words
words = socket_df.select(
        explode(
split(socket_df.value, " ")
   ).alias("word")
)

 # Generate running word count
wordCounts = words.groupBy("word").count()
```


```python
# 3

words = df.select(
        explode(
split(df.value, " ")
   ).alias("word")
)

 # Generate running word count
wordCounts = words.groupBy("word").count()
```

## 3. Inicio del procesamiento en Streaming
Una vez está definido el DataFrame resultado, todo lo que queda es inicial la computación en streaming. Para ello, se usa el objeto *DataStreamWriter* devuelto por la función *writeStream()* de la clase *DataFrame*. Es necesario espeficar algunos de los siguientes aspectos:
- **sink**: Sumidero o destino de los datos. Hay que especificar formato, localización, etc.
- **Output mode**: Permite especificar qué se escribe en el *sink* de salida.
- **Query**: Opcionalmente, se puede especificar un nombre único para identificar la consulta.
- **Trigger interval**: Intervalo 
- **Checkpoint location**: En algunos *sinks* es necesario especificar la localización donde se va a almacenar toda la información de control par garantizar la tolerancia a fallos de extremo a extremo (*end-to-end fault-tolerance*).
### Modos de salida
- **Modo adición (por defecto)**: Sólo se envían al destino las nuevas filas añadidas a la Tabla Resultado durante el último intervalo. Por lo tanto, sólo soportan este modo consultas que no cambian las filas que ya han sido añadidas a la Tabla Resultado ( select, where, map, flatmap, filter, join, etc.).
- **Modo completo**: La Tabla Resultado al completo es enviada al destino de datos en cada intervalo. Este modo soporte consultas de agregación.
- **Modo actualización**: Sólo se envían al destino las filas modificadas durante el último intervalo.
No todos los modos son adecuados para todos los tipos de consultas. A continuación se muestra una tabla de compatibilidades e incompatibilidades:
|Tipo de consulta | subtipo | Modos soportados | notas |
| --------------- | ------- | ---------------- | ----- |
| Consultas con agregación | Agregación en evento-tiempo con watermark | Append, Update, Complete | |
| | Otras agregaciones | Complete, Update |  |
| Consultas con mapGroupsWithState | | Update | No se permiten agregaciones |
| Consultas con flatMapGroupsWithState | Modo de operación Append | Append | Se permiten agregaciones **después** de la transformación *mapGroupsWithState* |
| | Modo de operación Update | Update | No se permiten agregaciones **después** de la transformación *mapGroupsWithState* |
| Consultas con joins | | Append | Los modos update y complete no está soportados todavía |
| Otras consultas | | Append, Update | El modo complete no está soportado |
### Sinks (Destinos de datos)
Hay los siguientes tipos de *sinks*:
- File sink: Almacena la salida en un directorio
- Kafka sink: Almacena la salida en uno o varios topics de Kafka
- Foreach sink: Realiza un procesamiento adicional sobre las filas de la salida.
- Console sink (pruebas): Imprime la salida por consola cada intervalo. Soporta los modos completo y adición. NOTA: En jupyter lo imprime en la consola, no en el notebook.
- Memory sink (pruebas): Almacena la salida en memoria. Soporta los modos adición y completo. NOTA: Para hacer pruebas en jupyter es el más indicado. En los ejemplos se muestra como se pueden visualizar los datos.

Algunos *sinks* no son tolerantes a fallos y están pensados sólo para pruebas y depuración. A continuación se muestra una tabla comparativa de *sinks*:

| Sink | Modos soportados | opciones | Tolerancia a fallos | Notas |
| ---- | ---------------- | -------- | ------------------- | ----- |
| File | Adición | path: la ruta al directorio de salida | Sí (exactly-once) | Soporta escritura a tablas particionadas. |
| Kafka | Adición, actualización y completo | Ver Ejemplo-1 | Sí (at-least-once) |  |
| Foreach | Adición, actualización y completo | - | Depende de la implementación |  | 
| Console | Adición, actualización y completo | *numRows*: número de filas a imprimir en cada intervalo (por defecto: 20). *truncate*: si truncar o no cuando la salida es demadsiado larga (por defecto: true) | No | |
| Memory | Adición, completo | - | No | El nombre de la tabla es el de la consulta |



```python
# File sink
#1 
query = streaming_file_df \
    .writeStream \
    .format("parquet") \
    .option("path", "/home/jovyan/output") \
    .option("topic", "notificaciones") \
    .outputMode("append") \
    .option("checkpointLocation", "/home/jovyan/chk-point-dir") \
    .start()

```


```python
# Kafka sink

from pyspark.sql.functions import to_json
kafka_output_topic = "salida"

kafka_output_config = {
    "kafka.bootstrap.servers": "kafka-1:9092",  # Coloca aquí los servidores de arranque de Kafka
    "topic": kafka_output_topic
}

query2 = wordCounts \
    .selectExpr("CAST(word AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .outputMode("complete") \
    .options(**kafka_output_config) \
    .option("checkpointLocation", "/home/jovyan/checkpoints") \
    .start()


```


```python
# Foreach



```


```python
# ForeachBatch



```


```python
# Console sink
from time import sleep  # Agregar importación para sleep
query = grouped_streaming_file_df.writeStream.format("console").outputMode("complete").start()
#query.awaitTermination()

print ("CONSULTA ACTIVA!!!!")
#while True:
   
#    print(query.status)
#    print(spark.sql('SELECT * FROM consulta_file2').show())
#    sleep(1)
query.awaitTermination()
```


```python
# Memory sink
from IPython.display import display, clear_output
from time import sleep  # Agregar importación para sleep

 # Start running the query that prints the running counts to the console
query = wordCounts \
          .writeStream \
          .outputMode("complete") \
          .format("memory") \
          .queryName("consulta1") \
          .start()



while query.isActive:
    clear_output(wait=True)
    display(query.status)
    display(spark.sql('SELECT * FROM consulta1').show())
    sleep(1)
```


```python
# Memory sink 2
from IPython.display import display, clear_output
from time import sleep  # Agregar importación para sleep

 # Start running the query that prints the running counts to the console
query = grouped_streaming_file_df \
          .writeStream \
          .outputMode("complete") \
          .format("memory") \
          .queryName("consulta2") \
          .start()



while query.isActive:
    clear_output(wait=True)
    display(query.status)
    display(spark.sql('SELECT * FROM consulta2').show())
    sleep(1)
```

### Disparadores
La configuración de los disparadores de una consulta en streaming define la temporalización del procesamiento de datos, si se va ejecutar como una consulta micro-batch o como una consulta de procesamiento continuo. Estos son los distintos tipos de disparadores soportados:
#### Por defecto
Si no se especifica el tipo de disparador la consulta se ejecutará en modo **micro-batch**, donde cada micro-batch se generará en el momento en que el anterior haya completado su procesamiento
#### Micro-batches de intervalo fijo
La consulta se ejecutará en modo **micro-batch**, donde cada micro-batch se lanzará en intervalos especificados por el usuario.
- Si el micro-batch anterior finaliza durante el intervalo, el motor esperará a que éste termine antes de lanzar el siguiente micro-batch
- Si el micro-batch anterior lleva más tiempo que el intervalo, el siguiente se lanzará una vez termine el procesamiento.
- Si no hay nuevos datos disponibles, no se lanzará ningún micro-batch.
#### One-time micro-batch (obsoleto)
La consulta ejecutará **un único** micro-batch para procesar todos los datos disponibles y luego detenerse. Está obsoleto y se recomienda el uso del disparador **Available-now micro-batch** en su lugar.
#### Available-now micro-batch
Similar al anterior. La diferencia radica en que los datos serán procesados (posiblemente) en múltiples micro-batches según las opciones de la fuente de datos (por ejemplo *maxFilesPerTrigger*), lo que proporciona una mayor escalabilidad.
- Este disparador proporciona una fuerte garantía de procesamiento: Independientemente del número de batches que falten de la anterior ejecución, adegura que todos los datos disponibles serán ejecutados antes de la terminación. Los batchs incompletos serán ejecutados primero.
- La *marca de agua* avanza por cada batch. 
#### Continuo con intervalo fixed-point (experimental)
La consulta se ejecutará en modo de procesamiento continuo de baja latencia. Es un modo experimental y presenta una serie de incompatibilidades:
- *Operaciones*: Sólo proyecciones (select, map, flatMap, mapPartitions, etc.) y selecciones (where, filter, etc.).
- *Fuentes*:
  - Kafka: Todas las opciones permitidas
  - Rate source: sólo numPartitions y rowsPerSecond
- *Destinos*:
  - Kafka: Todas las opciones permitidas.
  - Memory: Válido para depurar.
  - Console: Todas las opciones permitidas




```python
# Por defecto

query = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()



```


```python
# Fixed-interval micro-batch

from pyspark.sql.streaming import Trigger

query = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()


```


```python
# One-time micro-batch

query = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(once=True) \
    .start()


```


```python
# Available-now micro-batch


query = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start()

```


```python
# Continuous mode

spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "topic1") \
  .load() \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("topic", "topic1") \
  .trigger(continuous="1 second") \
  .start()



```
