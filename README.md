# Notebooks spark

Este repositorio contén **apuntes de teoría** en Markdown e **cadernos Jupyter** con exemplos guiados para traballar con Apache Spark (RDDs, DataFrames e Streaming).

---

## sobre o entorno de execución

Estes notebooks están pensados para executarse nun clúster local de big data baseado en **Hadoop + Spark + Jupyter**, despregado mediante Docker Compose.

▶️ **Recoméndase empregar o clúster definido no seguinte repositorio:**
https://github.com/adbgonzalez/big-data-cluster

Ese repositorio fornece:
- un clúster Hadoop (HDFS + YARN)
- Apache Spark configurado para a execución de aplicacións sobre YARN
- un contedor de JupyterLab preparado para traballar con PySpark

As instrucións para montar o entorno completo, clonar os repositorios e arrincar o clúster atópanse no README do repositorio do clúster.

--- 

## estrutura do repositorio

- [`apuntes_teoría/`](./apuntes_teoría/) → **apuntes de teoría en Markdown** (listados un a un máis abaixo)
- [`cuadernos/`](./cuadernos/) → **cadernos Jupyter** guiados por temática (listados un a un máis abaixo)
- [`Ejercicios/`](./Ejercicios/) → colección de exercicios propostos (ver cartafol)
- [`Soluciones/`](./Soluciones/) → solucións e versións actualizadas de cadernos/exercicios
- [`Examen/`](./Examen/) → material de exame (inclúe un cartafol `data/` propio)
- [`data/`](./data/) → conxunto de datasets para prácticas (ver cartafol)
- [`images/`](./images/) → imaxes comúns (algúns documentos tamén teñen `images/` propio)

---

## apuntes de teoría (markdown)

Cartafol: [`apuntes_teoría/`](./apuntes_teoría/)

1. [`00.intro_procesamento.md`](./apuntes_teoría/00.intro_procesamento.md)  
   introdución ao procesamento distribuído e contexto xeral.

2. [`00A.motores_procesamento.md`](./apuntes_teoría/00A.motores_procesamento.md)  
   comparativa entre motores de procesamento distribuído (mapreduce, spark, flink, storm, dask…).

3. [`00B.linguaxes_e_motores_procesamento.md`](./apuntes_teoría/00B.linguaxes_e_motores_procesamento.md)  
   linguaxes empregadas no procesamento distribuído (java/scala/python/sql…) e exemplos tipo *word count* por motor/linguaxe.

4. [`01.Apuntes_Spark_Teoria.md`](./apuntes_teoría/01.Apuntes_Spark_Teoria.md)  
   teoría base de Spark (arquitectura, execución, conceptos clave…).

5. [`02.streaming_structured_streaming.md`](./apuntes_teoría/02.streaming_structured_streaming.md)  
   teoría e modelo de programación de **Spark Structured Streaming** (estado, watermarking, sinks, triggers…).

> Nota: existe material antigo/arquivado en `apuntes_teoría/OLD_02.StreamDataFrames_mejorado.md`, útil como referencia histórica, pero non é a versión principal.

---

## cadernos (notebooks)

Cartafol xeral: [`cuadernos/`](./cuadernos/)

### 01 – RDDs
Cartafol: [`cuadernos/01-RDDs/`](./cuadernos/01-RDDs/)

- [`01-rdd1.ipynb`](./cuadernos/01-RDDs/01-rdd1.ipynb)
- [`02-rdd2.ipynb`](./cuadernos/01-RDDs/02-rdd2.ipynb)

### 02 – DataFrames
Cartafol: [`cuadernos/02-DataFrames/`](./cuadernos/02-DataFrames/)

- [`03-Dataframes.ipynb`](./cuadernos/02-DataFrames/03-Dataframes.ipynb)
- [`04-Dataframes2.ipynb`](./cuadernos/02-DataFrames/04-Dataframes2.ipynb)

*(inclúe imaxes de apoio en `cuadernos/02-DataFrames/images/`)*

### 03 – Streaming (Structured Streaming)
Cartafol: [`cuadernos/03-Streaming/`](./cuadernos/03-Streaming/)

Cadernos base e exemplos curtos:
- [`00.StreamDataFrames.ipynb`](./cuadernos/03-Streaming/00.StreamDataFrames.ipynb)
- [`01.ejemplo_socket.ipynb`](./cuadernos/03-Streaming/01.ejemplo_socket.ipynb)
- [`02.ejemplo_archivo.ipynb`](./cuadernos/03-Streaming/02.ejemplo_archivo.ipynb)
- [`03.a.ejemplo_kafka_1.ipynb`](./cuadernos/03-Streaming/03.a.ejemplo_kafka_1.ipynb)
- [`03.b.ejemplo_kafka_2.ipynb`](./cuadernos/03-Streaming/03.b.ejemplo_kafka_2.ipynb)
- [`03.c.ejejmplo_kafka_3.ipynb`](./cuadernos/03-Streaming/03.c.ejejmplo_kafka_3.ipynb)
- [`04.a.ejemplo_ventanas.ipynb`](./cuadernos/03-Streaming/04.a.ejemplo_ventanas.ipynb)
- [`04.b.ejemplo_ventanas-2.ipynb`](./cuadernos/03-Streaming/04.b.ejemplo_ventanas-2.ipynb)
- [`04.c.ejemplo_ventanas_3.ipynb`](./cuadernos/03-Streaming/04.c.ejemplo_ventanas_3.ipynb)
- [`04.d.ejemplo_ventanas_watermarking.ipynb`](./cuadernos/03-Streaming/04.d.ejemplo_ventanas_watermarking.ipynb)

Exemplos completos (streaming):
- [`Ejemplo 1 - OpenMeteo/`](./cuadernos/03-Streaming/Ejemplo%201%20-%20OpenMeteo/)  
  pipeline con produtores e procesamento sobre datos meteorolóxicos (inclúe guía e scripts auxiliares).

- [`Ejemplo 2 - join de dos fuentes/`](./cuadernos/03-Streaming/Ejemplo%202%20-%20join%20de%20dos%20fuentes/)  
  exemplo de *join* en streaming de dúas fontes (inclúe produtores e scripts).

*(inclúe imaxes de apoio en `cuadernos/03-Streaming/images/` e material antigo en `cuadernos/03-Streaming/old/`)*

---

## exercicios e datasets

- Exercicios propostos: [`Ejercicios/`](./Ejercicios/)  
- Datasets para prácticas: [`data/`](./data/)

> Estes cartafoles non se listan aquí ficheiro a ficheiro: a idea é que o alumnado os explore segundo a práctica indicada.

---

## notas rápidas de uso

- Para seguir o temario en orde: **apuntes_teoría → cuadernos → ejercicios**.
- Para streaming: revisar primeiro  
  [`apuntes_teoría/02.streaming_structured_streaming.md`](./apuntes_teoría/02.streaming_structured_streaming.md)  
  e despois traballar cos cadernos de [`cuadernos/03-Streaming/`](./cuadernos/03-Streaming/).
