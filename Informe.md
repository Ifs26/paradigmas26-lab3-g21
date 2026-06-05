# Informe — Laboratorio 3: Procesamiento distribuido con Apache Spark

## Ejercicio 1 — Identificar las regiones paralelizables

### a) Diagrama de flujo del pipeline

El pipeline tiene los siguientes pasos, en orden. Cada flecha indica el tipo Scala del
dato que fluye entre un paso y el siguiente.

```
[JSON de suscripciones]
           |
           | String (ruta del archivo)
           v
  1. Leer suscripciones
     (FileIO.readSubscriptions)
           |
           | List[Option[Subscription]]  →  aplanar  →  List[Subscription]
           v
  2. Descargar cada feed (HTTP)
     (FileIO.downloadFeed)
           |
           | List[(Boolean, List[Post])]
           v
  3. Parsear los posts de cada feed
     (JsonParser.parsePosts)
           |
           | List[Post]   (todos los posts de todos los feeds)
           v
  4. Filtrar posts vacíos
     (Analyzer.filterEmptyPosts)
           |
           | List[Post]   (posts válidos)
           v
  5. Detectar entidades en cada post
     (Analyzer.detectEntities)
           |
           | List[NamedEntity]   (todas las entidades de todos los posts)
           v
  6. Contar entidades
     (Analyzer.countEntities / countByType)
           |
           | Map[(String, String), Int]   y   Map[String, Int]
           v
  7. Ordenar y mostrar el ranking
     (Formatters.formatEntityStats / formatTypeStats)
           |
           | String (salida por consola)
           v
        [stdout]
```

Los pasos 2 y 3 están acoplados en el esqueleto (se descarga y parsea en la misma iteración), pero son pasos separados.

---

### b) Clasificación de cada paso según la abstracción de Spark

| Paso | Descripción | Abstracción de Spark | Justificación |
|------|-------------|----------------------|---------------|
| 1 | Leer suscripciones | — | El paso 1 es leer un archivo local del disco. No tiene sentido distribuirlo entre workers, es una sola lectura que tarda milisegundos |
| 2 y 3 | Descargar feed y parsear posts | `flatMap` | Cada `Subscription` produce cero o más `Post`. Las descargas son independientes entre sí y cada una puede producir una cantidad variable de resultados, tal lo que te permite flatMap. |
| 4 | Filtrar posts vacíos | `filter` (caso especial de `flatMap`) | Cada post se evalúa de forma independiente. `filter` es equivalente a un `flatMap` que devuelve el elemento o nada. |
| 5 | Detectar entidades por post | `flatMap` | Lo mismo que lo de los pasos 2 y 3: Cada `Post` produce cero o más `NamedEntity`. El procesamiento de cada post es independiente del resto. |
| 6a | Transformar entidad a par clave-valor | `map` | Cada `NamedEntity` se convierte en exactamente un par `((tipo, nombre), 1)`. |
| 6b | Contar entidades | `reduceByKey` | Hay que sumar los valores de la misma clave a través de todos los workers. El resultado depende de todos los elementos, no de uno solo |
| 7 | Ordenar y mostrar el ranking | —  | Este paso es solamenbte imprimir por pantalla. Para poder ordenar el ranking global necesitamos tener todos los datos juntos en un solo lugar, que es el driver. No podemos ordenar globalmente si los datos están repartidos.


---

### c) Pasos barrera vs. pasos independientes

**Pasos que son barreras de sincronización:**

- **`reduceByKey` (paso 6b):** Es la única barrera real del pipeline. Para calcular el conteo total de cada entidad, Spark necesita agregar los aportes de todos los workers. Ningún worker puede producir el conteo final hasta que todos hayan terminado de emitir.

- **La acción terminal (`collect` o `count`, paso 7):** Traer los resultados al driver es también una barrera, porque el driver no puede imprimir el ranking hasta que todos los workers hayan completado su trabajo.

**Pasos independientes:**

- Descarga y parseo de feeds (paso 2+3): cada worker procesa su propia URL sin necesitar datos de los demás
- Filtrado de posts (paso 4): cada post se evalúa de forma aislada.
- Detección de entidades (paso 5): el procesamiento de cada post es independiente.
- Transformación a pares clave-valor (paso 6a): cada entidad se convierte en un par sin estar mirando a las demás

---

### d) Restricciones sobre las funciones que se pasan a Spark


##### 1. Tienen que ser serializables
Cuando el driver le manda una función a un worker, la tiene que convertir en bytes para enviarla por la red. Eso se llama serialización. El problema es que si la función "captura" algún objeto del contexto (por ejemplo, una conexión a base de datos, o el propio SparkContext), ese objeto también tiene que serializarse. Si no puede, Spark tira una `NotSerializableException` en runtime. 
##### 2. No pueden usar estado compartido mutable
Los workers no comparten memoria entre sí ni con el driver. Si en el driver tenemos por ej. un var contador = 0 y lo capturamos en un flatMap, cada worker recibe su propia copia de ese contador al momento de la serialización. Las modificaciones que haga un worker no las ve nadie más. Por eso usar variables mutables para acumular resultados da valores incorrectos. Si necesitamos que los workers reporten métricas al driver, Spark tiene los Accumulator específicamente para eso.
##### 3. No deberían tener efectos secundarios
Spark puede re-ejecutar una tarea que falló, o incluso ejecutar la misma tarea en dos workers distintos. Si la función tiene efectos secundarios como escribir a un archivo o imprimir por consola, esas operaciones se pueden ejecutar más de una vez o en orden distinto al esperado. Por eso los efectos secundarios tienen que estar en las acciones terminales del pipeline (collect, saveAsTextFile, etc.), no en las transformaciones intermedias.