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