# Informe — Laboratorio 3: Procesamiento distribuido con Apache Spark

## Ejercicio 1 — Identificar las regiones paralelizables

### a) Diagrama de flujo del pipeline

El pipeline tiene los siguientes pasos, en orden. Cada flecha indica el tipo Scala del
dato que fluye entre un paso y el siguiente.

```
[Archivo JSON de suscripciones]
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

Los pasos 2 y 3 están acoplados en el esqueleto (se descarga y parsea en la misma
iteración), pero conceptualmente son separables. El diccionario de entidades se carga
una sola vez antes del paso 5 y se usa como dato de solo lectura.

---