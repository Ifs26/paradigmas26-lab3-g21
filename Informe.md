# Informe — Laboratorio 3: Procesamiento distribuido con Apache Spark

## Integrantes

* Luca Lorenzatti
* Santiago Issetta
* Fernando Lara
* Maximiliano Castelle

## Link de la consigna

[Consigna de laboratorio 3](https://docs.google.com/document/d/10dHff0_UhUOBF0A1o-m4XAN_t6xrtUWy_r60e5TZ7Gk/edit?tab=t.0)

# Ejercicios

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

## Ejercicio 2 — Paralelizar la descarga de feeds
En Main.scala se creó una SparkSession configurada en modo local:


scalaval spark = SparkSession.builder()
   .appName("RedditNER")
   .master("local[*]")
   .getOrCreate()
val sc = spark.sparkContext


### Implementación
*a)* Las suscripciones se leen del archivo indicado y se paralelizan con sc.parallelize(subscriptions), lo que crea un RDD de Subscription listo para distribuirse entre los workers.

*b)* Sobre ese RDD se aplica un flatMap que, por cada Subscription, descarga el feed y retorna una Seq[Post]. Las excepciones se manejan dentro de la función del flatMap para que un fallo en una suscripción no cancele el procesamiento de las demás (ver más abajo la pregunta conceptual sobre esto).

*c)* Para las métricas se usan acumuladores (sc.longAccumulator) que cuentan feeds exitosos y fallidos, posts descargados y fallidos, posts filtrados, y la suma de caracteres para calcular el promedio. Estos valores se imprimen al final con el mismo formato que el esqueleto.

*d)* Si después del filtrado por title y selftext no queda ningún post válido, el programa imprime Error: No valid posts downloaded after filtering y termina.

### Casos de error cubiertos

•⁠  ⁠Suscripción sin name o url → Warning: Skipping malformed subscription (missing 'name' or 'url' field)

•⁠  ⁠Archivo de suscripciones inexistente → Error: Could not load $filePath - file not found

•⁠  ⁠Archivo de suscripciones con JSON inválido → Error: Could not load $filePath - invalid JSON format

•⁠  ⁠Sin suscripciones válidas → Error: No valid subscriptions found (el programa termina)
Directorio de entidades inexistente → Error: entities directory '$entitiesDir' not found

•⁠  ⁠Archivo de entidad no encontrado → Warning: Could not load $entitiesDir/people.txt (y análogos)

•⁠  ⁠Posts de una suscripción no parseables → Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})

•⁠  ⁠URL sin respuesta → Warning: Failed to download from '${subscription.name}' (${subscription.url})

### Decisiones de diseño
Elegimos flatMap para la descarga porque es la abstracción natural cuando cada elemento de entrada (una suscripción) puede producir cero o muchos elementos de salida (los posts) y en cuanto al manejo de errores decidimos capturarlos dentro del flatMap en lugar de dejar que se propaguen.

Qué pasa si dejamos propagar la excepción? Si una excepción escapa de la función que se le pasa al flatMap, Spark la trata como un fallo de la tarea y la reintenta automáticamente. Si el error sigue, el job completo falla y no se produce ningún resultado, aunque el resto de las suscripciones fueran descargables. Capturando la excepción internamente, podemos devolver una Seq vacía para esa suscripción, incrementar el acumulador de fallos y seguir adelante con las demás.

Y al final el filtrado de posts vacíos se hace lo antes posible para no arrastrar trabajo innecesario al resto del pipeline.

## Ejercicio 3 — Paralelizar el cómputo de entidades nombradas (Luca)

El siguiente fragmento engloba la resolución general del ejercicio:

```scala
/*---- EJERCICIO 3 --------------------------------*/
    // FASE MAP
    val filteredPostsRDD = filteredRDD
      .flatMap{ post => //Serialización
        try{
          val entitiesFromPost = Analyzer.detectEntities(post.title + " " + post.selftext, dictionary)
          entitiesFromPost
        } 
        catch{
          case e: Exception =>
            println(s"Warning: Failed to count a post (${e})")
            List.empty[NamedEntity]
        }
      }

    val pairDataEntityOne = filteredPostsRDD
      .map(entity => ((entity.entityType, entity.text),1))

    //FASE REDUCE
    val reducedEntities = pairDataEntityOne
      .reduceByKey(_ + _)
      .sortBy(x => (-x._2, x._1._1)) // Ordenado por conteo descendente y por tipo
    
    val entitiesList = reducedEntities
      .collect //Trae los datos de los worker al driver
      .toList
    
    println(Formatters.formatTypeStatsDistributed(entitiesList))
    println()
    println(Formatters.formatEntityStats(entitiesList.toMap, cmdArgs.topK))
```

El pipeline implementa el patrón Map-Reduce para contar entidades nombradas en los posts de Reddit.

**Fase Map**

Las funciones de calculo distribuido que pertenecen a esta fase son: `flatMap` y `map`

`filteredRDD` es el RDD de posts ya filtrados del ejercicio anterior. El `flatMap` aplica `Analyzer.detectEntities` sobre el texto combinado de cada post (`title + selftext`), devolviendo una lista de `NamedEntity` por post. El resultado es un `RDD[NamedEntity]` con todas las entidades de todos los posts aplanadas en una sola colección distribuida. El `try/catch` aísla los fallos individuales para que un post problemático no cancele el job completo. `flatMap` serializa `dictionary`.

El `map` convierte cada `NamedEntity` en un par `((tipo, nombre), 1)`, preparando los datos para el conteo: cada aparición de una entidad emite un `1` asociado a su clave. `map` no captura nada externo, pero igual se serializa la función.

**Fase Reduce**

Las función de calculo distribuido que pertenece a esta fase es: `reduceByKey`

`reduceByKey(_ + _)` agrupa todos los pares con la misma clave `(tipo, nombre)` y suma sus valores. Esto implica un **shuffle**: Spark mueve los pares entre workers para garantizar que todos los `1`s de la misma entidad terminen en el mismo worker antes de sumar. El resultado es un par `((tipo, nombre), count)` por entidad distinta. `reduceByKey` serializa la función `_ + _`

El `sortBy(x => (-x._2, x._1._1))` ordena por conteo descendente y por tipo como desempate, todavía en los workers.

**Traída al driver**

Todo lo que se ha descrito ocurre entre los workers. El driver serializa las funciones y las envía, pero el cómputo y la comunicación entre workers (shuffle) ocurre sin que el driver intervenga. 

El `.collect()` es el único punto donde los datos viajan al driver. Materializa el RDD en el driver como un `Array`, disparando la ejecución de todo el pipeline lazy. Se convierte a `List` para trabajar con Scala puro.

**Formateo**

`formatTypeStatsDistributed` agrupa `entitiesList` por tipo en el driver para obtener el total por categoría. `formatEntityStats` toma los mismos datos como `Map` y muestra las entidades más frecuentes hasta el límite `topK`.

### Preguntas requeridas por consigna

#### `reduceByKey` es una barrera de sincronización. ¿Qué ocurre en el cluster en ese punto? ¿Por qué es inevitable para este problema?

Cuando se ejecuta `reduceByKey`, cada worker tiene conteos parciales de las entidades que aparecieron en sus posts. Para obtener el conteo global de cada entidad, Spark deberá juntar todos los pares con la misma clave en un mismo worker. Esto se llama **shuffle**. Cada worker serializa sus pares, los envía por la red a los workers responsables de cada clave, y ningún worker puede empezar la fase de reduce hasta que todos hayan terminado de enviar sus datos. Esa espera es la barrera de sincronización.

Es inevitable porque el conteo es un problema inherentemente global: no podés saber cuántas veces apareció "Python" en total sin juntar los conteos parciales de todos los workers. No existe forma de calcular una suma distribuida sin en algún momento comunicar los valores parciales.

#### ¿Qué restricciones debe cumplir la función que se le pasa a `reduceByKey`? Piensen en conmutatividad y asociatividad.

Debe ser **conmutativa** y **asociativa**.

- **Asociativa**: `f(f(a, b), c) == f(a, f(b, c))`. Spark puede combinar los valores en cualquier orden y en cualquier agrupación parcial, dependiendo de cómo distribuya las tareas. Si la función no es asociativa, el resultado cambia según el orden de combinación.
- **Conmutativa**: `f(a, b) == f(b, a)`. Spark no garantiza el orden en que llegan los valores al reduce. Si la función no es conmutativa, el resultado depende del orden de llegada y no es determinista.

La suma `_ + _` cumple ambas. La resta o la división no las cumplen.

#### ¿Dónde se hace la lectura del diccionario de entidades? ¿En el driver o los workers?

En el **driver**. La línea `Dictionary.loadAll(cmdArgs.entitiesDir)` corre en el `main`, que es código del driver. Cuando el closure del `flatMap` captura `dictionary`, Spark serializa esa lista y la envía como copia a cada worker. Cada worker trabaja con su propia copia en memoria y no vuelven a leer el archivo.

El diccionario no podría pasarse por referencia. Esto se debe a que los workers son procesos separados, no comparten heap con el driver. Una referencia en Java/Scala es solo una dirección de memoria, y esa dirección solo tiene sentido dentro del proceso donde fue creada. En otro proceso esa misma dirección apunta a basura o a nada.

Si la lectura estuviera dentro del `flatMap`, cada worker leería el archivo por su cuenta desde disco en cada tarea. Leerlo una vez en el driver y distribuirlo serializado es más eficiente, y para diccionarios grandes la optimización correcta sería usar una **broadcast variable** para enviar una sola copia por worker en lugar de una copia por tarea. el broadcast tiene la siguiente forma:

```scala
val dictBroadcast = sc.broadcast(dictionary)

filteredRDD.flatMap { post =>
  Analyzer.detectEntities(post.title + " " + post.selftext, dictBroadcast.value)
}
```

La diferencia con mandar el closure normal es:

- **Sin broadcast**: `dictionary` se serializa y se copia dentro de cada closure, una vez por tarea. Si tenés 100 tareas, `dictionary` viaja 100 veces.
- **Con broadcast**: `dictionary` se envía una sola vez a cada worker, se guarda en memoria ahí, y todas las tareas de ese worker la comparten. Si tenés 100 tareas en 4 workers, `dictionary` viaja 4 veces.

Para un diccionario chico no importa, por lo que no la implementamos. Sin embargo para datos grandes es una optimización importante y debe ser tenida en consideración.

## Ejercicio 4 - Monitoreo del éxito de las tareas (Santi)


### ¿Por qué los Accumulators solo deben usarse para métricas y no para tomar decisiones lógicas dentro de las etapas distribuidas del pipeline? ¿En qué situación un Accumulator puede dar un valor incorrecto?

Los Accumulators solo pueden ser incrementados por los workers y leídos por el driver, pero su valor no está garantizado durante la ejecución del pipeline. Si un task falla y se reintenta, el acumulador puede incrementarse más de una vez para el mismo dato. Por eso usarlos para tomar decisiones lógicas dentro de un worker puede dar resultados incorrectos o no reproducibles.


### ¿En qué momento del pipeline está disponible el valor de un Accumulator para ser leído por el driver?

El valor de un acumulador está disponible para ser leído por el drive recién después de que una acción terminal completa (count(), collect(), entre otros). Mientras el pipeline está en ejecución, el driver no puede leer un valor.


### Comparen el tiempo que tarda cada etapa del pipeline que midieron en la versión no paralelizada y la versión con Spark. ¿Qué conclusiones pueden sacar? Para la cantidad de datos que estamos trabajando, ¿se aprecia la diferencia? Justifique por qué.

Para aclarar, Spark fue diseñado para conjunto de datos inmensos, donde podrían entrar tranquilamente un millón de posts. En nuestro laboratorio, se trabaja con unos pocos cientos de posts, por lo que la carga que realiza Spark es mayor a una búsqueda secuencial en estos casos. Por ende, el tiempo de carga en este laboratorio resulta mayor que si hubiéramos utilizado la versión no paralelizada, con una diferencia aproximada de 6,4 segundos (6,7 Spark y 300ms como mucho en la versión no paralelizada). En conclusión, no es recomendable utilizar Spark con conjunto de datos pequeños, en este caso posts de reddit, sino no sería eficiente en el tiempo de carga en comparación con la otra versión.

## Ejercicio 5 - Acceso a datos y estadísticas del resultado (santi)

### ¿Qué ocurriría si no llamaran a cache()? ¿Cuántas veces se ejecutaría la descarga de feeds?

Sin cache(), cada acción terminal sobre el RDD recomputaría todo el pipeline desde el principio, incluyendo las descargas HTTP. En nuestro caso postRDD se usa en count() y luego en filter(), por lo que la descarga se ejecutaría dos veces.


### ¿Por qué es incorrecto llamar a collect() entre los pasos a y b del ejercicio 3 y luego continuar el pipeline? ¿Qué consecuencia tiene sobre la distribución del trabajo?

Porque collect() trae todos los datos al driver, y el paso b (el map) se ejecutaría localmente en el driver en lugar de distribuirse entre los workers. Esto rompe el modelo de Spark: en lugar de que cada worker procese su partición de entidades en paralelo, el driver tendría que procesar el total de entidades solo y de forma secuencial, perdiendo toda la ventaja de la distribución.


### cache() es también lazy. ¿En qué momento se almacena realmente el RDD en memoria?

Cache almacena realmente el RDD en memoria cuando se ejecuta la primera acción terminal (ejemplo: postRDD.count()), Spark ejecuta todo el pipeline, devuelve el resultado y se guarda el RDD en memoria. Desde la segunda llamada, directamente se busca en memoria.



# Decisiones de diseño

#### 1: `countEntities` y `countByType` quedan obsoletas para este pipeline. 

Las reemplaza el pipeline de Spark: estas dos funciones trabajan sobre una List completa en memoria. Aqui, el conteo no lo hace un groupBy en el driver sino que lo hace Spark distribuido con `reduceByKey`. El resultado de reduceByKey ya te da exactamente lo que hacía `countEntities`: un par `((tipo, nombre), count)` por cada entidad distinta.

#### 2: Driver se encarga del conteo por tipo, no se hace de forma distribuida

No vale la pena lanzar otro job de Spark para esto. El entitiesList ya tiene los datos agregados por reduceByKey — son pocos elementos (uno por entidad distinta), no miles de posts. Mandar eso a los workers, hacer el shuffle, y traerlo de vuelta costaría más de lo que ahorra.
La regla general es: si los datos ya están en el driver y son pequeños, Scala puro. Si los datos están distribuidos y son grandes, Spark.

Se podría haber hecho mapeando con workers segun el tipo de la entidad:

```scala
val pairDataEntityOne = filteredPostsRDD
      .map(entity => (entity.entityType,1))
      .reduceByKey(_ + _)
      .sortBy(x => (-x._2, x._1._1)) // Ordenado por conteo descendente y por tipo
      .collect 
``` 
#### 3: Se añadió trait `Serializable` a `NamedEntity`

Sin este agregado, al intentar serializar con Spark cualquier función que tomase objetos del tipo NamedEntity arrojaba una excepción del tipo

``` bash
Caused by: java.io.NotSerializableException: Person
```

La razón por la que Spark necesita es que, cuando el closure del flatMap captura dictionary (una List[NamedEntity]), Spark serializa esa lista entera para enviarla a los workers. Java serializa los objetos de adentro hacia afuera — primero intenta serializar cada NamedEntity de la lista, y si la clase no implementa Serializable, lanza NotSerializableException en ese punto.

#### 4: Uso de `try/catch` dentro de flatMap

Se implementa el manejo de excepcines para aislar fallos por post y evitar que un error individual no cancele el job completo.

# Makefile
El makefile planteado por el grupo fue el siguiente:

```makefile
# Variables
JAVA_HOME := /usr/lib/jvm/java-17-openjdk-amd64
PATH      := $(JAVA_HOME)/bin:$(PATH)
SBT_OPTS  := --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
SBT       := sbt

export JAVA_HOME
export PATH
export SBT_OPTS

.DEFAULT_GOAL := run

# --- Comandos previos ---

check-java:
	@echo "Verificando Java 17..."
	@java -version 2>&1 | grep -q "17" || (echo "ERROR: Java 17 no encontrado en $(JAVA_HOME)" && exit 1)

setup: check-java
	@echo "Setup listo."

compile: check-java
	$(SBT) compile

# --- Target principal ---

run: setup compile
	$(SBT) run

mock: setup compile
	$(SBT) "run --subscription-file ./data/local_subscriptions.json"

tests: setup
	bash tests.sh

.PHONY: check-java setup compile run
```
**Variables de entorno**

Define `JAVA_HOME` apuntando a Java 17, agrega el bin de Java al `PATH`, y configura `SBT_OPTS` con un flag para que Spark pueda acceder a módulos internos de la JVM. Las tres se exportan para que estén disponibles en los subprocesos que lanza Make.

**Targets**
- `check-java`: verifica que Java 17 esté disponible ejecutando `java -version` y buscando "17" en la salida. Si no lo encuentra, falla con un mensaje de error.
- `setup`: depende de `check-java`, imprime "Setup listo." como confirmación.
- `compile`: depende de `check-java` y ejecuta `sbt compile`.
- `run`: depende de `setup` y `compile`, ejecuta `sbt run` con los defaults de `CommandLineArgs`.
- `mock`: igual que `run` pero pasa `--subscription-file ./data/local_subscriptions.json` como argumento fijo para apuntar al archivo de suscripciones locales en lugar de usar Reddit real.
- `tests`: depende de `setup` y ejecuta `bash tests.sh` para correr la suite de tests de integración.

El target por defecto es `run`.

# Cambios en build.sbt
Se agregaron:

**flag `"--add-opens=java.base/java.nio=ALL-UNNAMED"`**

Se introdujo siguiendo el modelo de build.sbt de Hello_world. Spark 4.1.1 usa reflection para acceder a paquetes internos de la JVM (java.nio, sun.security.action) que Java 17 bloquea por defecto con el sistema de módulos (JPMS). Se agregaron flags --add-opens para abrir explícitamente esos paquetes, permitiendo que Spark funcione sin modificar su código fuente.

**flag `"--add-opens=java.base/sun.security.action=ALL-UNNAMED"`**

Spark fue escrito antes de Java 9. Su SerializationDebugger usa reflection para inspeccionar objetos al momento de serializarlos — mira los campos, los tipos, etc. Para hacer eso con clases internas de la JVM, usa MethodHandles y reflection que en Java 8 funcionaban sin restricciones. En Java 17, esas mismas llamadas fallan con IllegalAccessException porque el módulo java.base no le da permiso a código externo de acceder a sun.security.action.

El flag le dice a la JVM explícitamente: "abrí el paquete sun.security.action del módulo java.base para que cualquier código en módulos sin nombre pueda acceder a él con reflection". ALL-UNNAMED significa "cualquier código que no esté en un módulo nombrado" — que es exactamente donde cae Spark.

**Dependencias `"org.apache.spark" %% "spark-core" % "4.1.1"` y `"org.apache.spark" %% "spark-sql" % "4.1.1"`**

Agregan Spark como dependencia. `spark-core` provee el `SparkContext` y los RDDs. `spark-sql` provee `SparkSession`, que es el punto de entrada moderno para arrancar Spark.
