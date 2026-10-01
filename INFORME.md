# Informe

## Flujo general y protocolo interno

Todos los mensajes internos viajan serializados en JSON con la forma `[opcode, client_id, ...]`. El opcode (`FruitItem`, `EOF`, `Control`, `Fruits`, `PartialTop`, `FruitTop`) le dice a cada control cómo interpretar el resto de los campos.


El `message_handler` del gateway le asigna a cada cliente un `client_id` basado en un UUID. Todos los mensajes de ese cliente lo llevan, y cada control guarda su estado en diccionarios indexados por ese id. Cuando el gateway recibe un `FruitTop`, lo entrega solo al cliente cuyo id coincide.

## Coordinación entre instancias de Sum

Las instancias de Sum consumen de una única cola de trabajo, y RabbitMQ les reparte los mensajes de datos. Cada instancia acumula, por cliente y por fruta, usando la suma de `FruitItem`. El problema es que el EOF de un cliente lo recibe una sola instancia, la que lo saque de la cola, y en ese momento las demás pueden seguir procesando datos de ese cliente. Por eso la instancia que recibe el EOF no puede dar por terminado al cliente por su cuenta, y las demás ni siquiera se enteran del cierre.

Para resolverlo, el gateway cuenta cuántos mensajes de datos envió cada cliente e incluye ese total en el EOF, y cada Sum lleva la cuenta de cuántos mensajes procesó por cliente. Las instancias se comunican por un exchange de control, que cada una escucha con su propia cola desde un hilo aparte, en paralelo al hilo que consume datos (un lock protege el estado compartido entre ambos). La instancia que recibe el EOF publica en ese exchange su conteo junto con el total. Cada instancia, cuando se entera del cierre, publica su propio conteo, y si después procesa algún mensaje más de ese cliente, vuelve a publicarlo actualizado. Todas reciben todos los mensajes de control, así que cada una guarda el último conteo de cada Sum, y cuando la suma de los conteos iguala el total sabe que no queda ningún dato pendiente en ninguna instancia. En ese momento envía sus acumulados a los aggregators y libera el estado del cliente. Cada instancia detecta el cierre por su cuenta, sin un coordinador central.

Se descartó la alternativa de reencolar el EOF en la cola de entrada hasta que lo vieran todas las instancias. Esa solución no es determinista: nada garantiza qué instancia toma el EOF reencolado, así que puede volver muchas veces a una que ya lo vio, compitiendo con los datos de otros clientes. El tiempo hasta cerrar un cliente puede crecer mucho y de forma impredecible. El exchange de control, en cambio, entrega el aviso a todas las instancias de una sola vez.

## Coordinación entre Sum y Aggregation

Cuando un Sum detecta el cierre de un cliente, no le hace broadcast de todo a todos los aggregators. Lo que se hace en cambio es enviar las frutas a ciertos Aggregators según el valor de retorno de una función de hashing, de la cual se toma el módulo por la cantidad de Aggregators en el sistema. Dicho hash es determinista, así que todas las instancias de Sum mandan la misma fruta al mismo aggregator. De esa forma:

- Cada aggregator recibe solo una parte de las frutas, en vez de que todos procesen los mismos datos de manera redundante.
- Todo el acumulado de una fruta queda en un solo aggregator. Por eso su total es definitivo en ese aggregator, y el top de un aggregator es un top correcto para su subconjunto de frutas. El top global queda contenido en la unión de los tops parciales.

Después de enviar sus frutas, cada Sum envía `[EOF, client_id, sum_id]` a **todos** los aggregators, porque cualquiera de ellos podría estar esperando datos suyos, aunque no le haya mandado ninguna fruta. RabbitMQ mantiene el orden de los mensajes de un mismo publicador hacia una misma cola, así que el aggregator recibe todas las frutas de un Sum antes que su EOF.

Cada aggregator guarda, por cliente, el **conjunto** de ids de Sum de los que recibió EOF. Cuando el conjunto tiene `SUM_AMOUNT` elementos, calcula el top parcial (los `TOP_SIZE` mayores), lo envía al Join y libera el estado del cliente.

## Coordinación entre Aggregation y Join

El Join funciona como una **barrera** por cliente: espera a recibir el top parcial `[PartialTop, client_id, aggregation_id, top]` de los `AGGREGATION_AMOUNT` aggregators. Cuando la barrera se completa, combina los tops parciales, se queda con los `TOP_SIZE` mayores y envía `[FruitTop, client_id, top]` al gateway.

## Escalabilidad

### Respecto a los clientes

Todo el estado de cada control (acumulados, conteos y conjuntos de EOF) está indexado por `client_id`. Gracias a eso, varios clientes pueden atravesar el mismo pipeline al mismo tiempo sin que se mezclen sus datos, y cada uno termina de forma independiente: el cierre de un cliente no espera a que terminen los demás. Además, cuando un cliente termina, cada etapa libera su estado, así que la memoria depende de los clientes activos y no de todos los que pasaron por el sistema. Sumar clientes solo agrega trabajo proporcional a sus propios datos y unos pocos mensajes de coordinación; el mecanismo es el mismo para uno que para muchos.

### Respecto a grandes volúmenes de datos

Los datos crudos solo llegan a la primera etapa. Las instancias de Sum consumen de una única cola de trabajo, que RabbitMQ reparte entre ellas, y cada una guarda un único acumulado por fruta, así que su memoria crece con la cantidad de frutas distintas y no con la cantidad de registros. A partir de ahí el volumen se reduce: de Sum a Aggregation viaja un mensaje por fruta distinta, y de Aggregation a Join a lo sumo `TOP_SIZE` frutas por aggregator. Por eso, un cliente que manda millones de registros genera en las etapas siguientes el mismo tráfico que uno que manda pocos, siempre que tengan las mismas frutas distintas.

### Respecto a la cantidad de controles

Agregar instancias de Sum reparte la ingesta, que es la etapa con más volumen, y su coordinación casi no se encarece. El gateway publica el EOF después de todos los datos del cliente y la cola es FIFO, así que, cuando alguna instancia recibe el EOF, ya se repartieron todos los datos. Además, cada Sum procesa de a un mensaje (`prefetch_count=1`). Entonces, en el peor caso cada instancia publica un mensaje de control con su conteo y, si le quedaba uno en proceso, uno más. El cierre de un cliente cuesta del orden de dos mensajes de control por instancia, sin importar cuántos datos haya mandado, y termina en una cantidad acotada de pasos, no depende de que un EOF reencolado caiga en la instancia correcta.

Escalar Aggregation reparte las frutas por hash, así que cada instancia procesa una fracción menor, y el único costo extra es un top parcial más que tiene que esperar el Join. 

El Join es una sola instancia, pero recibe como máximo `AGGREGATION_AMOUNT × TOP_SIZE` frutas por cliente, así que no es un cuello de botella.

### Limitaciones

El reparto por hash supone que hay suficientes frutas distintas y que el hash las distribuye de forma pareja. Con pocas frutas, o con una fruta mucho más frecuente que el resto, algunos aggregators van a recibir más trabajo que otros.
