# Requerimientos analíticos I3

## Alcance y lenguaje

Población: exclusivamente filas válidas y deduplicadas de Silver del subconjunto
procesado por I1. Una interacción es una conexión observada al chat identificada
por la clave I1; no prueba visualización activa. Minutos estimados =
`10 × (time_stop − time_start)`; cero permitido. Solapamientos no se fusionan.
Sin fechas absolutas, geografías, seguidores, ingresos ni histórico completo.

Sea S la Silver del snapshot; S_c sus filas para streamer c; U_c los usuarios
distintos; n(u,c) el número de filas del usuario u con c. N = |S|.
Las tasas son fracciones [0,1], formateadas como porcentaje solo en BI.
Ningún número artificial se presenta como resultado del dataset real.

| ID / pregunta de negocio | Definición matemática | Campos y granularidad | Filtros y denominador | Criterio de aceptación |
| --- | --- | --- | --- | --- |
| R01 ¿Qué streamers concentran más conexiones observadas? | I_c = conteo(S_c); posición = row_number por I_c DESC, nombre ASC | streamer_username, interactions; una fila/streamer | Top N 1–1000, default 10; búsqueda exacta de streamer conserva posición global | suma I_c = N; orden determinista y misma posición con búsqueda |
| R02 ¿Cuántos usuarios se observaron con cada streamer? | U_c = cardinalidad de user_id en S_c | user_id, streamer_username; una fila/streamer | streamer exacto o Top N; todos los intervalos del snapshot | 1 ≤ U_c ≤ I_c; comparación exacta con Silver; nunca sumar U_c como usuarios globales |
| R03 ¿Qué proporción de usuarios registra múltiples conexiones con el mismo streamer? | C_c = conteo de u con n(u,c) ≥ 2; tasa = C_c / U_c | user_id, streamer_username y clave de interacción; una fila/streamer | streamer exacto; denominador TODOS sus usuarios distintos, incluso los de una sola fila | 0 ≤ C_c ≤ U_c; fixture 1/2=0,5; repetición en mismo stream cuenta; no es retención ni frecuencia de días |
| R04 ¿Cómo se distribuye la duración estimada de las conexiones? | H(c,b) = conteo de filas cuya duración pertenece a b; cuota = H / total de conexiones seleccionadas | estimated_minutes, streamer_username; streamer × bucket | streamer exacto o todos; buckets 0, (0,10], (10,30], (30,60], >60; sin filtro temporal | buckets disjuntos y exhaustivos; suma H=N y suma minutos=Silver; fronteras 0/10/30/60/70 probadas; vacío: tasa NULL |
| R05 ¿Cómo evolucionan los inicios de conexión por intervalo relativo? | A(c,t)=conteo de filas con time_start=t; agrupar t en floor(t/w)×w | time_start, streamer_username; streamer × paso nativo; BI agrupa w pasos | streamer; step_from/step_to inclusivos; w entero ≥1; default w=6; minutos relativos = paso×10 | sin filtro suma A=N; filtrado cuenta solo inicios en rango; no expandir start/stop; no llamar audiencia simultánea; huecos no rellenados |
| R06 ¿Qué fracción de interacciones acumulan los Top 10/100? | suma I_c de las primeras min(K,C) posiciones / N para K=10,100 | interactions, streamer_username; una fila global | sin filtro de streamer ni temporal; denominador completo del snapshot | 0 ≤ cuota10 ≤ cuota100 ≤1; empates por nombre; con menos de K streamers cuota=1 |
| R07 ¿Cuántas transmisiones distintas se observaron por streamer? | T_c = cardinalidad de stream_id en S_c | stream_id, streamer_username; una fila/streamer | streamer o Top N, snapshot completo | 1 ≤ T_c ≤ I_c; mismo stream con varias conexiones no suma dos; no sumar como streams globales |
| R08 ¿Son consistentes la población y los agregados publicados? | B=R+D+N; N=ΣI=ΣH=ΣA; M=Σminutos en cada agregado | conteos Bronze/rejects/Silver, versiones Delta, clave Silver, agregados; una fila/release | global, sin filtros de audiencia | Silver no vacía, clave única, contrato válido, Gold I1 equivalente por streamer, deltas cero y persistencia equivalente en ambos sentidos; marcador PASS presente |

R08: D se **infiere** por diferencia de conteos I1 y no certifica por sí solo
clasificación de cada línea Bronze; se confía en I1 validado para esa clasificación.
No se recalcula toda Silver desde Bronze en este incremento. PASS certifica el
alcance `I1_COUNTS_AND_I3_EXACT_AGGREGATES`, no calidad universal.

## Resolución y no aditividad

Gold I1 ya tiene I_c, U_c, T_c y M_c: R01/R02/R06/R07 se podrían consultar
directamente allí. I3 extiende esa pequeña dimensión agregada con C_c y tasa,
sin recalcular las métricas I1 para producirlas; la recomputación desde Silver se
usa exclusivamente como control de equivalencia antes de publicar.
No se guarda una tabla de 3M pares usuario/streamer: se agrega a ese nivel solo
para calcular recurrencia y se reduce inmediatamente. No se añade una dimensión
de usuario ni datos personales al dashboard.

Interacciones y minutos son aditivos por streamer, bucket e inicio dentro de su
familia. Usuarios, transmisiones y recurrencia no se suman entre streamers, ni
se recalculan al filtrar el eje temporal. El rango temporal afecta únicamente R05;
la selección de streamer afecta R01/R02/R03/R04/R05/R07. R06/R08 quedan globales.
Una selección sin coincidencias deja ranking vacío, distribución cero con tasas
NULL y actividad vacía; no convierte la ausencia en nueva observación.
