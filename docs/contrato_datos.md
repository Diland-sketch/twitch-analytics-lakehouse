# Contrato de datos v1 — propuesto para I1

Fuentes primarias: [UCSD, sección Twitch](https://cseweb.ucsd.edu/~jmcauley/datasets.html#twitch)
y [directorio oficial](https://mcauleylab.ucsd.edu/public_datasets/gdrive/twitch/).
Consultadas el 17 de noviembre de 2026, fecha de trabajo del equipo en Bogotá.

La fuente describe usuarios conectados al chat observados cada diez minutos durante
43 días. Publica aproximadamente 100k/15.5M usuarios y 3M/124M interacciones para
subconjunto/histórico: estadísticas publicadas, no resultados locales.

Orden oficial; los nombres técnicos son elegidos por el proyecto:

| Posición | Significado | Nombre Silver | Tipo y regla I1 |
| --- | --- | --- | --- |
| 1 | Usuario anonimizado | `user_id` | BIGINT positivo |
| 2 | Identificador de stream | `stream_id` | BIGINT positivo |
| 3 | Nombre del streamer | `streamer_username` | STRING no vacío, trim, conserva mayúsculas |
| 4 | Inicio | `time_start` | BIGINT, 0 a 2147483647 |
| 5 | Fin | `time_stop` | BIGINT, 0 a 2147483647, mayor o igual al inicio |

La documentación nombra el subconjunto `100k.csv`, pero el directorio publica
`100k_a.csv`; I1 fija este último. Ejemplo oficial: `1,34347669376,grimnax,5415,5419`.

## Supuestos explícitos pendientes de inspección del archivo cargado

- UTF-8, una interacción por línea, cinco tokens separados por coma, sin cabecera,
  comillas ni comas escapadas. CRLF mediante lector de texto Spark. Este parser simple
  no acepta todo RFC CSV. Si aparecen otras características, revisar el contrato.
- IDs numéricos positivos y tiempos no negativos. Regex excluye signos y decimales;
  `try_cast` convierte overflow en NULL para cuarentena incluso con ANSI activo.
- El límite técnico de tiempos protege el cálculo; no es un máximo oficial. No se
  impone 6148: la fuente publica pasos, no especifica aquí sus extremos.
- Se admite inicio igual a fin y duración cero. La fuente no define inequívocamente
  intervalos inclusivos/exclusivos; `estimated_minutes = (time_stop - time_start) * 10`
  es una convención analítica pendiente de revisión; no suma diez minutos extra.
- No se convierten pasos a Unix timestamp o fechas: falta un origen documentado.
- Una interacción no prueba atención efectiva ni visualización de video. Los minutos
  son una estimación desde observaciones de chat, no una medida exacta de audiencia.
- Duplicado: igualdad de los cinco campos después de trim y tipado. No se fusionan
  intervalos superpuestos ni se deduplica solo por usuario/stream.

No se atribuyen columnas de juego, país, seguidores, texto del chat ni audiencia simultánea.
