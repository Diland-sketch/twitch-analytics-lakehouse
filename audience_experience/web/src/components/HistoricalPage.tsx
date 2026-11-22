import {useEffect,useState} from 'react'
import {Card} from './ui/card'
type Export = {
  release:string; source_namespace:string; exported_at:string;
  metrics:{silver_rows:number;gold_streamers:number;estimated_minutes:number;rejected_rows:number;top10_share:number;top100_share:number};
  ranking:{streamer_username:string;interactions:number;unique_users:number;recurring_users:number;recurrence_rate:number;estimated_minutes:number}[];
  duration:{duration_bucket:number;interactions:number}[];
}
const f=new Intl.NumberFormat('es-CO')
const percent=(n:number)=>new Intl.NumberFormat('es-CO',{style:'percent',maximumFractionDigits:1}).format(n)
export function HistoricalPage() {
  const [data,setData]=useState<Export|null>(null)
  const [state,setState]=useState<'loading'|'missing'|'invalid'|'ready'>('loading')
  useEffect(()=>{
    const abort=new AbortController()
    fetch('/api/v1/historical',{cache:'no-store',signal:abort.signal})
      .then(async r=>{if(r.status===503){setState('missing');return}
        if(!r.ok){setState('invalid');return}
        setData(await r.json() as Export);setState('ready')})
      .catch(()=>{if(!abort.signal.aborted)setState('invalid')})
    return ()=>abort.abort()
  },[])
  if(state!=='ready'||!data)return <Card className="chart-panel"><h2 className="panel-title">Análisis histórico de Databricks</h2><p className="panel-subtitle mt-4" role="status">{state==='loading'?'Cargando resultados Gold I3…':state==='missing'?'Todavía falta importar la exportación I3. Ejecuta el notebook 10 y copia su JSON a audience_experience/data/.':'No se pudo leer o validar la exportación I3. Revisa /api/v1/historical.'}</p><p className="panel-subtitle mt-3">Mientras tanto puedes seguir usando el laboratorio de Kafka y Spark.</p></Card>
  const q=data.metrics
  return <div className="space-y-5"><div className="historical-note"><strong>Snapshot histórico · {data.release}</strong><span>Exportado {new Date(data.exported_at).toLocaleString('es-CO')} · sin conexión en vivo a Databricks</span></div>
    <div className="kpi-grid">
      {([{label:'Conexiones observadas',value:q.silver_rows},{label:'Streamers',value:q.gold_streamers},{label:'Minutos estimados',value:q.estimated_minutes},{label:'Rechazados',value:q.rejected_rows}]).map(k=><Card key={k.label} className="kpi"><div className="kpi-label">{k.label}</div><div className="kpi-value">{f.format(k.value)}</div><div className="kpi-foot">Datos Gold I3 · {data.release}</div></Card>)}
    </div>
    <Card className="chart-panel"><h2 className="panel-title">Top 20 streamers del histórico</h2><p className="panel-subtitle">Ranking por conexiones, no espectadores únicos globales.</p><div className="overflow-x-auto"><table className="data-table"><thead><tr><th>Streamer</th><th>Conexiones</th><th>Usuarios</th><th>Recurrentes</th><th>Recurrencia</th><th>Minutos estimados</th></tr></thead><tbody>{data.ranking.map(s=><tr key={s.streamer_username}><td>{s.streamer_username}</td><td>{f.format(s.interactions)}</td><td>{f.format(s.unique_users)}</td><td>{f.format(s.recurring_users)}</td><td>{percent(s.recurrence_rate)}</td><td>{f.format(s.estimated_minutes)}</td></tr>)}</tbody></table></div></Card>
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-5"><Card className="chart-panel"><h2 className="panel-title">Duración estimada</h2><p className="panel-subtitle">Distribución global de conexiones.</p><div className="overflow-x-auto"><table className="data-table"><thead><tr><th>Intervalo</th><th>Conexiones</th><th>Porcentaje</th></tr></thead><tbody>{data.duration.map(b=><tr key={b.duration_bucket}><td>{['0 min','(0,10] min','(10,30] min','(30,60] min','>60 min'][b.duration_bucket]}</td><td>{f.format(b.interactions)}</td><td>{percent(b.interactions/q.silver_rows)}</td></tr>)}</tbody></table></div></Card></div></Card></div>
    <Card className="chart-panel"><h2 className="panel-title">¿Qué significan estos resultados?</h2><p className="panel-subtitle mt-3">Las interacciones son conexiones observadas al chat, no espectadores activos. Los minutos son estimaciones por pasos de diez minutos. La recurrencia describe usuarios con dos o más registros del mismo streamer y no demuestra retención entre transmisiones.</p><p className="panel-subtitle mt-3">Fuente: {data.source_namespace}; versión de publicación: {data.release}. Estos datos no se suman con los eventos del laboratorio.</p></Card></div>
}