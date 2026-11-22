import {useMemo, useState} from 'react'
import {Activity, Radio, LayoutDashboard, Users, GitBranch, ExternalLink, CircleHelp, ChevronRight, Database, ShieldCheck, Check, Zap, RefreshCw, Clock3, Layers, AlertTriangle, FileText, MonitorPlay, Copy, CheckCheck, Signal, CircleDot, Menu, X} from 'lucide-react'
import {Button} from './components/ui/button'
import {Card} from './components/ui/card'
import {Badge} from './components/ui/badge'
import {Select, SelectContent, SelectItem, SelectTrigger, SelectValue} from './components/ui/select'
import {Tabs, TabsList, TabsTrigger} from './components/ui/tabs'
import {LiveChart} from './components/LiveChart'
import {HistoricalPage} from './components/HistoricalPage'
import {useLiveSnapshot} from './hooks/useLiveSnapshot'
import type {Snapshot} from './types'
import type {EChartsCoreOption} from 'echarts/core'

const number = new Intl.NumberFormat('es-CO')
const format = (n: number | undefined) => n === undefined ? '—' : number.format(n)
const time = (value?: string) => value ? new Date(value).toLocaleTimeString('es-CO', {hour:'2-digit',minute:'2-digit',second:'2-digit'}) : 'Sin actualización'
const stamp = (value?: string) => value ? new Date(value).toLocaleString('es-CO') : 'Sin datos'
type View = 'overview' | 'audience' | 'pipeline' | 'guide' | 'historical'
const navigation: {id:View; label:string; icon:typeof Activity}[] = [{id:'historical',label:'Análisis de audiencia',icon:Database},{id:'overview',label:'Laboratorio de datos',icon:Activity}]
const labViews: {id:View; label:string; icon:typeof Activity}[] = [{id:'overview',label:'Resumen',icon:LayoutDashboard},{id:'audience',label:'Audiencia del replay',icon:Users},{id:'pipeline',label:'Procesamiento',icon:GitBranch},{id:'guide',label:'Guía de demostración',icon:CircleHelp}]

function TwitchMark() {return <svg width="29" height="31" viewBox="0 0 29 31" fill="none" aria-hidden="true"><path d="M5 2H27V21L19 29H12V23H5V2Z" stroke="currentColor" strokeWidth="2.8"/><path d="M12 8V15M20 8V15" stroke="currentColor" strokeWidth="2.8"/></svg>}

function Kpi({label,value,detail,icon:Icon,featured=false,accent='',testId}: {label:string;value:number|undefined;detail:string;icon:typeof Activity;featured?:boolean;accent?:string;testId?:string}) {
  return <Card className={`kpi ${featured?'featured':''}`}><div className="kpi-label"><Icon size={15}/>{label}</div><div className={`kpi-value ${accent}`} data-testid={testId}>{format(value)}</div><div className="kpi-foot">{detail}</div></Card>
}

function ActivityPanel({data}: {data:Snapshot|null}) {
  const [mode,setMode] = useState('cumulative')
  const points = data?.activity || []
  const option = useMemo<EChartsCoreOption>(() => ({
    grid:{top:25,left:43,right:12,bottom:33}, tooltip:{trigger:'axis',backgroundColor:'#252230',borderColor:'#493d5e',textStyle:{color:'#e3dcef'},axisPointer:{type:'line',lineStyle:{color:'#78608f'}}, valueFormatter:(value:unknown)=>`${format(Number(value))} eventos`},
    xAxis:{type:'category',data:points.map(p=>time(p.completed_at)),boundaryGap:mode!=='cumulative',axisLine:{show:false},axisTick:{show:false},axisLabel:{color:'#6f7184',fontSize:10,margin:15,hideOverlap:true}},
    yAxis:{type:'value',minInterval:1,splitNumber:4,axisLabel:{color:'#77798e',fontSize:10},splitLine:{lineStyle:{color:'#292a35',type:'dashed'}}},
    series:[{id:'activity',name:mode==='cumulative'?'Eventos acumulados':'Eventos por micro-lote',type:mode==='cumulative'?'line':'bar',data:points.map(p=>mode==='cumulative'?p.cumulative_events:p.events),smooth:false,showSymbol:points.length===1,symbolSize:6,barMaxWidth:22,lineStyle:{color:'#af82ee',width:2.5},itemStyle:{color:'#ac7fe9',borderRadius:[3,3,0,0]},areaStyle: mode==='cumulative'?{color:{type:'linear',x:0,y:0,x2:0,y2:1,colorStops:[{offset:0,color:'#a376e83d'},{offset:1,color:'#a376e800'}]}}:undefined}]
  }), [points,mode])
  return <Card className="chart-panel"><div className="section-top"><div><h2 className="panel-title">La actividad, en perspectiva</h2><p className="panel-subtitle">Cada punto es un micro-lote confirmado por Spark.</p></div><Activity size={17} className="text-[#70627e]"/></div><div className="section-top mt-5"><div className="chart-total"><strong>{format(data?.metrics.total)}</strong><span>eventos procesados</span></div><Tabs value={mode} onValueChange={setMode}><TabsList className="h-8 bg-[#22232c]"><TabsTrigger className="text-[11px] px-2" value="cumulative">Acumulado</TabsTrigger><TabsTrigger className="text-[11px] px-2" value="batch">Por lote</TabsTrigger></TabsList></Tabs></div>
    {points.length?<LiveChart option={option} label="Evolución real de eventos confirmados por Spark"/>:<div className="empty-chart"><Activity size={32}/><strong>Listo para recibir actividad</strong><span>La gráfica aparecerá al completarse el primer micro-lote.</span></div>}
    <div className="chart-footer"><Clock3 size={12}/>Hora local del procesamiento · no fecha histórica de Twitch</div></Card>
}

function RankingPanel({data,expanded=false,onExpand}: {data:Snapshot|null;expanded?:boolean;onExpand:()=>void}) {
  const rows = (data?.streamers || []).slice(0,expanded?20:6)
  const option = useMemo<EChartsCoreOption>(() => ({grid:{left:0,right:33,top:18,bottom:10,containLabel:true},tooltip:{trigger:'item',backgroundColor:'#252230',borderColor:'#493d5e',textStyle:{color:'#e3dcef'},formatter:'{b}: {c} conexiones'},xAxis:{type:'value',show:false},yAxis:{type:'category',inverse:true,data:rows.map(r=>r.streamer),axisLine:{show:false},axisTick:{show:false},axisLabel:{color:'#c8c5d4',fontSize:12,width:110,overflow:'truncate'}},series:[{id:'streamers',type:'bar',realtimeSort:true,barWidth:9,showBackground:true,backgroundStyle:{color:'#25242e',borderRadius:4},data:rows.map((r,i)=>({name:r.streamer,value:r.interactions,itemStyle:{color:i===0?'#b48bef':'#7856aa',borderRadius:4}})),label:{show:true,position:'right',color:'#e7e1f2',fontSize:12},animationDurationUpdate:450}]}), [rows])
  const first=rows[0],share=data?.metrics.valid&&first?(first.interactions/data.metrics.valid*100).toFixed(1):null
  return <Card className="chart-panel"><div className="section-top"><div><h2 className="panel-title">Streamers que conectan</h2><p className="panel-subtitle">Ranking por conexiones válidas observadas.</p></div><Users size={17} className="text-[#70627e]"/></div>
    {rows.length?<LiveChart option={option} label="Ranking real de streamers por conexiones válidas" height={expanded?Math.max(280,rows.length*34):280}/>:<div className="empty-chart"><Users size={32}/><strong>Tu audiencia empieza aquí</strong><span>Aún no hay streamers en esta ejecución.</span></div>}
    {first&&<p className="text-[12px] text-[#9994a8] leading-5 mb-3"><span className="text-[#d1b6f0]">{first.streamer}</span> concentra el {share}% de las conexiones válidas de la selección.</p>}
    <div className="rank-footer"><span>Top {expanded?'20':'6'} · selección actual</span><Button variant="ghost" size="sm" onClick={onExpand} className="h-7 text-xs text-[#baa1d7]">{expanded?'Volver al resumen':'Explorar audiencia'}</Button></div></Card>
}

function Pipeline({data,expanded=false}: {data:Snapshot|null;expanded?:boolean}) {
  const batch=data?.last_batch
  const stages=[{icon:FileText,title:'Origen histórico',detail:'100k_a.csv · reproducción'},{icon:Radio,title:'Transporte Kafka',detail:'Eventos con identidad estable'},{icon:Zap,title:'Procesamiento Spark',detail:data?.consumer?.stale?'Sin señal reciente':data?.consumer?.state==='RUNNING'?'Consumidor activo':'Esperando consumidor'},{icon:Database,title:'Resultados persistidos',detail:'SQLite · lectura sin escrituras'}]
  return <Card className="pipeline-panel"><div className="section-top"><div><h2 className="panel-title">De un evento a una decisión</h2><p className="panel-subtitle">Trazabilidad del procesamiento · estado global</p></div><Badge variant="outline" className="text-[10px] uppercase tracking-wider text-[#aaa5b8] border-[#3a3546]">Datos verificables</Badge></div>
    <div className="pipeline-stages">{stages.map(stage=><div className="pipeline-stage" key={stage.title}><div className="stage-icon"><stage.icon size={16}/></div><strong>{stage.title}</strong><p>{stage.detail}</p></div>)}</div>
    <div className="batch-strip"><span><Layers size={12} className="inline mr-2"/>Último lote <strong>{batch?`#${batch.batch_id}`:'—'}</strong></span><span>Nuevos eventos <strong>{batch?format(batch.new_events):'—'}</strong></span><span>Offsets registrados <strong>{format(data?.transport_total)}</strong></span><span className="ml-auto">Confirmado <strong>{time(batch?.completed_at)}</strong></span></div>
    {expanded&&<><div className="mt-6 overflow-x-auto"><table className="data-table"><thead><tr><th>Topic Kafka</th><th>Partición</th><th>Primer offset</th><th>Último offset</th><th>Registros</th></tr></thead><tbody>{batch?.offsets.map(o=><tr key={`${o.topic}-${o.partition}`}><td className="mono">{o.topic}</td><td>{o.partition}</td><td>{o.first_offset}</td><td>{o.last_offset}</td><td>{o.records}</td></tr>)}</tbody></table></div><p className="text-xs text-[#888598] mt-5 break-all mono">pipeline_id: {batch?.pipeline_id||'Sin micro-lote confirmado'}</p><p className="text-xs text-[#9390a5] mt-3">Un reenvío puede aumentar offsets sin incrementar los eventos únicos. El último lote y el consumidor son globales, incluso con una ejecución seleccionada.</p></>}
  </Card>
}

export default function App() {
  const initial = new URLSearchParams(location.search).get('run_id') || ''
  const [runId,setRunId]=useState(initial),[view,setView]=useState<View>(initial ? 'overview' : 'historical'),[copied,setCopied]=useState(false)
  const {snapshot:data,link,error,reconnect}=useLiveSnapshot(runId, view !== 'historical')
  const selectedRun=data?.runs.find(r=>r.run_id===runId)
  const active=data?.consumer?.state==='RUNNING'&&!data.consumer.stale
  const connected=link==='websocket'||link==='polling'
  const validShare=data?.metrics.total?`${(data.metrics.valid/data.metrics.total*100).toFixed(1)}% de los eventos de la selección`:'Sin eventos para evaluar'
  const changeRun=(value:string)=>{const next=value==='all'?'':value;setRunId(next);const url=new URL(location.href);next?url.searchParams.set('run_id',next):url.searchParams.delete('run_id');history.replaceState(null,'',url)}
  const copyRun=async()=>{if(runId){await navigator.clipboard.writeText(runId);setCopied(true);setTimeout(()=>setCopied(false),1600)}}
  return <div className="app-shell dark">
    <aside className="sidebar">
      <div className="brand"><TwitchMark/><div>audience<span className="text-[#a67de2]">.</span><small>Twitch Intelligence</small></div></div>
      <div className="nav-label">Workspace</div>
      <nav aria-label="Secciones principales">{navigation.map(n=><button key={n.id} onClick={()=>setView(n.id)} className={`nav-item ${(n.id==='historical'?view==='historical':view!=='historical')?'active':''}`} aria-current={(n.id==='historical'?view==='historical':view!=='historical')?'page':undefined}><n.icon size={17}/>{n.label}</button>)}</nav>
      <div className="sidebar-bottom"><div className="flex items-center gap-2 px-3 text-[12px] text-[#c6c4d4]"><ShieldCheck size={16} className="text-[#a487cf]"/>Procedencia verificable</div><p>Analítica histórica Gold I3.<br/>Laboratorio Kafka y Spark.</p></div>
    </aside>
    <div className="min-w-0"><header className="topbar">
      <span className="breadcrumb">Workspace <ChevronRight size={12} className="inline mx-3"/><strong>{view==='historical'?'Análisis de audiencia':'Laboratorio de datos'}</strong></span><span className="mobile-brand"><TwitchMark/>audience.</span>
      {view!=='historical'&&<div className="topbar-actions"><time title={stamp(data?.last_batch?.completed_at)}>Actualizado {time(data?.last_batch?.completed_at)}</time><Badge variant="outline" className={`text-[11px] py-1 px-2.5 border-[#30433d] ${connected?'text-[#94d6ba]':'text-[#d8b375]'}`}><span className={`connection-dot ${!connected?'warning':''}`}/>{link==='websocket'?'WebSocket activo':link==='polling'?'Polling de respaldo':link==='loading'?'Conectando':link==='reconnecting'?'Reconectando':'Sin conexión'}</Badge><Button variant="ghost" size="icon" title="Reconectar a resultados" aria-label="Reconectar a resultados" className="size-8 text-[#8f89a1]" onClick={reconnect}><RefreshCw size={14}/></Button></div>}
    </header>
    <main className="workspace">
    <div className="page-heading"><div><p className="eyebrow">{view==='historical'?'Twitch audience intelligence':'Laboratorio de datos · Kafka + Spark'}</p>
    <h1>{view==='historical'?'Análisis de audiencia':view==='audience'?'Audiencia del replay':view==='pipeline'?'Procesamiento y calidad':view==='guide'?'Guía de demostración':'Monitoreo del procesamiento'}</h1>
    <p>{view==='historical'?'Indicadores históricos Gold I3 con explicaciones comprensibles.':view==='audience'?'Explora los streamers de la reproducción seleccionada.':view==='pipeline'?'Consulta micro-lotes, offsets y estado de Spark.':view==='guide'?'Demostración reproducible y verificación de los datos.':'Sigue el recorrido de eventos históricos por Kafka y Spark.'}</p></div>
    {view!=='historical'&&<div className="run-control"><label className="block text-[11px] uppercase tracking-[1.3px] text-[#7f8095] mb-2" id="run-label">Ejecución del laboratorio</label><Select value={runId||'all'} onValueChange={changeRun}><SelectTrigger aria-labelledby="run-label" className="w-[240px] h-10 bg-[#191a22] text-xs border-[#36343f]"><SelectValue/></SelectTrigger><SelectContent><SelectItem value="all">Todas las ejecuciones</SelectItem>{runId&&!selectedRun&&<SelectItem value={runId}>Preparada · {runId.slice(0,8)}</SelectItem>}{data?.runs.map(r=><SelectItem key={r.run_id} value={r.run_id}>Run · {r.run_id.slice(0,8)} · {r.producer_status==='COMPLETED'?'Completada':r.producer_status==='PUBLISHING'?'En reproducción':r.producer_status}</SelectItem>)}</SelectContent></Select></div>}</div>
    <div className="mobile-nav" aria-label="Secciones principales">{navigation.map(n=><Button key={n.id} variant={(n.id==='historical'?view==='historical':view!=='historical')?'secondary':'ghost'} size="sm" onClick={()=>setView(n.id)}><n.icon size={14}/>{n.label}</Button>)}</div>
    {view!=='historical'&&<div className="lab-tabs" role="navigation" aria-label="Vistas del laboratorio">{labViews.map(n=><button type="button" key={n.id} onClick={()=>setView(n.id)} className={`lab-tab ${view===n.id?'active':''}`} aria-current={view===n.id?'page':undefined}><n.icon size={15}/>{n.label}</button>)}<a href="http://localhost:8501" target="_blank" rel="noreferrer" className="lab-tab"><MonitorPlay size={15}/>Streamlit<ExternalLink size={12}/></a></div>}
        {view!=='historical'&&<div className="historical-note"><div className="flex items-center gap-2.5"><Radio size={15} className="shrink-0 text-[#b493db]"/><span><strong>Reproducción de datos históricos.</strong> Las conexiones observadas al chat no son actividad actual de Twitch ni espectadores activos comprobados.</span></div><span className="note-tag shrink-0 text-[10px] uppercase tracking-widest border border-[#4a375e] rounded px-2 py-1">Historical replay</span></div>}
    {view!=='historical'&&(error||link==='offline'||link==='reconnecting'||link==='polling')&&<div className="connection-warning" role="status"><AlertTriangle size={16}/><span>{link==='polling'?'WebSocket no disponible. Recibiendo resultados reales por polling de respaldo.':error||'Reconectando. Los últimos resultados confirmados permanecen visibles.'}</span></div>}
    {view!=='historical'&&<div className="flex items-center justify-between gap-3 flex-wrap mb-4 text-[11px] text-[#77798d]"><span className="flex items-center gap-2"><CircleDot size={12} className={active?'text-[#7cbca0]':'text-[#ad9774]'}/>{active?'Spark está escuchando eventos':data?.consumer?.stale?'Spark sin señal reciente':'Esperando señal del consumidor'}<span className="text-[#353541] mx-1">|</span>{runId?'Selección: una ejecución':'Selección: todas las ejecuciones'}</span>{runId&&<button className="flex items-center gap-2 mono text-[#a299b2]" onClick={()=>void copyRun()} title={runId}>{runId.slice(0,8)}…{copied?<CheckCheck size={12}/>:<Copy size={12}/>}</button>}</div>}
    {view!=='historical'&&!data&&!error?<div aria-label="Cargando resultados" className="kpi-grid">{[1,2,3,4].map(i=><div key={i} className="skeleton h-[137px]"/>)}</div>:view!=='guide'&&view!=='historical'&&<div className="kpi-grid"><Kpi label="Eventos procesados" value={data?.metrics.total} detail="Identidades únicas · sin duplicar reenvíos" icon={Activity} featured testId="total-events"/><Kpi label="Conexiones válidas" value={data?.metrics.valid} detail={validShare} icon={ShieldCheck} accent="text-[#addac4]"/><Kpi label="Registros inválidos" value={data?.metrics.invalid} detail="Reglas de calidad de Silver aplicadas" icon={AlertTriangle}/><Kpi label="Minutos estimados" value={data?.metrics.estimated_minutes_total} detail="Duración por intervalos · no tiempo visto" icon={Clock3}/></div>}
    {view==='historical'&&<HistoricalPage/>}
    {view==='overview'&&<><div className="charts-grid"><ActivityPanel data={data}/><RankingPanel data={data} onExpand={()=>setView('audience')}/></div><Pipeline data={data}/></>}
    {view==='audience'&&<><RankingPanel data={data} expanded onExpand={()=>setView('overview')}/><Card className="chart-panel mt-5"><h2 className="panel-title">Detrás del ranking</h2><p className="panel-subtitle">Hasta 20 streamers · usuarios distintos y retención no forman parte de este incremento.</p><div className="overflow-x-auto"><table className="data-table"><thead><tr><th>Posición</th><th>Streamer</th><th>Conexiones</th><th>Minutos estimados</th></tr></thead><tbody>{data?.streamers.map(r=><tr key={r.streamer}><td>{r.rank}</td><td>{r.streamer}</td><td>{format(r.interactions)}</td><td>{format(r.estimated_minutes)}</td></tr>)}</tbody></table></div></Card></>}
    {view==='pipeline'&&<><Pipeline data={data} expanded/><Card className="chart-panel mt-5"><h2 className="panel-title">Estado del consumidor</h2><p className="panel-subtitle">Independiente del estado de conexión de la interfaz.</p><div className="batch-strip"><span>Estado <strong>{data?.consumer?.state||'Sin señal'}</strong></span><span>Heartbeat <strong>{stamp(data?.consumer?.heartbeat)}</strong></span><span>Lectura API <strong>{stamp(data?.observed_at)}</strong></span></div></Card></>}
    {view==='guide'&&<Card className="chart-panel help-content"><h2 className="panel-title">Listo para la sustentación</h2><p className="mt-4">1. Mantén I4 en ejecución y selecciona el run_id que vas a reproducir. Una ejecución nueva empieza sin eventos.</p><p>2. Desde una terminal, usa el productor original de I4 con ese run_id, 100 eventos y velocidad configurable. La API de esta interfaz solo lee resultados.</p><p>3. Muestra el incremento en eventos confirmados, la actividad y los streamers. En Procesamiento puedes verificar el último micro-lote y sus offsets.</p><p>4. Si WebSocket se interrumpe, el monitor consulta por HTTP e intenta reconectar. Streamlit sigue disponible como respaldo.</p><p className="mt-4">Guía completa: <code>audience_experience/README.md</code><br/>Parada del productor: <code>live_monitor.control stop --run-id TU_UUID</code></p><a href="http://localhost:8501" target="_blank" rel="noreferrer" className="inline-flex gap-2 items-center text-[#bd99ec] mt-4">Abrir respaldo Streamlit<ExternalLink size={13}/></a></Card>}
    <footer className="footer"><span>Twitch Analytics Lakehouse <span className="mx-2">/</span> De datos observados a decisiones informadas.</span><span className="flex items-center gap-1.5"><Database size={11}/>Lectura de resultados persistidos · I5</span></footer>
    </main></div>
  </div>
}
