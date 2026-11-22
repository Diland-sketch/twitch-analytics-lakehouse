import {useEffect,useState} from 'react'

export type Historical = {
  schema_version:string; kind:string; release:string; source_namespace:string; exported_at:string
  quality:{status:string;bronze_rows:number;silver_rows:number;rejected_rows:number;gold_streamers:number;estimated_minutes:number}
  top_streamers:{streamer_username:string; interactions:number;unique_users:number;unique_streams:number;recurring_users:number;recurrence_rate:number;estimated_minutes:number}[]
  duration_buckets:{duration_bucket:number;interactions:number;estimated_minutes:number}[]
  activity:{relative_minutes:number;connection_starts:number}[]
  activity_truncated:boolean
  definitions:{interaction:string;estimated_minutes:string;recurrence:string}
}
export function useHistorical() {
  const [data,setData]=useState<Historical|null>(null)
  const [status,setStatus]=useState<'loading'|'ready'|'missing'|'error'>('loading')
  useEffect(()=>{
    const abort=new AbortController()
    fetch('/api/v1/historical', {signal:abort.signal,cache:'no-store'}).then(async response=>{
      if(response.status===404){setStatus('missing');return}
      if(!response.ok) throw new Error('Exportación histórica inválida o no disponible')
      setData(await response.json() as Historical)
      setStatus('ready')
    }).catch(()=>{if(!abort.signal.aborted)setStatus('error')})
    return ()=>abort.abort()
  },[])
  return {data,status}
}
