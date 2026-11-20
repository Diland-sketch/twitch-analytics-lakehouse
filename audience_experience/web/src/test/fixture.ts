import type {Snapshot} from '../types'
// Exclusivamente ARTIFICIAL. No se importa en el bundle de producto.
export function fixture(total=0, runId:string|null=null):Snapshot {
  return {schema_version:'1.0',revision:`fixture-${total}`,observed_at:`2026-01-01T00:00:${String(total?10:0).padStart(2,'0')}Z`,status:total?'ready':'empty',selection:{run_id:runId},metrics:{total,valid:total,invalid:0,estimated_minutes_total:total*20,streamer_interactions_total:total},streamers:[],activity:[],runs:[],consumer:null,last_batch:null,transport_total:total,scopes:{last_batch:'global'}}
}
