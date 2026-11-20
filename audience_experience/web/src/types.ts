export type Snapshot = {
  schema_version: string; revision: string; observed_at: string; status: 'ready' | 'empty'
  selection: {run_id: string | null}
  metrics: {total: number; valid: number; invalid: number; streamer_interactions_total: number; estimated_minutes_total: number}
  activity: {completed_at: string; events: number; cumulative_events: number}[]
  streamers: {streamer: string; rank: number; interactions: number; estimated_minutes: number}[]
  runs: {run_id: string; producer_status: string; acknowledged: number; requested: number; source_name: string}[]
  consumer: {state: string; heartbeat: string; stale: boolean; heartbeat_age_seconds: number} | null
  last_batch: {pipeline_id: string; batch_id: number; completed_at: string; input_rows: number; new_events: number; valid_events: number; invalid_events: number; offsets: {topic: string; partition: number; first_offset: number; last_offset: number; records: number}[]} | null
  transport_total: number; scopes: Record<string, string>
}
export type LinkState = 'loading' | 'websocket' | 'polling' | 'reconnecting' | 'offline'
