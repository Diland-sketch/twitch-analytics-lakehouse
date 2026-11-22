import {useEffect, useState} from 'react'
import type {Snapshot, LinkState} from '../types'

export function useLiveSnapshot(runId: string, enabled = true) {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null)
  const [link, setLink] = useState<LinkState>('loading')
  const [error, setError] = useState<string | null>(null)
  const [refresh, setRefresh] = useState(0)
  useEffect(() => {
    if (!enabled) {
      setSnapshot(null); setLink('loading'); setError(null)
      return
    }
    let disposed = false, socket: WebSocket | null = null, retry: ReturnType<typeof setTimeout> | undefined
    let poll: ReturnType<typeof setInterval> | undefined, watchdog: ReturnType<typeof setInterval> | undefined
    let lastMessage = Date.now(), polling = false, attempts = 0, requestId = 0, acceptedRequest = 0
    let latestAcceptedAt = 0
    const abort = new AbortController()
    const query = runId ? `?run_id=${encodeURIComponent(runId)}` : ''
    setSnapshot(null); setLink('loading'); setError(null)
    const accept = (data: Snapshot) => {
      if (disposed || data.selection.run_id !== (runId || null) || data.schema_version !== '1.0') return
      const observed = Date.parse(data.observed_at)
      if (!Number.isFinite(observed) || observed < latestAcceptedAt) return
      latestAcceptedAt = observed
      setSnapshot(data); setError(null)
    }
    async function fetchSnapshot() {
      if (disposed || polling) return
      polling = true
      const id = ++requestId
      try {
        const response = await fetch(`/api/v1/snapshot${query}`, {signal: abort.signal, cache: 'no-store'})
        if (!response.ok) throw new Error(response.status === 503 ? 'Esperando resultados persistidos de I4.' : 'No se pudo consultar esta ejecución.')
        const data = await response.json() as Snapshot
        if (disposed || id < acceptedRequest || socket?.readyState === WebSocket.OPEN) return
        acceptedRequest = id; accept(data); setLink('polling')
      } catch (e) {
        if (!disposed) {setError(e instanceof Error ? e.message : 'Conexión interrumpida.'); setLink('offline')}
      } finally {polling = false}
    }
    function fallback() {
      if (disposed) return
      setLink('reconnecting'); void fetchSnapshot()
      if (!poll) poll = setInterval(() => void fetchSnapshot(), 2000)
    }
    function connect() {
      if (disposed) return
      socket = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/api/v1/stream${query}`)
      socket.onopen = () => {if (!disposed) {attempts = 0; lastMessage = Date.now()}}
      socket.onmessage = (event) => {
        if (disposed) return
        lastMessage = Date.now()
        try {
          const message = JSON.parse(event.data)
          if (message.type === 'snapshot') {
            accept(message.data); setLink('websocket')
            if (poll) {clearInterval(poll); poll = undefined}
          } else if (message.type === 'unavailable') {
            setError('Resultados de I4 temporalmente no disponibles.'); setLink('offline')
          }
        } catch {setError('Respuesta no válida. Conservamos el último resultado confirmado.')}
      }
      socket.onerror = () => socket?.close()
      socket.onclose = () => {
        if (disposed) return
        fallback()
        retry = setTimeout(connect, Math.min(1000 * 2 ** attempts++, 10000))
      }
    }
    void fetchSnapshot(); connect()
    watchdog = setInterval(() => {
      if (socket?.readyState === WebSocket.OPEN && Date.now() - lastMessage > 10000) socket.close()
    }, 2000)
    return () => {
      disposed = true; abort.abort(); socket?.close()
      clearTimeout(retry); clearInterval(poll); clearInterval(watchdog)
    }
  }, [runId, refresh, enabled])
  return {snapshot, link, error, reconnect: () => setRefresh(x => x + 1)}
}
