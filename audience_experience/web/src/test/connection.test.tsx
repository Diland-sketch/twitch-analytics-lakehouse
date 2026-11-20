import {act,renderHook,waitFor} from '@testing-library/react'
import {afterEach,beforeEach,describe,expect,it,vi} from 'vitest'
import {useLiveSnapshot} from '../hooks/useLiveSnapshot'
import {fixture} from './fixture'

class FakeSocket {
  static OPEN=1
  static instances:FakeSocket[]=[]
  readyState=0
  onopen:(()=>void)|null=null;onclose:(()=>void)|null=null;onerror:(()=>void)|null=null
  onmessage:((event:{data:string})=>void)|null=null
  constructor(public url:string){FakeSocket.instances.push(this)}
  open(){this.readyState=1;this.onopen?.()}
  close(){this.readyState=3;this.onclose?.()}
  sendSnapshot(data:ReturnType<typeof fixture>){this.onmessage?.({data:JSON.stringify({type:'snapshot',data})})}
}
const fetchMock=vi.fn()
beforeEach(()=>{FakeSocket.instances=[];fetchMock.mockReset();vi.stubGlobal('WebSocket',FakeSocket);vi.stubGlobal('fetch',fetchMock);fetchMock.mockResolvedValue({ok:true,json:async()=>fixture()})})
afterEach(()=>vi.unstubAllGlobals())

describe('transporte de snapshots reales',()=>{
  it('muestra el valor recibido sin incrementar por reloj',async()=>{
    const {result}=renderHook(()=>useLiveSnapshot(''))
    await waitFor(()=>expect(result.current.snapshot?.metrics.total).toBe(0))
    const socket=FakeSocket.instances[0]
    act(()=>{socket.open();socket.sendSnapshot(fixture(100))})
    expect(result.current.snapshot?.metrics.total).toBe(100)
    expect(result.current.link).toBe('websocket')
  })
  it('recupera por HTTP cuando el socket se cierra',async()=>{
    const {result}=renderHook(()=>useLiveSnapshot(''))
    await waitFor(()=>expect(result.current.snapshot).not.toBeNull())
    act(()=>{FakeSocket.instances[0].open();FakeSocket.instances[0].sendSnapshot(fixture(10))})
    fetchMock.mockResolvedValue({ok:true,json:async()=>fixture(100)})
    act(()=>FakeSocket.instances[0].close())
    await waitFor(()=>expect(result.current.link).toBe('polling'))
    expect(result.current.snapshot?.metrics.total).toBe(100)
  })
  it('conserva el último dato si API y socket se interrumpen',async()=>{
    const {result}=renderHook(()=>useLiveSnapshot(''))
    await waitFor(()=>expect(result.current.snapshot).not.toBeNull())
    act(()=>{FakeSocket.instances[0].open();FakeSocket.instances[0].sendSnapshot(fixture(100))})
    fetchMock.mockRejectedValue(new Error('offline'))
    act(()=>FakeSocket.instances[0].close())
    await waitFor(()=>expect(result.current.link).toBe('offline'))
    expect(result.current.snapshot?.metrics.total).toBe(100)
  })
  it('ignora respuestas de la ejecución anterior al cambiar selección',async()=>{
    const {result,rerender}=renderHook(({run})=>useLiveSnapshot(run),{initialProps:{run:'run-a'}})
    fetchMock.mockResolvedValue({ok:true,json:async()=>fixture(0,'run-b')})
    const old=FakeSocket.instances[0]
    rerender({run:'run-b'})
    act(()=>old.sendSnapshot(fixture(100,'run-a')))
    await waitFor(()=>expect(result.current.snapshot?.selection.run_id).toBe('run-b'))
    expect(result.current.snapshot?.metrics.total).toBe(0)
  })
  it('rechaza snapshots anteriores al último ya aceptado',async()=>{
    const {result}=renderHook(()=>useLiveSnapshot(''))
    await waitFor(()=>expect(result.current.snapshot).not.toBeNull())
    act(()=>{FakeSocket.instances[0].open();FakeSocket.instances[0].sendSnapshot(fixture(100));FakeSocket.instances[0].sendSnapshot(fixture(0))})
    expect(result.current.snapshot?.metrics.total).toBe(100)
  })
})
