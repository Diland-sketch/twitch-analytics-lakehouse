import {render,screen,fireEvent} from '@testing-library/react'
import {describe,it,expect,vi} from 'vitest'
import App from '../App'
import {fixture} from './fixture'
const state=vi.hoisted(()=>({value:{snapshot:null as unknown,link:'websocket',error:null as string|null,reconnect:vi.fn()}}))
vi.mock('../hooks/useLiveSnapshot',()=>({useLiveSnapshot:()=>state.value}))
vi.mock('../components/LiveChart',()=>({LiveChart:({label}:{label:string})=><div role="img" aria-label={label}/>}))

describe('experiencia de audiencia',()=>{
  it('representa cero real y conserva la advertencia histórica',()=>{
    state.value.snapshot=fixture()
    render(<App/>)
    expect(screen.getByTestId('total-events')).toHaveTextContent('0')
    expect(screen.getByText(/no son actividad actual de Twitch/)).toBeInTheDocument()
    expect(screen.getByText('Listo para recibir actividad')).toBeInTheDocument()
    expect(screen.queryByText(/100.0%/)).not.toBeInTheDocument()
  })
  it('refleja el total confirmado y la selección de procesamiento',()=>{
    state.value.snapshot=fixture(100)
    render(<App/>)
    expect(screen.getByTestId('total-events')).toHaveTextContent('100')
    fireEvent.click(screen.getAllByRole('button',{name:'Procesamiento'})[0])
    expect(screen.getByText('Estado del consumidor')).toBeInTheDocument()
  })
  it('presenta el dato anterior con aviso de pérdida de conexión',()=>{
    state.value.snapshot=fixture(100);state.value.link='offline';state.value.error='Conexión interrumpida'
    render(<App/>)
    expect(screen.getByRole('status')).toHaveTextContent('Conexión interrumpida')
    expect(screen.getByTestId('total-events')).toHaveTextContent('100')
    state.value.link='websocket';state.value.error=null
  })
})
