import {test,expect} from '@playwright/test'
import {mkdirSync} from 'node:fs'
import path from 'node:path'
const evidence=path.resolve('../../work/i5-evidence/browser')
test.beforeAll(()=>mkdirSync(evidence,{recursive:true}))

test('render real, navegación y responsive sin modificar el origen',async({page})=>{
  const errors:string[]=[]
  page.on('pageerror',error=>errors.push(error.message))
  const api=await page.request.get('/api/v1/snapshot')
  expect(api.ok()).toBeTruthy()
  const data=await api.json()
  await page.goto('/')
  await expect(page.getByTestId('total-events')).toHaveText(data.metrics.total.toLocaleString('es-CO'))
  await expect(page.getByText('WebSocket activo')).toBeVisible()
  await expect(page.getByText(/no son actividad actual de Twitch/)).toBeVisible()
  await page.screenshot({path:path.join(evidence,'desktop-real.png'),fullPage:true})
  await page.getByRole('button',{name:'Procesamiento',exact:true}).first().click()
  await expect(page.getByText('Estado del consumidor',{exact:true})).toBeVisible()
  await page.getByRole('button',{name:'Audiencia',exact:true}).first().click()
  await expect(page.getByText('Detrás del ranking')).toBeVisible()
  await page.getByRole('button',{name:'Visión general',exact:true}).first().click()
  await page.setViewportSize({width:390,height:844})
  await expect(page.getByTestId('total-events')).toBeVisible()
  const overflow=await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth)
  expect(overflow).toBeFalsy()
  await page.screenshot({path:path.join(evidence,'mobile-real.png'),fullPage:true})
  expect(errors).toEqual([])
})

test('polling de respaldo usa la misma API real cuando falla WebSocket',async({page})=>{
  await page.routeWebSocket('**/api/v1/stream*',socket=>socket.close())
  const response=await page.request.get('/api/v1/snapshot')
  expect(response.ok()).toBeTruthy()
  const data=await response.json()
  await page.goto('/')
  await expect(page.getByText('Polling de respaldo',{exact:true})).toBeVisible()
  await expect(page.getByTestId('total-events')).toHaveText(data.metrics.total.toLocaleString('es-CO'))
})

test('indisponibilidad HTTP/WS no inventa ceros ni métricas',async({page})=>{
  await page.routeWebSocket('**/api/v1/stream*',socket=>socket.close())
  await page.route('**/api/v1/snapshot*',route=>route.fulfill({status:503,body:'{"detail":"unavailable"}'}))
  await page.goto('/')
  await expect(page.getByText('Sin conexión',{exact:true})).toBeVisible()
  await expect(page.getByTestId('total-events')).toHaveText('—')
  await expect(page.getByText(/Esperando resultados persistidos/)).toBeVisible()
})

test('reconecta WebSocket después de usar polling, sin cambiar conteos',async({page})=>{
  let connections=0
  await page.routeWebSocket('**/api/v1/stream*',socket=>{
    connections++
    if(connections===1) socket.close()
    else socket.connectToServer()
  })
  const api=await page.request.get('/api/v1/snapshot')
  const data=await api.json()
  await page.goto('/')
  await expect(page.getByText('Polling de respaldo',{exact:true})).toBeVisible()
  await expect(page.getByText('WebSocket activo',{exact:true})).toBeVisible({timeout:15000})
  await expect(page.getByTestId('total-events')).toHaveText(data.metrics.total.toLocaleString('es-CO'))
  expect(connections).toBeGreaterThan(1)
})
