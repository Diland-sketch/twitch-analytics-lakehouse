import {test,expect} from '@playwright/test'
import {spawn,execFileSync} from 'node:child_process'
import {randomUUID,createHash} from 'node:crypto'
import {mkdirSync,writeFileSync,readFileSync,existsSync} from 'node:fs'
import path from 'node:path'

test('Kafka → Spark I4 → API → React: cero a cien eventos reales',async({page})=>{
  test.skip(process.env.I5_REAL_E2E!=='1','Opt-in: I5_REAL_E2E=1 y TWITCH_CSV real')
  test.setTimeout(240000)
  const csv=process.env.TWITCH_CSV
  expect(csv&&existsSync(csv)).toBeTruthy()
  const repo=path.resolve('../..'),runId=randomUUID(),evidence=path.join(repo,'work/i5-evidence',runId)
  mkdirSync(evidence,{recursive:true})
  const snapshots:unknown[]=[],wsTotals:number[]=[],uiTotals:number[]=[]
  const errors:string[]=[]
  let stdout='',exit:number|null=null,spawnError=''
  page.on('pageerror',error=>errors.push(error.message))
  page.on('websocket',socket=>socket.on('framereceived',({payload})=>{
    try {const message=JSON.parse(payload.toString());if(message.type==='snapshot'&&message.data.selection.run_id===runId)wsTotals.push(message.data.metrics.total)}catch{}
  }))
  const initial=await page.request.get(`/api/v1/snapshot?run_id=${runId}`)
  expect(initial.ok()).toBeTruthy()
  expect((await initial.json()).metrics.total).toBe(0)
  await page.goto(`/?run_id=${runId}`)
  await expect(page.getByTestId('total-events')).toHaveText('0')
  await expect(page.getByText('WebSocket activo',{exact:true})).toBeVisible()
  await page.screenshot({path:path.join(evidence,'before-0.png'),fullPage:true})
  const args=['compose','-f','live_monitor/compose.yaml','run','--rm','--no-deps','producer','python3','-m','live_monitor.producer','--csv','/input/100k_a.csv','--run-id',runId,'--count','100','--rate','5']
  const producer=spawn('docker',args,{cwd:repo,env:{...process.env,TWITCH_CSV:csv}})
  producer.stdout.on('data',chunk=>stdout+=chunk.toString())
  producer.stderr.on('data',chunk=>stdout+=chunk.toString())
  producer.on('exit',code=>exit=code)
  producer.on('error',error=>{spawnError=error.message})
  const report:Record<string,unknown>={run_id:runId,status:'RUNNING',started_at:new Date().toISOString(),source:'real 100k_a.csv'}
  try {
    await expect.poll(async()=>{
      const response=await page.request.get(`/api/v1/snapshot?run_id=${runId}`)
      if(!response.ok())return -1
      const data=await response.json();snapshots.push(data)
      uiTotals.push(Number((await page.getByTestId('total-events').innerText()).replaceAll('.','')))
      return data.metrics.total
    },{timeout:180000,intervals:[1000,2000]}).toBe(100)
    await expect(page.getByTestId('total-events')).toHaveText('100')
    await expect.poll(()=>exit,{timeout:60000}).toBe(0)
    expect(spawnError).toBe('')
    const final=(await (await page.request.get(`/api/v1/snapshot?run_id=${runId}`)).json())
    expect(final.metrics.valid).toBe(100);expect(final.metrics.invalid).toBe(0)
    expect(final.metrics.streamer_interactions_total).toBe(100)
    const lines=readFileSync(csv!,'utf8').split(/\r?\n/).slice(0,100)
    const expectedMinutes=lines.reduce((sum,line)=>{const p=line.split(',');return sum+(Number(p[4])-Number(p[3]))*10},0)
    expect(final.metrics.estimated_minutes_total).toBe(expectedMinutes)
    const acknowledgements=stdout.split(/\r?\n/).filter(line=>line.startsWith('{')&&line.includes('"event_id"')).map(line=>JSON.parse(line))
    expect(acknowledgements).toHaveLength(100)
    const sourceHash=createHash('sha256').update(readFileSync(csv!)).digest('hex')
    const expectedIds=new Set(Array.from({length:100},(_,i)=>createHash('sha256').update(`${runId}|${sourceHash}|${i+1}`).digest('hex')))
    expect(new Set(acknowledgements.map(a=>a.event_id))).toEqual(expectedIds)
    expect(wsTotals).toContain(0);expect(wsTotals).toContain(100)
    expect(uiTotals.some(n=>n>0&&n<100)).toBeTruthy()
    await page.screenshot({path:path.join(evidence,'after-100.png'),fullPage:true})
    expect(errors).toEqual([])
    Object.assign(report,{status:'PASS',kafka_acknowledged:100,spark_unique:100,api_total:final.metrics.total,frontend_total:100,estimated_minutes:expectedMinutes,source_sha256:sourceHash,ws_totals:wsTotals,ui_totals:uiTotals,final})
  } catch(error) {Object.assign(report,{status:'FAILED',error:String(error)});throw error}
  finally {
    report.finished_at=new Date().toISOString()
    writeFileSync(path.join(evidence,'report.json'),JSON.stringify(report,null,2))
    writeFileSync(path.join(evidence,'snapshots.json'),JSON.stringify(snapshots,null,2))
    writeFileSync(path.join(evidence,'producer.log'),stdout)
    try {writeFileSync(path.join(evidence,'spark.log'),execFileSync('docker',['compose','-f','live_monitor/compose.yaml','logs','--no-color','spark'],{cwd:repo,env:process.env}))}catch{}
    console.log(`Evidencia: ${evidence}`)
  }
})
