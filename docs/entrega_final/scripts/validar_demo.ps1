#requires -Version 5.1
<#
.SYNOPSIS
  Auditoría E2E de una ejecución histórica controlada (Kafka -> Spark -> SQLite -> API).
.DESCRIPTION
  Genera UUID nuevo, registra SHA256/commit, publica N filas del CSV por Kafka,
  observa la API hasta N únicos, comprueba conciliaciones y guarda evidencias locales.
  Opcionalmente reinicia SOLO Spark conservando broker, SQLite, volúmenes y checkpoint;
  puede reenviar EXACTAMENTE el mismo rango para comprobar idempotencia.
  NO usa Twitch API; reproduce CSV histórico a velocidad objetivo.
  No borra datos ni archivos, no publica contraseñas ni simula eventos.
.EXAMPLE
  .\docs\entrega_final\scripts\validar_demo.ps1 -Count 100 -Rate 5
.EXAMPLE
  .\docs\entrega_final\scripts\validar_demo.ps1 -Count 1000 -Rate 20 -RestartSpark -Reenviar
#>
[CmdletBinding()]
param(
  [ValidateRange(1,10000)][int]$Count=100,
  [ValidateRange(0.1,1000)][double]$Rate=5,
  [ValidateRange(60,1800)][int]$TimeoutSeconds=300,
  [switch]$RestartSpark,
  [switch]$Reenviar
)
$ErrorActionPreference="Stop"
$repo=(Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
Set-Location $repo
$csv=if($env:TWITCH_CSV){$env:TWITCH_CSV}else{Join-Path $env:USERPROFILE 'Downloads\100k_a.csv'}
if(-not (Test-Path -LiteralPath $csv -PathType Leaf)){throw "Falta el CSV real: $csv"}
$env:TWITCH_CSV=(Resolve-Path -LiteralPath $csv).Path
$runId=[guid]::NewGuid().ToString()
$base='http://127.0.0.1:5173'
$dir=Join-Path $repo ("work\entrega_final\"+$runId)
New-Item -ItemType Directory -Force -Path $dir | Out-Null
$report=[ordered]@{run_id=$runId;status='RUNNING';requested=$Count;rate_target=$Rate;
 started_at=(Get-Date).ToUniversalTime().ToString('o');error=$null}
function Save-Json($name,$obj){
  $obj | ConvertTo-Json -Depth 32 | Set-Content -LiteralPath (Join-Path $dir $name) -Encoding UTF8
}
function Call-Api($url) {
  # El 503 nunca equivale a cero: la comprobación debe esperar o fallar.
  Invoke-RestMethod -Method Get -Uri $url -TimeoutSec 10
}
function Wait-Count([int]$target,[string]$stage){
  $until=(Get-Date).AddSeconds($TimeoutSeconds)
  $lastError=''
  while((Get-Date) -lt $until){
    try {
      $value=Call-Api "$base/api/v1/snapshot?run_id=$runId"
      $total=[long]$value.metrics.total
      if($total -gt $target){throw "Exceso de eventos unicos: $total > $target"}
      if($total -eq $target){return $value}
    } catch {
      if($_.Exception.Message -like '*Exceso de eventos*'){throw}
      $lastError=$_.Exception.Message
    }
    Start-Sleep -Seconds 2
  }
  throw "$stage: se agotó el tiempo esperando $target eventos únicos. $lastError"
}
function Publish-Same-Range([string]$fileName) {
  $argsList=@('compose','-f','live_monitor/compose.yaml','run','--rm','--no-deps',
    'producer','python3','-m','live_monitor.producer','--csv','/input/100k_a.csv',
    '--run-id',$runId,'--count',"$Count",'--rate',"$Rate")
  $log=@(& docker @argsList 2>&1 | ForEach-Object { "$_" })
  $code=$LASTEXITCODE
  $log | Set-Content -LiteralPath (Join-Path $dir $fileName) -Encoding UTF8
  if($code -ne 0){throw "Productor falló (exit=$code); revisar $fileName"}
  $lastLine=@($log | Where-Object { $_.Trim().StartsWith('{') }) | Select-Object -Last 1
  if(-not $lastLine){throw "Productor no registró resumen JSON"}
  $result=$lastLine | ConvertFrom-Json
  if($result.run_id -ne $runId -or $result.status -ne 'COMPLETED' -or
     [int]$result.acknowledged -ne $Count){throw "Confirmaciones del productor no concilian"}
  return $result
}
function Check-Counts($snap,$name){
  $m=$snap.metrics
  if([long]$m.total -ne $Count){throw "$name total distinto a $Count"}
  if(([long]$m.valid+[long]$m.invalid) -ne [long]$m.total){throw "$name valid+invalid != total"}
  if([long]$m.streamer_interactions_total -ne [long]$m.valid){throw "$name agregados != valid"}
  if([long]$m.estimated_minutes_total -lt 0){throw "$name minutos negativos"}
  $snap | ConvertTo-Json -Depth 32 | Set-Content -LiteralPath (Join-Path $dir $name) -Encoding UTF8
}
try {
  $branch=(& git branch --show-current).Trim()
  $commit=(& git rev-parse HEAD).Trim()
  if($branch -ne 'desarollo'){throw "Rama inesperada $branch; se exige desarollo"}
  $digest=(Get-FileHash -LiteralPath $env:TWITCH_CSV -Algorithm SHA256).Hash
  $report.branch=$branch
  $report.commit=$commit
  $report.source_sha256=$digest
  $report.source_name=[IO.Path]::GetFileName($env:TWITCH_CSV)
  $report.scope='historical_csv_replay_not_live_twitch'
  $apiHealth=Call-Api "$base/api/v1/health"
  if($apiHealth.status -ne 'ok'){throw 'API health no está OK'}
  $apiHealth | ConvertTo-Json -Depth 15 | Set-Content -LiteralPath (Join-Path $dir 'api_health.json') -Encoding UTF8
  $before=Call-Api "$base/api/v1/snapshot?run_id=$runId"
  if([long]$before.metrics.total -ne 0){throw 'UUID nuevo devuelve eventos antes de publicar'}
  Save-Json 'antes.json' $before
  Write-Host "run_id: $runId" -ForegroundColor Cyan
  Write-Host "Monitor: $base/?run_id=$runId"
  Write-Host 'Publicando filas históricas; Spark confirmará los conteos posteriormente.'
  # Un observador independiente registra la evolución real de la API durante el envío.
  # Cada línea es una lectura completa; no simula valores ni toca Kafka/Spark.
  $samples=Join-Path $dir 'evolucion_api.jsonl'
  $watcher=Start-Job -ArgumentList $base,$runId,$samples,$TimeoutSeconds -ScriptBlock {
    param($url,$id,$output,$seconds)
    $deadline=(Get-Date).AddSeconds($seconds)
    while((Get-Date) -lt $deadline){
      try {
        $snap=Invoke-RestMethod -Method Get -Uri "$url/api/v1/snapshot?run_id=$id" -TimeoutSec 8
        $line=$snap | ConvertTo-Json -Depth 32 -Compress
        [System.IO.File]::AppendAllText($output,$line+[Environment]::NewLine,[Text.Encoding]::UTF8)
      } catch {
        # El propio error se preserva con la prueba principal si impide llegar al total.
      }
      Start-Sleep -Seconds 2
    }
  }
  $published=Publish-Same-Range 'productor_primero.log'
  Save-Json 'productor_estado.json' $published
  $after=Wait-Count $Count 'Tras publicación'
  Check-Counts $after 'despues.json'
  $report.after_unique=[long]$after.metrics.total
  $report.after_valid=[long]$after.metrics.valid
  $report.after_invalid=[long]$after.metrics.invalid
  $report.after_minutes=[long]$after.metrics.estimated_minutes_total
  $report.transport_before_replay=[long]$after.transport_total
  Write-Host "PASS: $Count únicos; válidos=$($after.metrics.valid); inválidos=$($after.metrics.invalid)" -ForegroundColor Green
  if($RestartSpark){
    Write-Host 'Reiniciando solo Spark, sin eliminar volúmenes ni checkpoints.'
    & docker compose -f live_monitor/compose.yaml restart spark | Out-Null
    if($LASTEXITCODE -ne 0){throw 'Fallo restart spark'}
    $restartSnap=Wait-Count $Count 'Tras reinicio Spark'
    Check-Counts $restartSnap 'posterior_reinicio.json'
    if([long]$restartSnap.metrics.valid -ne [long]$after.metrics.valid -or
      [long]$restartSnap.metrics.invalid -ne [long]$after.metrics.invalid -or
      [long]$restartSnap.metrics.estimated_minutes_total -ne [long]$after.metrics.estimated_minutes_total){
      throw 'Métricas cambiaron tras reinicio controlado'
    }
    $report.restart_spark='PASS'
  }
  if($Reenviar){
    Write-Host 'Reenviando mismo run_id, fuente y rango: offsets pueden crecer pero únicos NO.'
    $baseline=Call-Api "$base/api/v1/snapshot?run_id=$runId"
    $transport=[long]$baseline.transport_total
    $again=Publish-Same-Range 'productor_reenvio.log'
    Save-Json 'reenvio_estado.json' $again
    $deadline=(Get-Date).AddSeconds($TimeoutSeconds)
    $last=$null
    while((Get-Date) -lt $deadline){
      $last=Call-Api "$base/api/v1/snapshot?run_id=$runId"
      if([long]$last.metrics.total -ne $Count){throw 'Reenvío infló eventos únicos'}
      if([long]$last.transport_total -ge ($transport+$Count)){break}
      Start-Sleep -Seconds 2
    }
    if(-not $last -or [long]$last.transport_total -lt ($transport+$Count)){
      throw 'No se corroboró el registro de todos los offsets del reenvío'
    }
    Check-Counts $last 'posterior_reenvio.json'
    $report.replay_transport_increment=[long]$last.transport_total-$transport
    $report.replay_idempotent='PASS'
  }
  $report.status='PASS'
  $sampled=@()
  $sampleFile=Join-Path $dir 'evolucion_api.jsonl'
  if(Test-Path -LiteralPath $sampleFile){
    $sampled=@(Get-Content -LiteralPath $sampleFile | ForEach-Object {
      try { ($_ | ConvertFrom-Json).metrics.total } catch { $null }
    } | Where-Object { $null -ne $_ })
  }
  $report.snapshots_observados=$sampled.Count
  $report.observo_intermedio=[bool](@($sampled | Where-Object { $_ -gt 0 -and $_ -lt $Count }).Count)
  # No inventar estados intermedios: si muestreo no capturó uno, queda false.
  Write-Host "PASS. Evidencias locales: $dir" -ForegroundColor Green
} catch {
  $report.status='FAIL'
  $report.error=$_.Exception.Message
  Write-Error "Prueba fallida: $($_.Exception.Message). Evidencias en $dir"
} finally {
  if($watcher){
    Stop-Job -Job $watcher -ErrorAction SilentlyContinue
    Receive-Job -Job $watcher -ErrorAction SilentlyContinue | Out-Null
    Remove-Job -Job $watcher -Force -ErrorAction SilentlyContinue
  }
  $report.finished_at=(Get-Date).ToUniversalTime().ToString('o')
  Save-Json 'report.json' $report
  $hashes=Get-ChildItem -LiteralPath $dir -File |
    ForEach-Object { Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256 } |
    Select-Object Path,Hash
  $hashes | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $dir 'sha256_manifest.json') -Encoding UTF8
  Write-Host "Reporte: $(Join-Path $dir 'report.json')"
}
if($report.status -ne 'PASS'){exit 1}
