# Regresión de instante absoluto: ISO string (PS5), DateTime (PS7), offset.
$ErrorActionPreference='Stop'
$source=Join-Path $PSScriptRoot '../docs/entrega_final/scripts/validar_demo.ps1'
$tokens=$null;$parseErrors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile((Resolve-Path $source),[ref]$tokens,[ref]$parseErrors)
if($parseErrors.Count){throw 'Script de demostración no compila'}
$fn=$tree.Find({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'Get-HeartbeatInstant'},$true)
if(-not $fn){throw 'Falta conversor de heartbeat'}
. ([scriptblock]::Create($fn.Extent.Text))
$iso='2026-11-22T00:31:40.0000000-05:00'
$expected=[datetimeoffset]::Parse('2026-11-22T05:31:40Z',[Globalization.CultureInfo]::InvariantCulture)
$offset=[datetimeoffset]::Parse($iso,[Globalization.CultureInfo]::InvariantCulture)
$localDate=$offset.LocalDateTime
foreach($value in @($iso,$offset,$localDate)){
  $actual=Get-HeartbeatInstant $value
  if($actual -ne $expected){throw "Instante alterado para tipo $($value.GetType())"}
}
Write-Output 'PASS: ISO, DateTimeOffset y DateTime conservan el mismo instante UTC'
$wait=$tree.Find({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'Wait-ConsumerReady'},$true)
. ([scriptblock]::Create($wait.Extent.Text))
$TimeoutSeconds=2;$base='http://fixture';$runId='ARTIFICIAL'
$since=[datetimeoffset]::UtcNow
$script:responses=[Collections.Generic.Queue[object]]::new()
function Call-Api($url){return $script:responses.Dequeue()}
function Start-Sleep([int]$Seconds){} # Solo test: no espera ni hace solicitudes.
$script:responses.Enqueue([pscustomobject]@{consumer=[pscustomobject]@{state='FAILED';stale=$false;heartbeat=$since.AddSeconds(-1)}})
$script:responses.Enqueue([pscustomobject]@{consumer=[pscustomobject]@{state='RUNNING';stale=$false;heartbeat=$since.AddSeconds(1)}})
$ready=Wait-ConsumerReady $since
if($ready.consumer.state -ne 'RUNNING' -or $script:responses.Count -ne 0){throw 'Se aceptó estado anterior al contenedor nuevo'}
$script:responses.Enqueue([pscustomobject]@{consumer=[pscustomobject]@{state='FAILED';stale=$false;heartbeat=$since.AddSeconds(1)}})
$failed=$false
try { Wait-ConsumerReady $since | Out-Null } catch {if($_.Exception.Message -like 'Nuevo consumidor Spark*'){$failed=$true}else{throw}}
if(-not $failed){throw 'Un FAILED nuevo no debe ignorarse'}
Write-Output 'PASS: cierre FAILED anterior se distingue de fallo nuevo; RUNNING exige heartbeat nuevo'
