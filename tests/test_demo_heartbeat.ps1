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
