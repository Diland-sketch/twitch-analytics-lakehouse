param(
    [Parameter(Mandatory=$true)][string]$Parts,
    [Parameter(Mandatory=$true)][string]$State,
    [Parameter(Mandatory=$true)][string]$Review,
    [string]$Python = 'python'
)
# Ejecución explícita del operador DESPUÉS de validar review. Sin contraseña en CLI.
$ErrorActionPreference = 'Stop'
$kafkaBootstrap = Read-Host 'Bootstrap público SASL_SSL (host:puerto)'
$kafkaUsername = Read-Host 'Usuario Kafka'
$kafkaPassword = Read-Host 'Contraseña Kafka (no se mostrará)' -AsSecureString
$previousKafkaEnv = @{}
foreach ($kafkaName in @('KAFKA_BOOTSTRAP_SERVERS','KAFKA_USERNAME','KAFKA_PASSWORD','KAFKA_SASL_MECHANISM')) {
    $previousKafkaEnv[$kafkaName] = [Environment]::GetEnvironmentVariable($kafkaName,'Process')
}
$kafkaPointer = [IntPtr]::Zero
try {
    $kafkaPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($kafkaPassword)
    $env:KAFKA_BOOTSTRAP_SERVERS = $kafkaBootstrap
    $env:KAFKA_USERNAME = $kafkaUsername
    $env:KAFKA_SASL_MECHANISM = 'SCRAM-SHA-256'
    $env:KAFKA_PASSWORD = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($kafkaPointer)
    & $Python -m bulk_lakehouse.kafka_async --parts $Parts --state $State --review $Review --publish
    if ($LASTEXITCODE -ne 0) { throw 'Productor detenido; conservar evidencias y revisar el tipo de fallo.' }
} finally {
    foreach ($kafkaName in $previousKafkaEnv.Keys) {
        [Environment]::SetEnvironmentVariable($kafkaName,$previousKafkaEnv[$kafkaName],'Process')
    }
    if ($kafkaPointer -ne [IntPtr]::Zero) { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($kafkaPointer) }
    $kafkaPassword.Dispose()
}
