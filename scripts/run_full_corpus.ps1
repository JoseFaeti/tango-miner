if ($args -contains "--help" -or $args -contains "-h" -or $args -contains "-?") {
    Write-Output 'Usage: ./scripts/run_full_corpus.ps1'
    Write-Output 'Run the configured full corpus with Anki disabled or in output-only mode.'
    exit 0
}
# Run from the project root so configuration and output paths resolve normally.
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
if ((Get-Location).ProviderPath -ne $projectRoot) {
    throw 'Run this script from the project root.'
}

$envFile = Join-Path $projectRoot '.env'
if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw 'Create .env with TANGO_CORPUS_PATH set to the agreed corpus directory.'
}
$entries = @(Get-Content -LiteralPath $envFile -Encoding UTF8 | Where-Object { $_ -match '^\s*TANGO_CORPUS_PATH\s*=' })
if ($entries.Count -ne 1) { throw '.env must contain exactly one TANGO_CORPUS_PATH entry.' }
$inputPath = ($entries[0] -replace '^\s*TANGO_CORPUS_PATH\s*=\s*', '').Trim()
if ($inputPath.Length -ge 2 -and (($inputPath[0] -eq '"' -and $inputPath[-1] -eq '"') -or ($inputPath[0] -eq "'" -and $inputPath[-1] -eq "'"))) {
    $inputPath = $inputPath.Substring(1, $inputPath.Length - 2)
}
if (-not [System.IO.Path]::IsPathRooted($inputPath)) { throw 'TANGO_CORPUS_PATH must be an absolute directory path.' }
if (-not (Test-Path -LiteralPath $inputPath -PathType Container)) {
    throw "Corpus directory not found: $inputPath"
}

$configJson = & python -X utf8 tango_miner.py --input $inputPath -r --show-config
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$config = $configJson | ConvertFrom-Json
if ($config.anki.enabled -and -not $config.anki.output_only) {
    throw 'Full corpus runs require Anki to be disabled or in output-only mode.'
}

# Pick up the persistent Windows user setting even in an already-open shell.
if (-not $env:NTFY_URL) {
    $env:NTFY_URL = [Environment]::GetEnvironmentVariable('NTFY_URL', 'User')
}
if ($env:NTFY_URL) {
    Write-Host 'ntfy notifications enabled.'
} else {
    Write-Warning 'NTFY_URL is not set; this run will not send an ntfy notification.'
}
Write-Host 'Running full corpus.'
& python -X utf8 tango_miner.py --input $inputPath -r
$pipelineExit = $LASTEXITCODE
if ($pipelineExit -ne 0) {
    Write-Host "Pipeline failed with exit code $pipelineExit."
    exit $pipelineExit
}
Write-Host 'Pipeline completed.'
exit 0
