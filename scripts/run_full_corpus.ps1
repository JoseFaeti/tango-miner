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

$inputPath = 'C:\Users\Jose\workshop\Japanese\ScriptsToMine\input'
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
