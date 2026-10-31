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

Write-Host 'Running full corpus.'
& python -X utf8 tango_miner.py --input $inputPath -r
$pipelineExit = $LASTEXITCODE
if ($pipelineExit -ne 0) {
    Write-Host "Pipeline failed with exit code $pipelineExit."
    exit $pipelineExit
}
Write-Host 'Pipeline completed.'
exit 0
