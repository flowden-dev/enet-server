param(
    [ValidateSet('Debug', 'Release', 'Debug-LZ4', 'Release-LZ4')]
    [string]$Configuration = 'Release'
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$bin = Join-Path $root "build\vs2022\x64\$Configuration"

function Invoke-Test([string]$Name, [string[]]$TestArguments = @()) {
    $path = Join-Path $bin "$Name.exe"
    if (!(Test-Path -LiteralPath $path -PathType Leaf)) { throw "Build enet-server.sln ($Configuration|x64) first; missing $path" }
    $process = New-Object System.Diagnostics.Process
    $process.StartInfo.FileName = $path
    $process.StartInfo.WorkingDirectory = $root
    $process.StartInfo.UseShellExecute = $false
    $process.StartInfo.Arguments = $TestArguments -join ' '
    try {
        if (!$process.Start()) { throw "Could not start $Name" }
        if (!$process.WaitForExit(15000)) {
            $process.Kill()
            $process.WaitForExit()
            throw "$Name $TestArguments timed out"
        }
        if ($process.ExitCode -ne 0) { throw "$Name $TestArguments failed with exit code $($process.ExitCode)" }
    } finally { $process.Dispose() }
}

foreach ($test in @('core_tests', 'network_tests', 'fragment_tests', 'buffer_alias_tests', 'send_limit_tests', 'config_io_tests', 'log_macro_tests')) {
    Invoke-Test $test
}
foreach ($mode in @('cleanup', 'cleanup-active', 'cleanup-async', 'delayed', 'graceful', 'send-close', 'immediate')) {
    Invoke-Test 'lifecycle_tests' @($mode)
}
Write-Host 'All 14 tests passed.'
