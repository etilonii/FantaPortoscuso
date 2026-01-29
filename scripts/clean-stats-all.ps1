param(
  [switch]$Force
)
$ErrorActionPreference = "Stop"
$root = "C:\Users\Kekko\PycharmProjects\FantaPortoscuso"
$args = @()
if ($Force) {
  $args += "--force"
}
python "$root\scripts\clean_stats_batch.py" @args
