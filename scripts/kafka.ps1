# Start (or format, then start) the single-node Kafka 3.9 broker in KRaft mode.
#   powershell -File scripts/kafka.ps1          start the broker (foreground)
#   powershell -File scripts/kafka.ps1 -Format  wipe and format the log directory first
param([switch]$Format)
$env:JAVA_HOME = "C:\Users\courts\tools\jdk17"
$env:KAFKA_HEAP_OPTS = "-Xmx384M -Xms128M"
$kafka = "C:\Users\courts\tools\kafka"
Set-Location $kafka
if ($Format) {
    Remove-Item -Recurse -Force "C:\Users\courts\tools\kafka-data" -ErrorAction SilentlyContinue
    $id = (& .\bin\windows\kafka-storage.bat random-uuid).Trim()
    & .\bin\windows\kafka-storage.bat format -t $id -c config\kraft\server.properties
}
& .\bin\windows\kafka-server-start.bat config\kraft\server.properties
