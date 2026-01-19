# --- НАСТРОЙКИ ---
$csvFiles = @("drone_events_million.csv")
$hdfsDataPath = "/drone_data"
$hiveScriptName = "init_hive.sql"
$finalCsvName = "drone_swarm_analytics.csv"
$containerName = "hive-server"

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 1. ФУНКЦИЯ ОЖИДАНИЯ С ПРОВЕРКОЙ ВЫПОЛНЕНИЯ ЗАПРОСА
function Wait-For-Service($Name, $TestCommand, $MaxRetries = 30) {
    Write-Host "Waiting for ${Name}..." -ForegroundColor Cyan
    for ($i = 1; $i -le $MaxRetries; $i++) {
        try {
            $result = Invoke-Expression $TestCommand 2>&1
            if ($result -match "Safe mode is OFF" -or $result -match "default") {
                Write-Host "${Name} is ready!" -ForegroundColor Green ; return $true
            }
        } catch {}
        Write-Host "Attempt ${i}/${MaxRetries}: Service not ready, waiting..."
        Start-Sleep -Seconds 10
    }
    throw "${Name} failed to start. Check 'docker logs $containerName'"
}

# 2. ПРИНУДИТЕЛЬНАЯ ИНИЦИАЛИЗАЦИЯ (Решает проблему зависания Hive)
Write-Host "Forcing Metastore initialization..." -ForegroundColor Yellow
docker exec $containerName schematool -dbType postgres -initSchema 2>$null

# 3. ОЖИДАНИЕ СЕРВИСОВ
Wait-For-Service "HDFS" "docker exec namenode hdfs dfsadmin -safemode get"
# Ждем именно ответа от Beeline
Wait-For-Service "Hive" "docker exec $containerName beeline -u jdbc:hive2://127.0.0.1:10000 -n root -e 'SHOW DATABASES;'"

# 4. ПОДГОТОВКА HDFS
docker exec namenode hdfs dfs -mkdir -p $hdfsDataPath /user/hive/warehouse /tmp
docker exec namenode hdfs dfs -chmod g+w /user/hive/warehouse /tmp

# 5. ЗАГРУЗКА ДАННЫХ
foreach ($file in $csvFiles) {
    Write-Host "Processing ${file}..." -ForegroundColor Yellow
    if (Test-Path $file) {
        $content = Get-Content $file -Encoding Default | Select-Object -Skip 1
        $Utf8NoBom = New-Object System.Text.UTF8Encoding $false
        [System.IO.File]::WriteAllLines("$(Get-Location)\tmp_$file", $content, $Utf8NoBom)
        docker cp "tmp_$file" "namenode:/tmp/$file"
        docker exec namenode hdfs dfs -put -f /tmp/$file ${hdfsDataPath}/${file}
        docker exec namenode rm /tmp/$file
        Remove-Item "tmp_$file"
    }
}

# 6. ВЫПОЛНЕНИЕ HIVE И ЭКСПОРТ
if (Test-Path "./$hiveScriptName") {
    Write-Host "Executing Hive Script..." -ForegroundColor Yellow
    docker cp "./$hiveScriptName" ${containerName}:/tmp/init_hive.sql
    
    # Запуск вашего SQL-скрипта
    docker exec $containerName beeline -u "jdbc:hive2://127.0.0.1:10000/default" -n root -f /tmp/init_hive.sql

    Write-Host "Exporting results to ${finalCsvName}..." -ForegroundColor Cyan
    
    # SQL запрос на основе вашей структуры из init_hive.sql
    $hiveQuery = @"
    SELECT 
        drone_id, 
        drone_efficiency, 
        processed_zones, 
        CAST(avg_battery_during_mission AS DECIMAL(5,2)), 
        unique_zones_handled 
    FROM (
        SELECT 
            drone_id,
            CASE 
                WHEN processed_zones >= 5 THEN 'Highly Effective'
                WHEN processed_zones >= 2 THEN 'Effective'
                ELSE 'Needs Optimization'
            END as drone_efficiency,
            processed_zones,
            battery as avg_battery_during_mission,
            zone_id as unique_zones_handled
        FROM drone_db.drone_analytics
        WHERE zone_priority_class = 'High'
          AND event_type IN ('zone_claimed', 'zone_processed')
    ) t 
    LIMIT 1000
"@
    
    # Прямой экспорт в CSV без лишнего мусора (флаг --silent)
    docker exec $containerName beeline -u "jdbc:hive2://127.0.0.1:10000/default" -n root --silent=true --showHeader=true --outputformat=csv2 -e "$hiveQuery" | Out-File -FilePath "./$finalCsvName" -Encoding utf8

    # Исправление кодировки для Excel
    if (Test-Path $finalCsvName) {
        $rawContent = Get-Content $finalCsvName
        if ($rawContent.Count -gt 1) {
            $utf8WithBOM = New-Object System.Text.UTF8Encoding $true
            [System.IO.File]::WriteAllLines("$(Get-Location)\$finalCsvName", $rawContent, $utf8WithBOM)
            Write-Host "DONE! File saved: ${finalCsvName}" -ForegroundColor Green
        } else {
            Write-Host "Warning: Hive returned 0 rows. Check Spark processing." -ForegroundColor Red
        }
    }
}

# --- ЗАПУСК ДАШБОРДА (НОВОЕ) ---
Write-Host "Launching Dashboard..." -ForegroundColor Magenta
docker-compose up -d dashboard
Write-Host "✅ Dashboard is running at http://localhost:8501" -ForegroundColor Green