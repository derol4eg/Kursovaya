-- ВКЛЮЧАЕМ ЛОКАЛЬНЫЙ РЕЖИМ (если данных немного)
SET hive.exec.mode.local.auto=true;
SET mapreduce.framework.name=local;

-- 1. Настройка клиента
SET hive.cli.print.header=true;
CREATE DATABASE IF NOT EXISTS drone_db;
USE drone_db;

-- 2. Таблица событий от дронов
DROP TABLE IF EXISTS events;
CREATE EXTERNAL TABLE events (
    `timestamp` DOUBLE, 
    event_type STRING,
    drone_id INT,
    zone_id INT,
    x DOUBLE,
    y DOUBLE,
    battery DOUBLE,
    `state` STRING,      
    `mode` INT,          
    mission_time DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/drone_data'
TBLPROPERTIES ('skip.header.line.count'='1', 'serialization.encoding'='UTF-8');

-- 3. Проверка данных
SELECT 'Total rows in events:', COUNT(*) FROM events LIMIT 1;

-- 4. УЛУЧШЕННАЯ АНАЛИТИКА: Обогащённые данные о зонах и дронах
DROP TABLE IF EXISTS drone_analytics;
CREATE TABLE drone_analytics AS
SELECT 
    e.*,
    -- Расстояние до центра поля (пример: центр = (800, 350))
    SQRT(POW(e.x - 800, 2) + POW(e.y - 350, 2)) as dist_to_center,

    -- Количество обработанных зон на дрон
    COUNT(CASE WHEN e.event_type = 'zone_processed' THEN 1 END) 
        OVER (PARTITION BY e.drone_id) as processed_zones,

    -- Классифицируем приоритет зоны по координатам
    CASE 
        WHEN SQRT(POW(e.x - 800, 2) + POW(e.y - 350, 2)) < 200 THEN 'High'
        WHEN SQRT(POW(e.x - 800, 2) + POW(e.y - 350, 2)) BETWEEN 200 AND 400 THEN 'Medium'
        ELSE 'Low'
    END as zone_priority_class,

    -- Состояние батареи при событии
    CASE 
        WHEN e.battery > 70 THEN 'High'
        WHEN e.battery BETWEEN 30 AND 70 THEN 'Medium'
        ELSE 'Critical'
    END as battery_status

FROM events e;

-- 5. Промежуточная проверка
SELECT 'Rows in drone_analytics:', COUNT(*) FROM drone_analytics LIMIT 1;

-- 6. ВЫВОД: Аналитическая сводка
SELECT 
    drone_id,
    MAX(drone_efficiency) as drone_efficiency,
    MAX(processed_zones) as processed_zones,
    AVG(battery) as avg_battery_during_mission,
    COUNT(DISTINCT zone_id) as unique_zones_handled
FROM (
    SELECT 
        da.*,
        -- Эффективность дрона: сколько зон обработал
        CASE 
            WHEN processed_zones >= 5 THEN 'Highly Effective'
            WHEN processed_zones >= 2 THEN 'Effective'
            ELSE 'Needs Optimization'
        END as drone_efficiency
    FROM drone_analytics da
    WHERE zone_priority_class = 'High'
      AND event_type IN ('zone_claimed', 'zone_processed')
) t
GROUP BY drone_id
ORDER BY processed_zones DESC, avg_battery_during_mission DESC
LIMIT 10;
