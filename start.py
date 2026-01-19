import subprocess
import time
import socket
import os
import sys

# --- НАСТРОЙКИ ---
# Файлы должны лежать РЯДОМ со скриптом внутри контейнера
CSV_FILES = ["drone_events_million.csv"]
HDFS_DATA_PATH = "/drone_data"
HIVE_SCRIPT_NAME = "init_hive.sql"
FINAL_CSV_NAME = "drone_swarm_analytics.csv"

# Сетевые имена контейнеров (как они видны в docker network)
HIVE_HOST = "hive-server"
NAMENODE_HOST = "namenode"

def log(message, color="WHITE"):
    """Вывод с цветом"""
    colors = {
        "CYAN": "\033[96m", "GREEN": "\033[92m",
        "YELLOW": "\033[93m", "RED": "\033[91m", "RESET": "\033[0m"
    }
    print(f"{colors.get(color, '')}{message}{colors['RESET']}")

def run_cmd(command):
    """Запуск Bash команды внутри текущего контейнера"""
    # Мы больше не используем docker exec, так как мы уже внутри
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        log(f"Command failed: {command}", "RED")
        raise e

def check_hive_port():
    """Проверка доступности порта Hive по сети"""
    try:
        with socket.create_connection((HIVE_HOST, 10000), timeout=2):
            return True
    except (OSError, ConnectionRefusedError):
        return False

def check_hdfs_safemode():
    """Проверка Safe Mode (через прямой вызов hdfs клиента)"""
    try:
        # Пытаемся вызвать hdfs команду напрямую
        res = subprocess.run("hdfs dfsadmin -safemode get", shell=True, capture_output=True, text=True)
        return "Safe mode is OFF" in res.stdout
    except:
        return False

def wait_for_service(name, check_func, max_retries=30):
    log(f"Waiting for {name}...", "CYAN")
    for i in range(1, max_retries + 1):
        if check_func():
            log(f"{name} is ready!", "GREEN")
            return True
        print(f"Attempt {i}/{max_retries}: Waiting...")
        time.sleep(5)
    log(f"{name} failed to start.", "RED")
    sys.exit(1)

def main():
    # 0. Проверка наличия данных ВНУТРИ контейнера
    for f in CSV_FILES + [HIVE_SCRIPT_NAME]:
        if not os.path.exists(f):
            log(f"ERROR: File '{f}' not found inside container!", "RED")
            log("Did you allow time for copy? Run: docker cp local_file spark_master:/path", "YELLOW")
            sys.exit(1)

    # 1. ОЖИДАНИЕ СЕРВИСОВ
    wait_for_service("HDFS", check_hdfs_safemode)
    wait_for_service("Hive", check_hive_port)

    # 2. ПОДГОТОВКА HDFS (Прямые команды)
    log("Preparing HDFS directories...", "CYAN")
    # Используем || true, чтобы не падать, если папка уже есть
    run_cmd(f"hdfs dfs -mkdir -p {HDFS_DATA_PATH} /user/hive/warehouse /tmp")
    run_cmd("hdfs dfs -chmod g+w /user/hive/warehouse /tmp")

    # 3. ЗАГРУЗКА ДАННЫХ
    for file_name in CSV_FILES:
        log(f"Processing {file_name}...", "YELLOW")
        
        # Конвертация кодировки (Windows-1251 -> UTF-8 без BOM)
        # Мы делаем это прямо здесь, создавая временный файл
        tmp_file = f"ready_{file_name}"
        try:
            with open(file_name, "r", encoding="cp1251") as f_in:
                lines = f_in.readlines()
            
            content = lines[1:] if len(lines) > 1 else [] # Skip header
            
            with open(tmp_file, "w", encoding="utf-8", newline='\n') as f_out:
                f_out.writelines(content)
            
            # Загрузка в HDFS (напрямую)
            log(f"Uploading {file_name} to HDFS...", "CYAN")
            run_cmd(f"hdfs dfs -put -f {tmp_file} {HDFS_DATA_PATH}/{file_name}")
            os.remove(tmp_file)
            
        except Exception as e:
            log(f"Error processing {file_name}: {e}", "RED")

    # 4. HIVE (Через beeline, подключение к удаленному хосту)
    log("Executing Hive Script...", "YELLOW")
    # Обратите внимание: connect string указывает на HIVE_HOST
    # -u jdbc:hive2://hive-server:10000
    beeline_cmd = f"beeline -u jdbc:hive2://{HIVE_HOST}:10000 -n root --silent=true"
    
    run_cmd(f"{beeline_cmd} -f {HIVE_SCRIPT_NAME}")
# 5. ЭКСПОРТ
    log(f"Exporting analytics to {FINAL_CSV_NAME}...", "CYAN")
    hive_query = (
        "SET hive.cli.print.header=true; "
        "SELECT drone_id, drone_efficiency, processed_zones, "
        "CAST(avg_battery_during_mission AS DECIMAL(5,2)), unique_zones_handled "
        "FROM drone_db.drone_analytics LIMIT 1000"
    )
    
    run_cmd(f"{beeline_cmd} --outputformat=csv2 -e '{hive_query}' > {FINAL_CSV_NAME}")

    # 6. КОДИРОВКА ДЛЯ EXCEL (Добавляем BOM)
    if os.path.exists(FINAL_CSV_NAME):
        try:
            with open(FINAL_CSV_NAME, "r", encoding="utf-8") as f:
                content = f.read()
            with open(FINAL_CSV_NAME, "w", encoding="utf-8-sig", newline='\n') as f:
                f.write(content)
            log(f"Success! Result saved inside container at: /{FINAL_CSV_NAME}", "GREEN")
        except Exception as e:
            log(f"Error fix encoding: {e}", "RED")

if __name__ == "__main__":
    main()
