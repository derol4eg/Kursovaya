import csv
import random
import os
import math
from shapely.geometry import Polygon, Point

# --- ПАРАМЕТРЫ ---
TARGET_RECORDS = 1_000_000
OUTPUT_FILE = "data/drone_events_million.csv"
POLYGON = Polygon([(200, 200), (1200, 200), (1200, 500), (200, 500)])
BASE_POS = (800, 650)
MODES = [0, 1]  # MODE_WEEDS, MODE_IRRIGATION
EVENT_TYPES = ["zone_discovered", "zone_claimed", "zone_processed", "drone_disabled"]
DRONE_COUNT = 10
ZONE_COUNT_PER_SIM = 200  # ~200 зон на симуляцию → ~600 событий


def generate_random_point_in_polygon(poly):
    minx, miny, maxx, maxy = poly.bounds
    while True:
        p = Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
        if poly.contains(p):
            return (p.x, p.y)


def main():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(
            ["timestamp", "event_type", "drone_id", "zone_id", "x", "y", "battery", "state", "mode", "mission_time"])

        records = 0
        zone_id = 0
        sim_id = 0

        while records < TARGET_RECORDS:
            mode = random.choice(MODES)
            # Генерируем зоны
            zones = []
            for _ in range(ZONE_COUNT_PER_SIM):
                x, y = generate_random_point_in_polygon(POLYGON)
                zones.append((zone_id, x, y))
                zone_id += 1

            # События по зонам
            for zid, x, y in zones:
                if records >= TARGET_RECORDS:
                    break

                # zone_discovered (от сканера)
                timestamp = round(random.uniform(0, 300), 3)
                writer.writerow([
                    timestamp, "zone_discovered", -1, zid, x, y,
                    100.0, "SCOUT", mode, timestamp
                ])
                records += 1

                # zone_claimed (случайный дрон)
                if random.random() > 0.1 and records < TARGET_RECORDS:  # 90% зон берутся в работу
                    drone_id = random.randint(0, DRONE_COUNT - 1)
                    timestamp = round(timestamp + random.uniform(1, 60), 3)
                    battery = round(random.uniform(40, 100), 1)
                    writer.writerow([
                        timestamp, "zone_claimed", drone_id, zid, x, y,
                        battery, "CLAIMING", mode, timestamp
                    ])
                    records += 1

                    # zone_processed
                    if random.random() > 0.05 and records < TARGET_RECORDS:  # 95% обрабатываются
                        timestamp = round(timestamp + random.uniform(1, 10), 3)
                        battery = round(battery - random.uniform(5, 20), 1)
                        state = "WORKED" if mode == 0 else "PAINTED"
                        writer.writerow([
                            timestamp, "zone_processed", drone_id, zid, x, y,
                            battery, state, mode, timestamp
                        ])
                        records += 1

            # Иногда добавляем отказы дронов
            if random.random() < 0.3 and records < TARGET_RECORDS:
                drone_id = random.randint(0, DRONE_COUNT - 1)
                x, y = BASE_POS
                timestamp = round(random.uniform(0, 300), 3)
                battery = round(random.uniform(0, 30), 1)
                writer.writerow([
                    timestamp, "drone_disabled", drone_id, -1, x, y,
                    battery, "DISABLED", mode, timestamp
                ])
                records += 1

            sim_id += 1
            if sim_id % 100 == 0:
                print(f"Generated {records} / {TARGET_RECORDS} records ({sim_id} simulations)")

    print(f"✅ Done! {records} records saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()