import sqlite3
import os
from typing import Dict
from src.shared.config import DB_PATH

def get_connection():
    return sqlite3.connect(DB_PATH)

def initialize_database():
    # Ensure the directory for the database exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    with get_connection() as conn:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS completed_stop_times (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stop_id TEXT,
                trip_id TEXT,
                route_id TEXT,
                date TEXT,
                actual_arrival TEXT,
                actual_departure TEXT,
                scheduled_arrival TEXT,
                scheduled_departure TEXT,
                UNIQUE(stop_id, trip_id, date)
            )
        ''')
        # Table for vehicle positions over time
        c.execute('''
            CREATE TABLE IF NOT EXISTS vehicle_positions (
                trip_id TEXT,
                vehicle_id TEXT,
                route_id TEXT,
                latitude REAL,
                longitude REAL,
                timestamp INTEGER,
                PRIMARY KEY (trip_id, timestamp)
            )
        ''')
        conn.commit()


    

def insert_vehicle_data(data: Dict):
    with get_connection() as conn:
        c = conn.cursor()
        try:
            c.execute('''
                INSERT OR IGNORE INTO completed_stop_times (
                    stop_id, trip_id, route_id, date,
                    actual_arrival, actual_departure,
                    scheduled_arrival, scheduled_departure
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data["stop_id"],
                data["trip_id"],
                data["route_id"],
                data["date"],
                data["actual_arrival"],
                data["actual_departure"],
                data["scheduled_arrival"],
                data["scheduled_departure"]
            ))
            conn.commit()
        except Exception as e:
            print(f"Error inserting data for stop_id={data['stop_id']}, trip_id={data['trip_id']}: {e}")


def insert_vehicle_position(trip_id, vehicle_id, route_id, lat, lon, timestamp):
    with get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO vehicle_positions (
                    trip_id, vehicle_id, route_id, latitude, longitude, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (trip_id, vehicle_id, route_id, lat, lon, timestamp))
        except Exception as e:
            print(f"[DB] insert_vehicle_position error: {e}")


