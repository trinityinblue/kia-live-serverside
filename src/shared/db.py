import sqlite3
import os
from typing import Dict
from src.shared.config import DB_PATH

def get_connection():
    return sqlite3.connect(DB_PATH)

def initialize_database():
    with get_connection() as conn:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS vehicle_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vehicle_id TEXT,
                trip_id TEXT,
                route_id TEXT,
                lat REAL,
                lon REAL,
                timestamp INTEGER,
                speed REAL,
                stop_id TEXT,
                status TEXT
            )
        ''')
        conn.commit()

def insert_vehicle_data(data: Dict):
    with get_connection() as conn:
        c = conn.cursor()
        c.execute('''
            INSERT INTO vehicle_positions (
                vehicle_id, trip_id, route_id, lat, lon,
                timestamp, speed, stop_id, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get("vehicle_id"),
            data.get("trip_id"),
            data.get("route_id"),
            data.get("lat"),
            data.get("lon"),
            data.get("timestamp"),
            data.get("speed"),
            data.get("stop_id"),
            data.get("status"),
        ))
        conn.commit()

