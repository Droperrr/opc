#!/usr/bin/env python3
"""
Create database backup for BANT project
"""

import sqlite3
import shutil
from datetime import datetime
import os

def create_sqlite_backup():
    """Create backup of SQLite databases"""
    try:
        # Create backup directory
        backup_dir = "backups"
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Backup sol_iv.db
        if os.path.exists("data/sol_iv.db"):
            backup_file = f"{backup_dir}/sol_iv_backup_{timestamp}.db"
            shutil.copy2("data/sol_iv.db", backup_file)
            print(f"✅ Created backup: {backup_file}")
        
        # Backup options_enriched.db
        if os.path.exists("data/options_enriched.db"):
            backup_file = f"{backup_dir}/options_enriched_backup_{timestamp}.db"
            shutil.copy2("data/options_enriched.db", backup_file)
            print(f"✅ Created backup: {backup_file}")
        
        # Create SQL dump for PostgreSQL compatibility
        create_sql_dump()
        
        print("✅ Database backup completed successfully")
        
    except Exception as e:
        print(f"❌ Error creating backup: {e}")

def create_sql_dump():
    """Create SQL dump for PostgreSQL compatibility"""
    try:
        conn = sqlite3.connect("data/sol_iv.db")
        
        # Get all table names
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        dump_file = "bant_backup.sql"
        with open(dump_file, 'w', encoding='utf-8') as f:
            f.write("-- BANT Database Backup\n")
            f.write(f"-- Created: {datetime.now().isoformat()}\n")
            f.write("-- Source: SQLite\n")
            f.write("-- Target: PostgreSQL\n\n")
            
            for table in tables:
                table_name = table[0]
                
                # Get table schema
                cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
                schema = cursor.fetchone()
                
                if schema:
                    # Convert SQLite schema to PostgreSQL
                    sqlite_schema = schema[0]
                    postgres_schema = convert_sqlite_to_postgres(sqlite_schema)
                    f.write(f"-- Table: {table_name}\n")
                    f.write(f"{postgres_schema};\n\n")
                    
                    # Get table data
                    cursor.execute(f"SELECT * FROM {table_name};")
                    rows = cursor.fetchall()
                    
                    if rows:
                        # Get column names
                        cursor.execute(f"PRAGMA table_info({table_name});")
                        columns = [col[1] for col in cursor.fetchall()]
                        
                        # Insert data
                        for row in rows:
                            values = []
                            for value in row:
                                if value is None:
                                    values.append("NULL")
                                elif isinstance(value, str):
                                    values.append(f"'{value.replace("'", "''")}'")
                                else:
                                    values.append(str(value))
                            
                            f.write(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});\n")
                        
                        f.write("\n")
        
        conn.close()
        print(f"✅ Created SQL dump: {dump_file}")
        
    except Exception as e:
        print(f"❌ Error creating SQL dump: {e}")

def convert_sqlite_to_postgres(sqlite_schema):
    """Convert SQLite schema to PostgreSQL"""
    # Basic conversion - replace SQLite specific types with PostgreSQL equivalents
    postgres_schema = sqlite_schema.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    postgres_schema = postgres_schema.replace("INTEGER", "INTEGER")
    postgres_schema = postgres_schema.replace("REAL", "DOUBLE PRECISION")
    postgres_schema = postgres_schema.replace("TEXT", "TEXT")
    postgres_schema = postgres_schema.replace("TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    
    return postgres_schema

if __name__ == "__main__":
    create_sqlite_backup()
