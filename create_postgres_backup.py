#!/usr/bin/env python3
"""
Create PostgreSQL database backup for BANT project
"""

import os
import subprocess
import psycopg2
from datetime import datetime
import tempfile

def get_db_connection_params():
    """Get database connection parameters from environment variables"""
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'bant_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }

def create_postgres_backup():
    """Create PostgreSQL database backup using pg_dump"""
    try:
        # Get connection parameters
        params = get_db_connection_params()
        
        # Create backup filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"bant_backup_{timestamp}.sql"
        
        # Create pg_dump command
        cmd = [
            'pg_dump',
            '-h', params['host'],
            '-p', params['port'],
            '-U', params['user'],
            '-d', params['database'],
            '-f', backup_file,
            '--verbose'
        ]
        
        # Set password via environment variable to avoid prompt
        env = os.environ.copy()
        env['PGPASSWORD'] = params['password']
        
        print(f"🔄 Creating PostgreSQL backup: {backup_file}")
        print(f"📊 Database: {params['database']}@{params['host']}:{params['port']}")
        
        # Execute pg_dump
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Backup created successfully: {backup_file}")
            
            # Check file size
            if os.path.exists(backup_file):
                size = os.path.getsize(backup_file)
                print(f"📁 Backup size: {size / 1024 / 1024:.2f} MB")
            
            return backup_file
        else:
            print(f"❌ Error creating backup:")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            return None
            
    except FileNotFoundError:
        print("❌ pg_dump not found. Please install PostgreSQL client tools.")
        return None
    except Exception as e:
        print(f"❌ Error creating backup: {e}")
        return None

def test_db_connection():
    """Test database connection"""
    try:
        params = get_db_connection_params()
        
        print(f"🔄 Testing connection to {params['database']}@{params['host']}:{params['port']}")
        
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"✅ Database connection successful")
        print(f"📊 PostgreSQL version: {version}")
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def create_env_template():
    """Create .env template file"""
    env_template = """# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=bant_db
DB_USER=postgres
DB_PASSWORD=your_password_here

# Other Configuration
DEBUG=True
LOG_LEVEL=INFO
"""
    
    with open('.env.template', 'w') as f:
        f.write(env_template)
    
    print("✅ Created .env.template file")
    print("📝 Please copy .env.template to .env and update with your database credentials")

def main():
    """Main function"""
    print("🚀 BANT PostgreSQL Backup Tool")
    print("=" * 50)
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        print("⚠️ .env file not found")
        create_env_template()
        print("\n📋 Please create .env file with your database credentials and run again.")
        return
    
    # Test database connection
    if not test_db_connection():
        print("\n❌ Cannot proceed without database connection")
        return
    
    # Create backup
    backup_file = create_postgres_backup()
    
    if backup_file:
        print(f"\n🎉 Backup completed successfully!")
        print(f"📁 Backup file: {backup_file}")
        print(f"📋 Next steps:")
        print(f"   1. Copy {backup_file} to your server")
        print(f"   2. Use psql to restore: psql -U username -d database < {backup_file}")
    else:
        print("\n❌ Backup failed")

if __name__ == "__main__":
    main()
