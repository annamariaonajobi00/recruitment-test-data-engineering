import mysql.connector
from mysql.connector import Error
import csv
import json
from datetime import datetime
import sys

class DataETL:
    def __init__(self):
        """Initialize database connection parameters"""
        self.config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'codetest',
            'password': 'swordfish',
            'database': 'codetest',
            'charset': 'utf8mb4',
            'use_unicode': True
        }
        self.connection = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            if self.connection.is_connected():
                print("Successfully connected to MySQL database")
                return True
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            return False
    
    def create_schema(self):
        """Create database schema for people and places"""
        try:
            cursor = self.connection.cursor()
            
            # Drop tables if they exist (for repeatability)
            cursor.execute("DROP TABLE IF EXISTS people")
            cursor.execute("DROP TABLE IF EXISTS places")
            
            # Create places table
            cursor.execute("""
                CREATE TABLE places (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    city VARCHAR(255) NOT NULL,
                    county VARCHAR(255),
                    country VARCHAR(255),
                    UNIQUE KEY unique_city (city)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Create people table
            cursor.execute("""
                CREATE TABLE people (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(255) NOT NULL,
                    last_name VARCHAR(255) NOT NULL,
                    date_of_birth DATE NOT NULL,
                    place_of_birth_id INT,
                    FOREIGN KEY (place_of_birth_id) REFERENCES places(id),
                    INDEX idx_last_name (last_name),
                    INDEX idx_dob (date_of_birth)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            self.connection.commit()
            print("Schema created successfully")
            return True
            
        except Error as e:
            print(f"Error creating schema: {e}")
            return False
    
    def load_places(self, filepath='data/places.csv'):
        """Load places data from CSV"""
        try:
            cursor = self.connection.cursor()
            
            with open(filepath, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                for row in csv_reader:
                    try:
                        cursor.execute("""
                            INSERT INTO places (city, county, country)
                            VALUES (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE city=city
                        """, (row['city'], row.get('county'), row.get('country')))
                    except Error as e:
                        print(f"Error inserting place {row.get('city')}: {e}")
                        continue
            
            self.connection.commit()
            print(f"Places loaded successfully")
            return True
            
        except FileNotFoundError:
            print(f"File {filepath} not found")
            return False
        except Error as e:
            print(f"Error loading places: {e}")
            return False
    
    def load_people(self, filepath='data/people.csv'):
        """Load people data from CSV"""
        try:
            cursor = self.connection.cursor()
            
            with open(filepath, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                count = 0
                
                for row in csv_reader:
                    try:
                        # Parse date of birth
                        dob = datetime.strptime(row['date_of_birth'], '%Y-%m-%d').date()
                        
                        # Get place_id
                        cursor.execute("SELECT id FROM places WHERE city = %s", (row['place_of_birth'],))
                        place_result = cursor.fetchone()
                        place_id = place_result[0] if place_result else None
                        
                        # Insert person - using given_name and family_name from CSV
                        cursor.execute("""
                            INSERT INTO people (first_name, last_name, date_of_birth, place_of_birth_id)
                            VALUES (%s, %s, %s, %s)
                        """, (row['given_name'], row['family_name'], dob, place_id))
                        
                        count += 1
                        if count % 1000 == 0:
                            self.connection.commit()
                            print(f"Loaded {count} people...")
                            
                    except Exception as e:
                        print(f"Error inserting person {row.get('given_name')} {row.get('family_name')}: {e}")
                        continue
            
            self.connection.commit()
            print(f"Total people loaded: {count}")
            return True
            
        except FileNotFoundError:
            print(f"File {filepath} not found")
            return False
        except Error as e:
            print(f"Error loading people: {e}")
            return False
    
    def generate_output(self, output_file='output.json'):
        """Generate JSON output with people and their birth places"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT 
                    p.first_name,
                    p.last_name,
                    DATE_FORMAT(p.date_of_birth, '%Y-%m-%d') as date_of_birth,
                    pl.city as place_of_birth,
                    pl.county,
                    pl.country
                FROM people p
                LEFT JOIN places pl ON p.place_of_birth_id = pl.id
                ORDER BY p.last_name, p.first_name
            """)
            
            results = cursor.fetchall()
            
            # Format output
            output_data = []
            for row in results:
                person = {
                    'first_name': row['first_name'],
                    'last_name': row['last_name'],
                    'date_of_birth': row['date_of_birth'],
                    'place_of_birth': {
                        'city': row['place_of_birth'],
                        'county': row['county'],
                        'country': row['country']
                    }
                }
                output_data.append(person)
            
            # Write to JSON file
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            print(f"Output generated: {output_file} ({len(output_data)} records)")
            return True
            
        except Error as e:
            print(f"Error generating output: {e}")
            return False
   
    def close(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Database connection closed")

def main():
    """Main execution function"""
    etl = DataETL()
    
    try:
        # Connect to database
        if not etl.connect():
            sys.exit(1)
        
        # Create schema
        if not etl.create_schema():
            sys.exit(1)
        
        # Load data
        print("\nLoading places...")
        if not etl.load_places('data/places.csv'):
            print("Warning: Could not load places")
        
        print("\nLoading people...")
        if not etl.load_people('data/people.csv'):
            sys.exit(1)
        
        # Generate output
        print("\nGenerating output...")
        if not etl.generate_output('output.json'):
            sys.exit(1)

        print("\nETL process completed successfully!")
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        etl.close()

if __name__ == "__main__":
    main()
