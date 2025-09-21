import random
import mysql.connector
from faker import Faker
import requests
from mysql.connector import Error

# Global variables for dynamic configuration
NUM_COMPANIES = 50  # Maximum total number of companies allowed in the database
NUM_EMPLOYEES = 5000  # Target number of employees per company

# Initialize Faker
fake = Faker()

# MySQL database configuration
db_config = {
    'host': 'localhost',
    'user': 'root',  # Replace with your MySQL username
    'password': 'root',  # Replace with your MySQL password
    'database': 'company_employee_db'
}


# Function to get random company suffix from a public API (fallback to Faker)
def get_company_suffix():
    try:
        response = requests.get('https://random-data-api.com/api/company/random_company', timeout=5)
        if response.status_code == 200:
            return response.json().get('suffix', fake.company_suffix())
        return fake.company_suffix()
    except requests.RequestException:
        return fake.company_suffix()


# Function to initialize database and tables (only if they don't exist)
def initialize_database():
    try:
        # Connect to MySQL server (without specifying a database to create it)
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password']
        )
        cursor = conn.cursor()

        # Create database if it doesn't exist
        cursor.execute("CREATE DATABASE IF NOT EXISTS company_employee_db")
        conn.commit()
        cursor.close()
        conn.close()

        # Connect to the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Check if companies table exists
        cursor.execute("SHOW TABLES LIKE 'companies'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE companies (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(255) NOT NULL UNIQUE,
                    location VARCHAR(255),
                    industry VARCHAR(255)
                )
            """)
            print("Created companies table.")

        # Check if employees table exists
        cursor.execute("SHOW TABLES LIKE 'employees'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE employees (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL UNIQUE,
                    company_id INT,
                    FOREIGN KEY (company_id) REFERENCES companies(id)
                )
            """)
            print("Created employees table.")

        conn.commit()
        cursor.close()
        conn.close()
        print("Database initialization checked/completed.")
    except Error as e:
        print(f"Error initializing database: {e}")


# Function to check if a company exists
def company_exists(cursor, company_name):
    cursor.execute("SELECT id FROM companies WHERE name = %s", (company_name,))
    result = cursor.fetchone()
    return result[0] if result else None


# Function to check if an employee exists
def employee_exists(cursor, employee_email):
    cursor.execute("SELECT id FROM employees WHERE email = %s", (employee_email,))
    return cursor.fetchone() is not None


# Function to get the number of employees for a company
def get_employee_count(cursor, company_id):
    cursor.execute("SELECT COUNT(*) FROM employees WHERE company_id = %s", (company_id,))
    return cursor.fetchone()[0]


# Function to generate and insert data into the database
def generate_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Get the current number of companies
        cursor.execute("SELECT COUNT(*) FROM companies")
        current_company_count = cursor.fetchone()[0]

        # Get the current max company ID to avoid conflicts
        cursor.execute("SELECT COALESCE(MAX(id), 0) FROM companies")
        max_company_id = cursor.fetchone()[0]

        # Get the current max employee ID to avoid conflicts
        cursor.execute("SELECT COALESCE(MAX(id), 0) FROM employees")
        max_employee_id = cursor.fetchone()[0]

        # Initialize new employee ID
        new_employee_id = max_employee_id + 1

        # Step 1: Add employees to existing companies if they have fewer than NUM_EMPLOYEES
        cursor.execute("SELECT id FROM companies")
        existing_companies = [row[0] for row in cursor.fetchall()]

        for company_id in existing_companies:
            employee_count = get_employee_count(cursor, company_id)
            if employee_count >= NUM_EMPLOYEES:
                print(
                    f"Company ID {company_id} already has {employee_count} employees (target: {NUM_EMPLOYEES}). Skipping.")
                continue

            employees_to_add = NUM_EMPLOYEES - employee_count
            print(f"Adding {employees_to_add} employees to Company ID {company_id}.")
            for j in range(1, employees_to_add + 1):
                employee_name = fake.name()
                employee_email = fake.email()

                # Check for duplicate employee
                if employee_exists(cursor, employee_email):
                    print(f"Skipping employee with email '{employee_email}' (already exists)")
                    continue

                # Insert employee into database
                cursor.execute(
                    "INSERT INTO employees (id, name, email, company_id) VALUES (%s, %s, %s, %s)",
                    (new_employee_id, employee_name, employee_email, company_id)
                )
                new_employee_id += 1

        # Step 2: Generate new companies if total is below NUM_COMPANIES
        if current_company_count >= NUM_COMPANIES:
            print(f"Maximum number of companies ({NUM_COMPANIES}) already reached. No new companies added.")
        else:
            # Calculate how many new companies can be added
            remaining_slots = NUM_COMPANIES - current_company_count
            companies_to_generate = min(remaining_slots, NUM_COMPANIES)

            # Generate up to companies_to_generate new companies
            inserted_companies = 0
            for i in range(max_company_id + 1, max_company_id + companies_to_generate + 1):
                company_name = f"{fake.company()} {get_company_suffix()}"
                company_location = fake.city()
                company_industry = fake.bs().split(' ')[-1].capitalize()

                # Check for duplicate company
                existing_company_id = company_exists(cursor, company_name)
                if existing_company_id:
                    print(f"Skipping company '{company_name}' (already exists with ID {existing_company_id})")
                    continue

                # Insert company into database
                cursor.execute(
                    "INSERT INTO companies (id, name, location, industry) VALUES (%s, %s, %s, %s)",
                    (i, company_name, company_location, company_industry)
                )
                company_id = i
                inserted_companies += 1

                # Generate NUM_EMPLOYEES employees for the new company
                for j in range(1, NUM_EMPLOYEES + 1):
                    employee_name = fake.name()
                    employee_email = fake.email()

                    # Check for duplicate employee
                    if employee_exists(cursor, employee_email):
                        print(f"Skipping employee with email '{employee_email}' (already exists)")
                        continue

                    # Insert employee into database
                    cursor.execute(
                        "INSERT INTO employees (id, name, email, company_id) VALUES (%s, %s, %s, %s)",
                        (new_employee_id, employee_name, employee_email, company_id)
                    )
                    new_employee_id += 1

            if inserted_companies > 0:
                print(f"Inserted {inserted_companies} new companies.")
            else:
                print("No new companies inserted (all generated companies were duplicates).")

        conn.commit()
        print("Data inserted successfully.")
    except Error as e:
        print(f"Error inserting data: {e}")
    finally:
        cursor.close()
        conn.close()


# Function to print summary from database
def print_summary():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Fetch and print companies
        cursor.execute("""
            SELECT c.id, c.name, c.location, c.industry, COUNT(e.id) as employee_count
            FROM companies c
            LEFT JOIN employees e ON c.id = e.company_id
            GROUP BY c.id, c.name, c.location, c.industry
        """)
        companies = cursor.fetchall()

        for company in companies:
            company_id, name, location, industry, emp_count = company
            print(f"Company: {name}, Location: {location}, Industry: {industry}, Employees: {emp_count}")

            # Fetch first 3 employees for the company
            cursor.execute("""
                SELECT e.name, e.email
                FROM employees e
                WHERE e.company_id = %s
                LIMIT 3
            """, (company_id,))
            employees = cursor.fetchall()
            print("Sample employees (first 3):")
            for emp in employees:
                print(f"  Employee: {emp[0]}, Email: {emp[1]}, Company ID: {company_id}")
            print()

        # Print total counts
        cursor.execute("SELECT COUNT(*) FROM companies")
        total_companies = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM employees")
        total_employees = cursor.fetchone()[0]
        print(f"Total Companies: {total_companies} (Max Allowed: {NUM_COMPANIES})")
        print(f"Total Employees: {total_employees}")

    except Error as e:
        print(f"Error retrieving summary: {e}")
    finally:
        cursor.close()
        conn.close()


# Main execution
if __name__ == "__main__":
    # Initialize database and tables (only creates if they don't exist)
    initialize_database()

    # Generate and insert data
    generate_data()

    # Print summary
    print("\nGenerated Companies and Employees:")
    print_summary()