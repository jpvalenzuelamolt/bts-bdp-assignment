from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from bdi_api.settings import Settings

settings = Settings()

s5 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s5",
    tags=["s5"],
)


def get_engine() -> Engine:
    """Create and return a database engine from the configured database URL."""
    return create_engine(settings.db_url)


def is_postgres() -> bool:
    """Check if the database is PostgreSQL."""
    return "postgresql" in settings.db_url


# HR Database Schema SQL for SQLite
HR_SCHEMA_SQL_SQLITE = """
-- Drop tables in reverse dependency order (child tables first)
DROP TABLE IF EXISTS salary_history;
DROP TABLE IF EXISTS employee_project;
DROP TABLE IF EXISTS project;
DROP TABLE IF EXISTS employee;
DROP TABLE IF EXISTS department;

-- Create department table
CREATE TABLE department (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    location TEXT
);

-- Create employee table
CREATE TABLE employee (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    salary REAL NOT NULL,
    department_id INTEGER NOT NULL,
    hire_date DATE NOT NULL,
    FOREIGN KEY (department_id) REFERENCES department(id)
);

-- Create project table
CREATE TABLE project (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    department_id INTEGER NOT NULL,
    FOREIGN KEY (department_id) REFERENCES department(id)
);

-- Create employee_project junction table
CREATE TABLE employee_project (
    employee_id INTEGER NOT NULL,
    project_id INTEGER NOT NULL,
    PRIMARY KEY (employee_id, project_id),
    FOREIGN KEY (employee_id) REFERENCES employee(id),
    FOREIGN KEY (project_id) REFERENCES project(id)
);

-- Create salary_history table
CREATE TABLE salary_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id INTEGER NOT NULL,
    change_date DATE NOT NULL,
    old_salary REAL,
    new_salary REAL NOT NULL,
    reason TEXT,
    FOREIGN KEY (employee_id) REFERENCES employee(id)
);

-- Create indexes
CREATE INDEX idx_employee_department_id ON employee(department_id);
CREATE INDEX idx_project_department_id ON project(department_id);
CREATE INDEX idx_employee_project_employee_id ON employee_project(employee_id);
CREATE INDEX idx_employee_project_project_id ON employee_project(project_id);
CREATE INDEX idx_salary_history_employee_id ON salary_history(employee_id);
"""

# HR Database Schema SQL for PostgreSQL
HR_SCHEMA_SQL_POSTGRES = """
-- Drop tables in reverse dependency order (child tables first)
DROP TABLE IF EXISTS salary_history CASCADE;
DROP TABLE IF EXISTS employee_project CASCADE;
DROP TABLE IF EXISTS project CASCADE;
DROP TABLE IF EXISTS employee CASCADE;
DROP TABLE IF EXISTS department CASCADE;

-- Create department table
CREATE TABLE department (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(100)
);

-- Create employee table
CREATE TABLE employee (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    salary DECIMAL(10, 2) NOT NULL,
    department_id INTEGER NOT NULL,
    hire_date DATE NOT NULL,
    FOREIGN KEY (department_id) REFERENCES department(id) ON DELETE CASCADE
);

-- Create project table
CREATE TABLE project (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    department_id INTEGER NOT NULL,
    FOREIGN KEY (department_id) REFERENCES department(id) ON DELETE CASCADE
);

-- Create employee_project junction table
CREATE TABLE employee_project (
    employee_id INTEGER NOT NULL,
    project_id INTEGER NOT NULL,
    PRIMARY KEY (employee_id, project_id),
    FOREIGN KEY (employee_id) REFERENCES employee(id) ON DELETE CASCADE,
    FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE
);

-- Create salary_history table
CREATE TABLE salary_history (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    change_date DATE NOT NULL,
    old_salary DECIMAL(10, 2),
    new_salary DECIMAL(10, 2) NOT NULL,
    reason VARCHAR(100),
    FOREIGN KEY (employee_id) REFERENCES employee(id) ON DELETE CASCADE
);

-- Create indexes
CREATE INDEX idx_employee_department_id ON employee(department_id);
CREATE INDEX idx_project_department_id ON project(department_id);
CREATE INDEX idx_employee_project_employee_id ON employee_project(employee_id);
CREATE INDEX idx_employee_project_project_id ON employee_project(project_id);
CREATE INDEX idx_salary_history_employee_id ON salary_history(employee_id);
"""

# Sample seed data
HR_SEED_DATA_SQL = """
-- Insert departments
INSERT INTO department (name, location) VALUES
('Engineering', 'San Francisco'),
('Sales', 'New York'),
('HR', 'Chicago'),
('Marketing', 'Los Angeles');

-- Insert employees
INSERT INTO employee (first_name, last_name, email, salary, department_id, hire_date) VALUES
('Alice', 'Johnson', 'alice@example.com', 85000.00, 1, '2020-01-15'),
('Bob', 'Smith', 'bob@example.com', 75000.00, 1, '2021-03-20'),
('Charlie', 'Brown', 'charlie@example.com', 70000.00, 2, '2020-06-10'),
('Diana', 'Prince', 'diana@example.com', 90000.00, 1, '2019-09-05'),
('Eve', 'Wilson', 'eve@example.com', 65000.00, 3, '2022-01-01'),
('Frank', 'Miller', 'frank@example.com', 72000.00, 2, '2021-07-12'),
('Grace', 'Lee', 'grace@example.com', 68000.00, 4, '2020-11-30'),
('Henry', 'Taylor', 'henry@example.com', 88000.00, 1, '2021-02-14');

-- Insert projects
INSERT INTO project (name, department_id) VALUES
('Project Alpha', 1),
('Project Beta', 1),
('Project Gamma', 2),
('Project Delta', 2),
('Project Epsilon', 4);

-- Assign employees to projects
INSERT INTO employee_project (employee_id, project_id) VALUES
(1, 1), (1, 2),
(2, 1), (2, 3),
(3, 3), (3, 4),
(4, 1), (4, 2),
(5, 5),
(6, 3), (6, 4),
(7, 5),
(8, 1), (8, 2);

-- Insert salary history
INSERT INTO salary_history (employee_id, change_date, old_salary, new_salary, reason) VALUES
(1, '2021-01-15', 80000.00, 85000.00, 'Promotion'),
(1, '2022-06-01', 85000.00, 90000.00, 'Merit increase'),
(2, '2022-01-01', 70000.00, 75000.00, 'Annual raise'),
(3, '2021-06-15', 65000.00, 70000.00, 'Promotion'),
(4, '2020-09-05', 85000.00, 90000.00, 'Hire'),
(8, '2022-03-01', 82000.00, 88000.00, 'Promotion');
"""


@s5.post("/db/init")
def init_database() -> str:
    """Create all HR database tables (department, employee, project,
    employee_project, salary_history) with their relationships and indexes.

    Use the BDI_DB_URL environment variable to configure the database connection.
    Default: sqlite:///hr_database.db
    """
    engine = get_engine()
    schema_sql = HR_SCHEMA_SQL_POSTGRES if is_postgres() else HR_SCHEMA_SQL_SQLITE
    
    with engine.connect() as conn:
        # Split SQL statements and execute each one separately
        # This is necessary because SQLite doesn't support multiple statements in one execute()
        statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
        for stmt in statements:
            conn.execute(text(stmt))
        conn.commit()
    return "OK"


@s5.post("/db/seed")
def seed_database() -> str:
    """Populate the HR database with sample data.

    Inserts departments, employees, projects, assignments, and salary history.
    """
    engine = get_engine()
    with engine.connect() as conn:
        # Clear existing data (in reverse dependency order)
        try:
            clear_statements = [
                "DELETE FROM salary_history",
                "DELETE FROM employee_project",
                "DELETE FROM project",
                "DELETE FROM employee",
                "DELETE FROM department"
            ]
            for stmt in clear_statements:
                try:
                    conn.execute(text(stmt))
                except Exception:
                    pass  # Ignore errors if tables don't exist
        except Exception:
            pass
        
        # Execute seed SQL statements one by one
        statements = [stmt.strip() for stmt in HR_SEED_DATA_SQL.split(';') if stmt.strip()]
        for stmt in statements:
            conn.execute(text(stmt))
        conn.commit()
    return "OK"


@s5.get("/departments/")
def list_departments() -> list[dict]:
    """Return all departments.

    Each department should include: id, name, location
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT id, name, location FROM department ORDER BY id")
        )
        departments = [
            {"id": row[0], "name": row[1], "location": row[2]}
            for row in result
        ]
    return departments


@s5.get("/employees/")
def list_employees(
    page: Annotated[
        int,
        Query(description="Page number (1-indexed)", ge=1),
    ] = 1,
    per_page: Annotated[
        int,
        Query(description="Number of employees per page", ge=1, le=100),
    ] = 10,
) -> list[dict]:
    """Return employees with their department name, paginated.

    Each employee should include: id, first_name, last_name, email, salary, department_name
    """
    engine = get_engine()
    offset = (page - 1) * per_page
    
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT e.id, e.first_name, e.last_name, e.email, e.salary, d.name as department_name
            FROM employee e
            JOIN department d ON e.department_id = d.id
            ORDER BY e.id
            LIMIT :limit OFFSET :offset
            """),
            {"limit": per_page, "offset": offset}
        )
        employees = [
            {
                "id": row[0],
                "first_name": row[1],
                "last_name": row[2],
                "email": row[3],
                "salary": float(row[4]),
                "department_name": row[5]
            }
            for row in result
        ]
    return employees


@s5.get("/departments/{dept_id}/employees")
def list_department_employees(dept_id: int) -> list[dict]:
    """Return all employees in a specific department.

    Each employee should include: id, first_name, last_name, email, salary, hire_date
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT id, first_name, last_name, email, salary, hire_date
            FROM employee
            WHERE department_id = :dept_id
            ORDER BY id
            """),
            {"dept_id": dept_id}
        )
        employees = [
            {
                "id": row[0],
                "first_name": row[1],
                "last_name": row[2],
                "email": row[3],
                "salary": float(row[4]),
                "hire_date": str(row[5])
            }
            for row in result
        ]
    return employees


@s5.get("/departments/{dept_id}/stats")
def department_stats(dept_id: int) -> dict:
    """Return KPI statistics for a department.

    Response should include: department_name, employee_count, avg_salary, project_count
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT 
                d.name,
                COUNT(DISTINCT e.id) as employee_count,
                AVG(e.salary) as avg_salary,
                COUNT(DISTINCT p.id) as project_count
            FROM department d
            LEFT JOIN employee e ON d.id = e.department_id
            LEFT JOIN project p ON d.id = p.department_id
            WHERE d.id = :dept_id
            GROUP BY d.id, d.name
            """),
            {"dept_id": dept_id}
        )
        row = result.fetchone()
        if row:
            return {
                "department_name": row[0],
                "employee_count": int(row[1]),
                "avg_salary": float(row[2]) if row[2] else 0,
                "project_count": int(row[3])
            }
    return {"department_name": "", "employee_count": 0, "avg_salary": 0, "project_count": 0}


@s5.get("/employees/{emp_id}/salary-history")
def salary_history(emp_id: int) -> list[dict]:
    """Return the salary evolution for an employee, ordered by date.

    Each entry should include: change_date, old_salary, new_salary, reason
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT change_date, old_salary, new_salary, reason
            FROM salary_history
            WHERE employee_id = :emp_id
            ORDER BY change_date
            """),
            {"emp_id": emp_id}
        )
        history = [
            {
                "change_date": str(row[0]),
                "old_salary": float(row[1]) if row[1] is not None else None,
                "new_salary": float(row[2]),
                "reason": row[3]
            }
            for row in result
        ]
    return history
