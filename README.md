# Project Overview

## Table of contents

<ul>
  <li><a href="#1-installation--setup">1. Installation & Setup</a>
    <ul>
      <li><a href="#11-install-postgresql-client">1.1 Install PostgreSQL Client</a></li>
      <li><a href="#12-set-up-virtual-environment-and-install-python-packages">1.2 Set up Virtual Environment and Install Python Packages</a></li>
    </ul>
  </li>

  <li><a href="#2-postgres-usage">2. Postgres Usage</a>
    <ul>
      <li><a href="#21-first-time-setup">2.1 First Time Setup</a></li>
      <li><a href="#22-interacting-with-the-database">2.2 Interacting with the Database</a></li>
      <li><a href="#23-deleting-a-database">2.3 Deleting a Database</a></li>
    </ul>
  </li>

  <li><a href="#3-dbeaver-installation--usage-optional">3. DBeaver Installation & Usage (Optional)</a>
    <ul>
      <li><a href="#31-install-dbeaver">3.1 Install DBeaver</a></li>
      <li><a href="#32-connect-to-your-database">3.2 Connect to your database</a></li>
      <li><a href="#33-deleting-a-database">3.3 Deleting a database</a></li>
    </ul>
  </li>
</ul>

## Project Description

This package creates a postgres database from provided csv files which can be interacted with directly through the terminal, a software called DBeaver, or via a web-app visual interface.

## Project Structure

```
project/
├── csv/ 
├── app.py 
├── db.py 
├── config.py 
├── import_csv_to_postgres.py 
├── requirements.txt
├── templates/ 
│   └── index.html 
└── static/ 
    ├── style.css 
    └── app.js
```

## File Descriptions

| File Name | Description |
|-----------|-------------|
| `config.py` | Stores configuration settings such as the database connection URL |
|`db.py` | Creates the SQLAlchemy engine and provides helper functions for interacting with the database. |
| `import_csv_to_postgres.py` | Imports CSV files into PostgreSQL, handling type conversion (e.g., strings, numbers, dates). |
| `app.py` | Runs the Flask application and defines API routes and page endpoints. |
| `templates/index.html` | Defines the structure of the webpage (HTML layout). |
| `static/app.js` | Handles frontend behavior such as fetching data from the backend and updating the UI. |
| `static/style.css` | Contains styling for the webpage (layout, colors, spacing, etc.). |

[Back to Top](#project-overview)

# 1. Installation & Setup

## 1.1 Install PostgreSQL Client

```bash
sudo apt update
sudo apt install postgresql postgresql-client

sudo systemctl start postgresql
sudo systemctl status postgresql
sudo systemctl enable postgresql
```

## 1.2 Set up Virtual Environment and Install Python Packages

Install python virtual environment libraries if not installed. This can be done using either conda or python virtual environments, however these instructions will be provided using python virtual environments.

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip

# make a directory for your virtual environments
mkdir -p ~/venvs
```

Create and source your virtual environment and install package requirements.

```bash
# if you followed along above, your virtual environment will be called database and installed in your ~/venvs directory
python3 -m venv ~/venv/database

# navigate to the base directory of this repository and install python libraries
# e.g. cd ~/database_ws/nist_response_db
pip install -r requirements.txt

# run script to build database by uploading csv files to database as tables
python import_csv_to_postgres.py
```

# 2. Postgres Usage

## 2.1 First Time Setup
```bash
# for first time setup, enter psql (postgres sql server) and perform some setup operations
sudo -u postgres psql

# for local work, this temporary password will suffice, make sure password matches in config.py
ALTER USER postgres WITH PASSWORD 'postgres';
CREATE DATABASE nist_response_db;
```

[Back to Top](#project-overview)

## 2.2 Interacting with the Database

These are some actions you can perform in order to interact with the database such as listing and connecting to available databases, and performing some SQL Queries. This is an optional step if you do not need to interact with the database manually.

```bash
# see available databses
\l

# connect to a database
\c nist_response_db
# or \connect nist_response_db

# confirm which database you're connected to
\conninfo

# connect to the database directly from bash
psql -U postgres -h localhost -d nist_response_db

# list all tables
\dt

# inspect a table
\d sensing

# do some sql
SELECT * FROM sensing LIMIT 5;
SELECT COUNT(*) FROM sensing;
SELECT robot_make, robot_model FROM sensing LIMIT 10;
SELECT DISTINCT robot_make FROM sensing;

# filter some data
SELECT * FROM sensing
WHERE robot_make = 'Boston Dynamics'
LIMIT 10;

SELECT robot_make, COUNT(*)
FROM sensing
GROUP BY robot_make
ORDER BY COUNT(*) DESC;

# find missing data
SELECT COUNT(*) FROM sensing WHERE robot_make IS NULL;

# sort data
SELECT * FROM sensing ORDER BY time DESC LIMIT 10;

# exit psql
\q
```

## 2.3 Deleting a Database

If you need to remove a database for whatever reason, follow these instructions.

```bash
# enter psql, enter your password
psql -U postgres -h localhost -d postgres

# list all tables
\dt

# delete a database
DROP DATABASE nist_response_db;

# if there are active connections, you can try to force the drop
DROP DATABASE nist_response_db WITH (FORCE);
```

[Back to Top](#project-overview)

# 3. DBeaver Installation & Usage (Optional)

DBeaver can optionally be installed and used in order to interact with the database using a visual UI, however it is not a requirement and can be skipped if you wish to interact with the database through a terminal or have no need to interact directly with the database.

## 3.1 Install DBeaver

```bash
# You can check out the DBeaver installation page here - https://dbeaver.io/download/
sudo wget -q -O - https://dbeaver.io/debs/dbeaver.gpg.key | sudo gpg --dearmor -o /usr/share/keyrings/dbeaver.gpg.key
echo "deb [signed-by=/usr/share/keyrings/dbeaver.gpg.key] https://dbeaver.io/debs/dbeaver-ce /" | sudo tee /etc/apt/sources.list.d/dbeaver.list
sudo apt-get update && sudo apt-get install dbeaver-ce
```

## 3.2 Connect to your database

1. Open DBeaver
2. Click "New Database Connection (top left, plug with (+)
3. Enter connection details, same values as in config.py
4. Choose PostgreSQL
5. Click Next

## 3.3 Deleting a database

1. disconnect from the database if connected
2. right click the database
3. delete/drop the database and confirm

[Back to Top](#project-overview)