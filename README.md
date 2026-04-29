# Business Intelligence Course Projects

[![Power BI](https://img.shields.io/badge/Open-Power%20BI%20Project-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](./Power%20BI)
[![Python T-SQL](https://img.shields.io/badge/Open-Python%20T--SQL%20Project-3776AB?style=for-the-badge&logo=python&logoColor=white)](./Python%20T-SQL%20Connection)
[![License: MIT](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](./LICENSE)

This repository contains two projects completed in the scope of a Business Intelligence course. Together, they cover core BI workflow concepts: dimensional modeling, SQL Server data warehouse preparation, Python-based ETL orchestration, and Power BI reporting.

## Repository Contents

| Project | Description | Main Artifacts |
| --- | --- | --- |
| [Power BI](./Power%20BI) | Power BI reporting project supported by a dimensional model. | `.pbix` report file, dimensional diagram PDF, dimensional diagram source text |
| [Python T-SQL Connection](./Python%20T-SQL%20Connection) | Python and T-SQL project for creating SQL Server BI infrastructure and loading dimensional data. | Python pipeline scripts, SQL table creation scripts, SCD scripts, staging scripts, raw Excel source, dependency file |

## Project 1: Power BI

[![Open Folder](https://img.shields.io/badge/Open%20Folder-Power%20BI-F2C811?style=flat-square&logo=powerbi&logoColor=black)](./Power%20BI)
[![Open Report](https://img.shields.io/badge/Open%20Report-PBIX-F2C811?style=flat-square)](./Power%20BI/DS_206_Group_7.pbix)
[![View Diagram](https://img.shields.io/badge/View%20Diagram-PDF-red?style=flat-square&logo=adobeacrobatreader&logoColor=white)](./Power%20BI/DS206_Group7_Dimensional_Diagram.pdf)

The Power BI folder contains the reporting component of the Business Intelligence coursework. It includes a Power BI Desktop report and supporting dimensional model documentation.

### Files

- [`DS_206_Group_7.pbix`](./Power%20BI/DS_206_Group_7.pbix): Power BI Desktop report file.
- [`DS206_Group7_Dimensional_Diagram.pdf`](./Power%20BI/DS206_Group7_Dimensional_Diagram.pdf): Exported dimensional model diagram.
- [`DS206_Group7_Dimensional_Diagram.txt`](./Power%20BI/DS206_Group7_Dimensional_Diagram.txt): Text definition of the dimensional model.

### Dimensional Model Summary

The included dimensional diagram defines a star-schema style model with:

- `FactSales` as the central fact table.
- `DimStore`, `DimItem`, `DimTime`, and `DimDistrict` as supporting dimensions.
- Surrogate keys for dimensional joins.
- Business and reporting attributes such as fiscal period, category, segment, district, store, and sales measures.

The fact table includes measures such as gross margin, regular sales, and markdown sales, making it suitable for sales performance analysis across store, product, time, and district perspectives.

## Project 2: Python T-SQL Connection

[![Open Folder](https://img.shields.io/badge/Open%20Folder-Python%20T--SQL-3776AB?style=flat-square&logo=python&logoColor=white)](./Python%20T-SQL%20Connection)
[![View Main Script](https://img.shields.io/badge/View-main.py-3776AB?style=flat-square&logo=python&logoColor=white)](./Python%20T-SQL%20Connection/main.py)
[![View Requirements](https://img.shields.io/badge/View-requirements--2.txt-222222?style=flat-square)](./Python%20T-SQL%20Connection/requirements-2.txt)

The Python T-SQL Connection folder contains the data engineering component of the coursework. It combines Python, SQL Server connectivity, raw Excel ingestion, SQL scripts, and logging to support a dimensional data pipeline.

### Main Components

- [`main.py`](./Python%20T-SQL%20Connection/main.py): Entry point for executing the dimensional data flow.
- [`utils.py`](./Python%20T-SQL%20Connection/utils.py): Utility functions for UUID generation, database connections, and SQL script execution.
- [`py_logging.py`](./Python%20T-SQL%20Connection/py_logging.py): Logging setup with execution IDs for traceability.
- [`requirements-2.txt`](./Python%20T-SQL%20Connection/requirements-2.txt): Python dependencies.
- [`Infrastructure Initiation`](./Python%20T-SQL%20Connection/Infrastructure%20Initiation): SQL scripts for database, staging, dimensional, SOR, and SCD setup.
- [`Pipeline Dimensional Data`](./Python%20T-SQL%20Connection/Pipeline%20Dimensional%20Data): Pipeline code, data loading logic, SQL insert/update queries, and the raw Excel data source.

### SQL and ETL Scope

The project includes scripts and code for:

- Creating the `ORDER_DDS` SQL Server database.
- Creating staging tables for raw source data.
- Creating dimension and fact tables.
- Creating source-of-record support through `Dim_SOR`.
- Creating slowly changing dimension support tables.
- Loading source data from `raw_data_source.xlsx`.
- Populating dimensional tables and fact-related structures.
- Logging each pipeline execution with a generated execution ID.

### Setup

From the `Python T-SQL Connection` folder:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements-2.txt
```

On Windows, activate the virtual environment with:

```bash
venv\Scripts\activate
```

### Database Configuration

The Python code expects a SQL Server configuration file named `sql_server_config.cfg` under the infrastructure folder. That file is not included in this repository, so create it locally before running the pipeline.

Expected section name:

```ini
[ORDER_DDS]
DRIVER={ODBC Driver 17 for SQL Server}
SERVER=your_server_name
DATABASE=ORDER_DDS
Trusted_Connection=yes
```

Depending on your SQL Server setup, you may use username/password authentication instead of `Trusted_Connection`.

### Running

After dependencies and SQL Server configuration are ready:

```bash
python main.py
```

The pipeline currently uses the date range defined in `main.py`:

```python
start_date = "2023-01-01"
end_date = "2023-12-31"
```

### Important Note About Folder Names

Some Python imports and paths in the code refer to underscore-style package names such as `pipeline_dimensional_data` and `infrastructure_initiation`, while the current folders use spaces and title case. If you run this project directly, make sure the local folder names and imports are aligned for your operating system and Python environment.

## Technologies Used

- Power BI Desktop
- Python
- T-SQL
- Microsoft SQL Server
- `pyodbc`
- `pandas`
- `openpyxl`
- SQLAlchemy

## Learning Objectives Covered

These coursework projects demonstrate:

- Dimensional modeling for BI reporting.
- Fact and dimension table design.
- Surrogate and natural key usage.
- SQL Server warehouse initialization.
- Staging, SOR, and SCD concepts.
- Python-driven ETL execution.
- Database connectivity with ODBC.
- Power BI report development using a dimensional model.

## License

This repository is licensed under the [MIT License](./LICENSE).
