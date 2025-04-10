# Fetch Rewards – Analytics Engineer Assessment

## Project Overview

This project demonstrates my ability to extract, transform, and analyze unstructured JSON data to support data-driven decisions. It was developed as part of the Analytics Engineer assessment for Fetch Rewards.

The project showcases:
- A structured relational model designed from semi-structured JSON
- ETL using Python and SQL Server
- SQL queries that address key business questions
- Validation of data quality
- Business communication and documentation
-----------------------------------------------------------------

## Directory Structure
FetchRewards/ ├── Source/ → Raw JSON input files 
              ├── Scripts/ → ETL pipeline and validation scripts 
              ├── ER Diagram/ → Entity Relationship diagram (FetchERDiagram.png) 
              ├── Description/ → Written stakeholder communication (README.docx) 
              ├── README.md → This project overview


-------------------------------------------------------------------

## Tools & Technologies

| Tool            | Purpose                                 |
|-----------------|------------------------------------------|
| **Python**      | Data transformation, ETL, and validation |
| **Pandas**      | JSON normalization and DataFrame ops     |
| **SQL Server**  | Structured data warehouse                |
| **pyodbc**      | Python-to-SQL Server connection          |
| **Git & GitHub**| Version control & project delivery       |
| **Markdown**    | Documentation                            |

---------------------------------------------------------------------

## Relational Data Model

The data was modeled into the following SQL tables:

- `users(user_id, state, created_date, last_login, role, active)`
- `brands(brand_id, barcode, brand_code, category, category_code, cpg_id, top_brand, name)`
- `receipts(receipt_id, user_id, bonus_points_earned, bonus_reason, create_date, scanned_date, finished_date, modify_date, points_awarded_date, points_earned, purchase_date, item_count, status, total_spent)`
- `items(item_id, receipt_id, barcode, description, item_price, final_price, quantity_purchased)`

A detailed ER diagram is provided in `ER Diagram/FetchERDiagram.png`.

-------------------------------------------------------------------------

## Key Deliverables

### 1️ ETL & Data Normalization
- Python scripts normalize nested JSON
- Converts MongoDB-style epochs to proper dates
- Inserts clean data into SQL Server

### 2️ Business Questions Answered
- Top 5 brands by receipts scanned (this month and MoM comparison)
- Average spend and total items purchased for “Accepted” vs “Rejected” statuses
- Most spending brand among newly created users

### 3️ Data Quality Checks
- Identified:
  - Nulls and missing fields
  - Items with "ITEM NOT FOUND"
  - Inconsistent datatypes and missing joins

### 4️ Stakeholder Communication
- Written summary provided under `Description/README.docx` including:
  - Observed data issues
  - Clarifying questions
  - Next-step suggestions for scaling and validation

-----------------------------------------------------------------------

## SQL Dialect

All SQL queries use **T-SQL (Microsoft SQL Server)** syntax.

------------------------------------------------------------------------

## How to Run Locally

1. Ensure SQL Server is running and create a DB named `FetchADB`
2. Run `Scripts/DataTransform.py` to perform ETL and load tables
3. Run `Scripts/validationscript.py` to validate structure and queries

---------------------------------------------------------------------------

## Submission

All source files, documentation, and visualizations are included in this repository.  
If you have any questions, I'm happy to walk through the work in a follow-up discussion.

----------------------------------------------------------------------------

**Prepared by:**  
Sreeja N


