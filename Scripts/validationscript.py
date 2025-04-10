import pyodbc
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler()]
)

def validate_tables(conn):
    logging.info(" Validating expected tables and columns...")

    expected = dict(
        users=["user_id", "state", "created_date", "last_login", "role", "active"],
        brands=["brand_id", "barcode", "brand_code", "category", "category_code", "cpg_id", "top_brand", "name"],
        receipts=["receipt_id", "user_id", "bonus_points_earned", "bonus_reason", "create_date",
                  "scanned_date", "finished_date", "modify_date", "points_awarded_date", "points_earned",
                  "purchase_date", "item_count", "status", "total_spent"],
        items=["item_id", "receipt_id", "barcode", "description", "item_price", "final_price", "quantity_purchased"]
    )

    for table, expected_columns in expected.items():
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table}'
            """)
            actual_columns = [row[0] for row in cursor.fetchall()]
            missing = [col for col in expected_columns if col not in actual_columns]

            if missing:
                logging.error(f" Table '{table}' is missing columns: {missing}")
            else:
                logging.info(f" Table '{table}' has all required columns.")
        except Exception as e:
            logging.error(f" Error validating table '{table}': {e}")

def run_query(conn, description, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        if rows:
            logging.info(f" {description}: {len(rows)} row(s) found.")
            for r in rows:
                print(r)
        else:
            logging.warning(f" {description}: No results found.")
    except Exception as e:
        logging.error(f" {description}: Failed to execute. Reason: {e}")

def validate_queries(conn):
    logging.info(" Validating stakeholder queries...\n")

    queries = [
        ("Top 5 brands by receipts scanned this month", """
            SELECT TOP 5 b.name
            FROM receipts r
            JOIN items i ON r.receipt_id = i.receipt_id
            JOIN brands b ON i.barcode = b.barcode
            GROUP BY b.name
            ORDER BY COUNT(DISTINCT r.receipt_id) DESC;
        """),
        ("Compare top 5 brands this month vs last month", """
            WITH ranked_brands AS (
                SELECT b.name AS brand_name,
                       FORMAT(r.purchase_date, 'yyyy-MM') AS month,
                       COUNT(DISTINCT r.receipt_id) AS receipt_count,
                       RANK() OVER (PARTITION BY FORMAT(r.purchase_date, 'yyyy-MM')
                           ORDER BY COUNT(DISTINCT r.receipt_id) DESC) AS brand_rank
                FROM receipts r
                JOIN items i ON r.receipt_id = i.receipt_id
                JOIN brands b ON i.barcode = b.barcode
                GROUP BY b.name, FORMAT(r.purchase_date, 'yyyy-MM')
            )
            SELECT * FROM ranked_brands
            WHERE brand_rank <= 10
            ORDER BY month, brand_rank;
        """),
        ("Avg spend: Accepted vs Rejected", """
            SELECT UPPER(status), AVG(total_spent)
            FROM receipts
            WHERE UPPER(status) IN ('ACCEPTED', 'REJECTED')
            GROUP BY UPPER(status);
        """),
        ("Total items purchased: Accepted vs Rejected", """
            SELECT UPPER(r.status), SUM(i.quantity_purchased)
            FROM receipts r
            JOIN items i ON r.receipt_id = i.receipt_id
            WHERE UPPER(r.status) IN ('ACCEPTED', 'REJECTED')
            GROUP BY UPPER(r.status);
        """),
        ("Brand with most spend by new users", """
            SELECT TOP 1 b.name, SUM(i.final_price * i.quantity_purchased) AS total_spend
            FROM users u
            JOIN receipts r ON u.user_id = r.user_id
            JOIN items i ON r.receipt_id = i.receipt_id
            JOIN brands b ON i.barcode = b.barcode
            GROUP BY b.name
            ORDER BY total_spend DESC;
        """)
    ]

    for desc, sql in queries:
        run_query(conn, desc, sql)

if __name__ == "__main__":
    logging.info(" Starting validation script...")

    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=DESKTOP-AP03CV9;"
        "DATABASE=FetchADB;"
        "Trusted_Connection=yes;"
    )

    try:
        conn = pyodbc.connect(conn_str)
        logging.info(" Connected to SQL Server.")

        validate_tables(conn)
        validate_queries(conn)

        logging.info(" Validation complete.")
        conn.close()
    except Exception as e:
        logging.error(f" Could not connect to database: {e}")
