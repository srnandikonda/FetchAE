import json
import pandas as pd
import pyodbc
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler()]
)

class FetchRewardsETL:
    def __init__(self, conn_str=None):
        if conn_str is None:
            conn_str = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=DESKTOP-AP03CV9;"
                "DATABASE=FetchADB;"
                "Trusted_Connection=yes;"
            )
        logging.info("Initializing SQL Server connection.")
        self.conn = pyodbc.connect(conn_str)
        self.cursor = self.conn.cursor()

    def load_json(self, path: str) -> List[Dict[str, Any]]:
        logging.info(f"Loading JSON data from {path}")
        with open(path, 'r', encoding='utf-8') as f:
            return [json.loads(line) for line in f]

    def _convert_epoch(self, epoch: int) -> str:
        return datetime.utcfromtimestamp(epoch / 1000.0).strftime('%Y-%m-%d') if epoch else None

    def normalize_users(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        logging.info("Normalizing users data.")
        records = []
        for record in data:
            try:
                user_id = record.get('_id', {}).get('$oid')
                state = record.get('state')
                role = record.get('role', '').upper()
                active = record.get('active', False)
                created_epoch = record.get('createdDate', {}).get('$date')
                created_date = self._convert_epoch(created_epoch)
                last_login_epoch = record.get('lastLogin', {}).get('$date')
                last_login = self._convert_epoch(last_login_epoch)
                records.append({
                    'user_id': user_id,
                    'state': state,
                    'created_date': created_date,
                    'last_login': last_login,
                    'role': role,
                    'active': active
                })
            except Exception as e:
                logging.warning(f"Skipping user record: {e}")
        df = pd.DataFrame(records).drop_duplicates(subset='user_id')
        return df

    def normalize_brands(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        logging.info("Normalizing brands data.")
        records = []
        for record in data:
            try:
                brand_id = record.get('_id', {}).get('$oid')
                barcode = record.get('barcode')
                brandCode = record.get('brandCode')
                category = record.get('category')
                categoryCode = record.get('categoryCode')
                cpg_id = record.get('cpg', {}).get('$id', {}).get('$oid')
                top_brand = record.get('topBrand', False)
                name = record.get('name')
                records.append({
                    'brand_id': brand_id,
                    'barcode': barcode,
                    'brand_code': brandCode,
                    'category': category,
                    'category_code': categoryCode,
                    'cpg_id': cpg_id,
                    'top_brand': top_brand,
                    'name': name
                })
            except Exception as e:
                logging.warning(f"Skipping brand record: {e}")
        return pd.DataFrame(records).drop_duplicates(subset='brand_id')

    def normalize_receipts(self, data: List[Dict[str, Any]]) -> (pd.DataFrame, pd.DataFrame):
        logging.info("Normalizing receipts and items data.")
        receipts = []
        items = []

        for record in data:
            try:
                receipt_id = record['_id']['$oid']
                bonus_points = record.get('bonusPointsEarned')
                bonus_reason = record.get('bonusPointsEarnedReason')
                create_date = self._convert_epoch(record.get('createDate', {}).get('$date'))
                scanned_date = self._convert_epoch(record.get('dateScanned', {}).get('$date'))
                finished_date = self._convert_epoch(record.get('finishedDate', {}).get('$date'))
                modify_date = self._convert_epoch(record.get('modifyDate', {}).get('$date'))
                awarded_date = self._convert_epoch(record.get('pointsAwardedDate', {}).get('$date'))
                points_earned = float(record.get('pointsEarned', 0.0))
                purchase_date = self._convert_epoch(record.get('purchaseDate', {}).get('$date'))
                item_count = record.get('purchasedItemCount', 0)
                status = record.get('rewardsReceiptStatus')
                total_spent = float(record.get('totalSpent', 0.0))
                user_id = record.get('userId')

                receipts.append({
                    'receipt_id': receipt_id,
                    'user_id': user_id,
                    'bonus_points_earned': bonus_points,
                    'bonus_reason': bonus_reason,
                    'create_date': create_date,
                    'scanned_date': scanned_date,
                    'finished_date': finished_date,
                    'modify_date': modify_date,
                    'points_awarded_date': awarded_date,
                    'points_earned': points_earned,
                    'purchase_date': purchase_date,
                    'item_count': item_count,
                    'status': status,
                    'total_spent': total_spent
                })

                for item in record.get('rewardsReceiptItemList', []):
                    items.append({
                        'item_id': str(uuid.uuid4()),
                        'receipt_id': receipt_id,
                        'barcode': item.get('barcode'),
                        'description': item.get('description', ''),
                        'item_price': float(item.get('itemPrice', 0.0)),
                        'final_price': float(item.get('finalPrice', 0.0)),
                        'quantity_purchased': int(item.get('quantityPurchased', 1))
                    })

            except Exception as e:
                logging.warning(f"Skipping receipt: {e}")

        return pd.DataFrame(receipts), pd.DataFrame(items)

    def create_tables(self):
        logging.info("Creating SQL Server tables...")
        self.cursor.execute("IF OBJECT_ID('items', 'U') IS NOT NULL DROP TABLE items;")
        self.cursor.execute("IF OBJECT_ID('receipts', 'U') IS NOT NULL DROP TABLE receipts;")
        self.cursor.execute("IF OBJECT_ID('brands', 'U') IS NOT NULL DROP TABLE brands;")
        self.cursor.execute("IF OBJECT_ID('users', 'U') IS NOT NULL DROP TABLE users;")

        self.cursor.execute("""
        CREATE TABLE users (
            user_id VARCHAR(50) PRIMARY KEY,
            state VARCHAR(10),
            created_date DATE,
            last_login DATE,
            role VARCHAR(20),
            active BIT
        );
        """)
        self.cursor.execute("""
        CREATE TABLE brands (
            brand_id VARCHAR(50) PRIMARY KEY,
            barcode VARCHAR(100),
            brand_code VARCHAR(100),
            category VARCHAR(100),
            category_code VARCHAR(100),
            cpg_id VARCHAR(50),
            top_brand BIT,
            name VARCHAR(255)
        );
        """)
        self.cursor.execute("""
        CREATE TABLE receipts (
            receipt_id VARCHAR(50) PRIMARY KEY,
            user_id VARCHAR(50),
            bonus_points_earned INT,
            bonus_reason VARCHAR(MAX),
            create_date DATE,
            scanned_date DATE,
            finished_date DATE,
            modify_date DATE,
            points_awarded_date DATE,
            points_earned FLOAT,
            purchase_date DATE,
            item_count INT,
            status VARCHAR(50),
            total_spent FLOAT
        );
        """)
        self.cursor.execute("""
        CREATE TABLE items (
            item_id VARCHAR(50) PRIMARY KEY,
            receipt_id VARCHAR(50),
            barcode VARCHAR(100),
            description VARCHAR(MAX),
            item_price FLOAT,
            final_price FLOAT,
            quantity_purchased INT
        );
        """)
        self.conn.commit()

    def insert_dataframe(self, df: pd.DataFrame, table: str):
        logging.info(f"Inserting into {table} ({len(df)} records).")
        if df.empty:
            logging.warning(f"No data to insert for table {table}")
            return

        # Clean up based on datatype
        for col in df.columns:
            if df[col].dtype in ['float64', 'int64']:
                df[col] = df[col].fillna(0)
            elif df[col].dtype == 'bool':
                df[col] = df[col].fillna(False).astype(bool)
            else:
                df[col] = df[col].where(pd.notnull(df[col]), None)  # use None for NULLs

        placeholders = ', '.join(['?'] * len(df.columns))
        columns = ', '.join(df.columns)
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        try:
            self.cursor.fast_executemany = True
            self.cursor.executemany(sql, df.values.tolist())
            self.conn.commit()
            logging.info(f"Data inserted into {table} successfully.")
        except Exception as e:
            logging.error(f"Insert failed for table {table}. Error: {e}")
            logging.debug(f"Sample row: {df.head(1).to_dict()}")

    def data_quality_check(self):
        logging.info("Running data quality checks...")
        self.cursor.execute("""
            SELECT receipt_id, COUNT(*) AS issue_count
            FROM items
            WHERE barcode IS NULL OR barcode = ''
               OR final_price IS NULL
               OR quantity_purchased IS NULL
            GROUP BY receipt_id;
        """)
        issues = self.cursor.fetchall()
        if issues:
            logging.warning("Data Quality Issues Detected:")
            for issue in issues:
                logging.warning(f"Receipt {issue[0]} has {issue[1]} bad item(s)")
        else:
            logging.info("No data quality issues found.")

    def run_stakeholder_queries(self):
        logging.info("Running stakeholder queries...")

        logging.info("1. Top 5 brands by receipts scanned this month:")
        self.cursor.execute(f"""
            SELECT TOP 5 b.name, COUNT(DISTINCT r.receipt_id) AS receipt_count
            FROM receipts r
            JOIN items i ON r.receipt_id = i.receipt_id
            JOIN brands b ON i.barcode = b.barcode
            GROUP BY b.name
            ORDER BY receipt_count DESC;
        """)
        print("1. Top 5 brands by receipts scanned this month:")
        for row in self.cursor.fetchall():
            print(f"{row[0]} — {row[1]} receipts")

        logging.info("2. Comparing top brands between months:")
        self.cursor.execute(f"""
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
        """)
        print("2. Compare top 5 brands (this month vs previous):")
        for row in self.cursor.fetchall():
            print(row)

        logging.info("3. Average spend by status:")
        self.cursor.execute("""
            SELECT UPPER(status), AVG(total_spent)
            FROM receipts
            WHERE UPPER(status) IN ('ACCEPTED', 'REJECTED')
            GROUP BY UPPER(status);
        """)
        print("3. Average spend: Accepted vs Rejected:")
        for row in self.cursor.fetchall():
            print(f"{row[0]} — ${row[1]:.2f}")

        logging.info("4. Total items purchased by status:")
        self.cursor.execute("""
            SELECT UPPER(r.status), SUM(i.quantity_purchased)
            FROM receipts r
            JOIN items i ON r.receipt_id = i.receipt_id
            WHERE UPPER(r.status) IN ('ACCEPTED', 'REJECTED')
            GROUP BY UPPER(r.status);
        """)
        print("4. Total items purchased: Accepted vs Rejected:")
        for row in self.cursor.fetchall():
            print(f"{row[0]} — {row[1]} items")

        logging.info("5. Brand with most spend (new users):")
        self.cursor.execute("""
            SELECT TOP 1 b.name, SUM(i.final_price * i.quantity_purchased) AS total_spend
            FROM users u
            JOIN receipts r ON u.user_id = r.user_id
            JOIN items i ON r.receipt_id = i.receipt_id
            JOIN brands b ON i.barcode = b.barcode
            GROUP BY b.name
            ORDER BY total_spend DESC;
        """)
        row = self.cursor.fetchone()
        if row:
            print(f"5. Brand with most spend (new users last 6 months): {row[0]} — ${row[1]:.2f}")

        logging.info("6. Brand with most transactions (new users):")
        self.cursor.execute("""
            SELECT TOP 1 b.name, COUNT(DISTINCT r.receipt_id) AS txn_count
            FROM users u
            JOIN receipts r ON u.user_id = r.user_id
            JOIN items i ON r.receipt_id = i.receipt_id
            JOIN brands b ON i.barcode = b.barcode
            GROUP BY b.name
            ORDER BY txn_count DESC;
        """)
        row = self.cursor.fetchone()
        if row:
            print(f"6. Brand with most transactions (new users last 6 months): {row[0]} — {row[1]} transactions")

    def run_etl(self):
        try:

            base_path = 'C:\\Users\\Srijareddy\\PycharmProjects\\FetchRewards\\Source\\'

            users = self.normalize_users(self.load_json(base_path + 'users.json'))
            brands = self.normalize_brands(self.load_json(base_path + 'brands.json'))
            receipts, items = self.normalize_receipts(self.load_json(base_path + 'receipts.json'))
            self.create_tables()
            self.insert_dataframe(users, 'users')
            self.insert_dataframe(brands, 'brands')
            self.insert_dataframe(receipts, 'receipts')
            self.insert_dataframe(items, 'items')
            logging.info("ETL process completed successfully.")
        except Exception as e:
            logging.error(f"ETL process failed: {e}")

    def close(self):
        logging.info("Closing SQL Server connection.")
        self.conn.close()


if __name__ == '__main__':
    logging.info("Starting ETL pipeline...")
    etl = FetchRewardsETL()
    etl.run_etl()
    etl.data_quality_check()
    etl.run_stakeholder_queries()
    etl.close()
    logging.info("All done.")