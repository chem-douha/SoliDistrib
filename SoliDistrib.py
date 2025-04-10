# -*- coding: utf-8 -*-
import tkinter as tk
from tkinter import ttk, messagebox, Toplevel, filedialog
import fdb
import os
import logging
import threading
import pandas as pd
from datetime import datetime
import time
print("تم تحميل المكتبات بنجاح.")


# إعداد تسجيل الأخطاء
log_file_path = "g:/SoliDistrib/app_errors.log"
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
logging.basicConfig(
    filename=log_file_path,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

def connect_to_firebird(db_path):
    try:
        con = fdb.connect(dsn=db_path, user='SYSDBA', password='masterkey', charset='WIN1256')
        logging.info(f"Connected successfully to: {db_path}")
        return con
    except fdb.Error as e:
        error_message = str(e).lower()
        if "-902" in error_message and ("cannot find the file specified" in error_message or "database not found" in error_message):
            try:
                logging.info(f"Database not found, attempting to create: {db_path}")
                fdb.create_database(dsn=db_path, user='SYSDBA', password='masterkey', charset='WIN1256')
                con = fdb.connect(dsn=db_path, user='SYSDBA', password='masterkey', charset='WIN1256')
                logging.info(f"Created and connected to new database at: {db_path}")
                return con
            except fdb.Error as create_error:
                error_msg = (
                    f"فشل إنشاء قاعدة بيانات Firebird ({db_path}): {str(create_error)}\n"
                    f"تأكد من أن الدليل 'D:\\ORGA_SOFT\\data' لديه أذونات كتابة لمستخدم Firebird.\n"
                    f"تأكد أيضًا من أن خادم Firebird يعمل ويمكنه الوصول إلى المسار المحدد."
                )
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)
                return None
        else:
            error_msg = (
                f"فشل الاتصال بـ Firebird ({db_path}): {str(e)}\n"
                f"تأكد من أن خادم Firebird يعمل على '100.100.100.1' وأن المنفذ 3050 متاح.\n"
                f"تحقق أيضًا من إعدادات الشبكة وتأكد من أن المسار صحيح."
            )
            logging.error(error_msg)
            messagebox.showerror("خطأ", error_msg)
            return None

class DatabaseTab:
    def __init__(self, notebook, items_list_tab, bonus_tab,  cash_discount_tab, product_type_tab, tax_tab, suppliers_tab, agreement_discount_tab):
        self.frame = ttk.Frame(notebook)
        notebook.add(self.frame, text="قاعدة البيانات")
        self.connection_stores = None
        self.connection_main = None
        self.connection_dis = None
        self.items_list_tab = items_list_tab
        self.bonus_tab = bonus_tab
        self.cash_discount_tab = cash_discount_tab
        self.product_type_tab = product_type_tab
        self.tax_tab = tax_tab
        self.suppliers_tab = suppliers_tab
        self.agreement_discount_tab = agreement_discount_tab  # إضافة تبويب خصم الاتفاق

        db_frame = tk.Frame(self.frame, bg="#f0f0f0")
        db_frame.pack(pady=10, padx=10, fill='x')

        tk.Label(db_frame, text="مسار قاعدة بيانات المخازن:", font=('Arial', 12), bg="#f0f0f0").pack(pady=(0, 5))
        self.entry_db_path_stores = tk.Entry(db_frame, width=50, justify="right", font=('Arial', 12))
        self.entry_db_path_stores.pack(pady=(0, 10))
        self.entry_db_path_stores.insert(0, "100.100.100.1:D:\\ORGA_SOFT\\data\\ORGA_ORGA.GDB")

        tk.Label(db_frame, text="مسار قاعدة البيانات الرئيسية:", font=('Arial', 12), bg="#f0f0f0").pack(pady=(0, 5))
        self.entry_db_path_main = tk.Entry(db_frame, width=50, justify="right", font=('Arial', 12))
        self.entry_db_path_main.pack(pady=(0, 10))
        self.entry_db_path_main.insert(0, "100.100.100.1:D:\\ORGA_SOFT\\data\\ORGA.GDB")

        tk.Label(db_frame, text="مسار قاعدة بيانات dis_db:", font=('Arial', 12), bg="#f0f0f0").pack(pady=(0, 5))
        self.entry_db_path_dis = tk.Entry(db_frame, width=50, justify="right", font=('Arial', 12))
        self.entry_db_path_dis.pack(pady=(0, 10))
        self.entry_db_path_dis.insert(0, "100.100.100.1:D:\\ORGA_SOFT\\data\\DIST_DB")

        tk.Button(db_frame, text="اتصال", command=self.connect_and_test, bg="#4CAF50", fg="white", font=('Arial', 12)).pack(pady=5)
        tk.Button(db_frame, text="تحديث من Excel", command=self.update_from_excel, bg="#2196F3", fg="white", font=('Arial', 12)).pack(pady=5)

        ttk.Button(self.frame, text="مزامنة الأصناف", command=self.sync_additions).pack(pady=5)

        self.connect_automatically()

    

    def connect_automatically(self):
        logging.info("Starting automatic connection")
        progress_window = Toplevel(self.frame)
        progress_window.title("جارٍ الاتصال...")
        progress_window.geometry("300x100")
        ttk.Label(progress_window, text="جارٍ الاتصال بقواعد البيانات، الرجاء الانتظار...").pack(pady=20)
        progress_window.transient(self.frame)
        progress_window.grab_set()
        progress_bar = ttk.Progressbar(progress_window, mode='indeterminate')
        progress_bar.pack(pady=10)
        progress_bar.start()
        threading.Thread(target=self.connect_thread, args=(progress_window,), daemon=True).start()

    def connect_and_test(self):
        logging.info("Starting manual connection")
        progress_window = Toplevel(self.frame)
        progress_window.title("جارٍ الاتصال...")
        progress_window.geometry("300x100")
        progress_window.transient(self.frame)
        progress_window.grab_set()
        ttk.Label(progress_window, text="جارٍ الاتصال بقواعد البيانات، الرجاء الانتظار...").pack(pady=20)
        progress_bar = ttk.Progressbar(progress_window, mode='indeterminate')
        progress_bar.pack(pady=10)
        progress_bar.start()

    def connect_thread(self, dialog):
        import time
        start_time = time.time()
        try:
            db_path_stores = self.entry_db_path_stores.get()
            db_path_main = self.entry_db_path_main.get()
            db_path_dis = self.entry_db_path_dis.get()

            self.connection_stores = connect_to_firebird(db_path_stores)
            logging.info(f"Connected to stores DB in {time.time() - start_time:.2f} seconds")
            t1 = time.time()
            self.connection_main = connect_to_firebird(db_path_main)
            logging.info(f"Connected to main DB in {time.time() - t1:.2f} seconds")
            t2 = time.time()
            self.connection_dis = connect_to_firebird(db_path_dis)
            logging.info(f"Connected to dis DB in {time.time() - t2:.2f} seconds")

            if all([self.connection_stores, self.connection_main, self.connection_dis]):
                self.setup_main_db()
                self.setup_dis_db()
                self.test_database()
                self.load_stores()

                # إدخال البيانات الافتراضية أولاً
                self.bonus_tab.insert_bonus_data()
                self.cash_discount_tab.insert_cash_discount_data()
                self.product_type_tab.insert_product_type_data()
                self.tax_tab.insert_tax_data()
                self.suppliers_tab.insert_suppliers_data()
                self.agreement_discount_tab.insert_agreement_discount_data()

                # إغلاق نافذة الاتصال فورًا
                self.frame.after(0, lambda: [
                    dialog.destroy(),
                    messagebox.showinfo("نجاح", "تم الاتصال بجميع قواعد البيانات بنجاح!")
                ])

                # مزامنة الأصناف وتحميل البيانات في الخلفية
                self.frame.after(100, self.sync_additions_with_progress)
                self.frame.after(150, lambda: [
                    self.bonus_tab.load_bonus_data(),
                    
                    self.cash_discount_tab.load_cash_discount_data(),
                    self.product_type_tab.load_product_type_data(),
                    self.tax_tab.load_tax_data(),
                    self.suppliers_tab.load_suppliers_data(),
                    self.agreement_discount_tab.load_agreement_discount_data()
                ])
        except Exception as e:
            logging.error(f"Error in connect_thread: {str(e)}")
            self.frame.after(0, lambda: [
                dialog.destroy(),
                messagebox.showerror("خطأ", f"حدث خطأ أثناء الاتصال: {str(e)}")
            ])       


    def sync_additions_with_progress(self):
        """نسخة معدلة من sync_additions مع شريط تقدم"""
        _, connection_main, connection_dis = self.get_connections()
        if not all([connection_main, connection_dis]):
            logging.error("لا يمكن المزامنة: أحد الاتصالات مفقود!")
            return

        logging.info("Starting sync_additions_with_progress")  # تأكيد بدء الدالة
        progress_window = tk.Toplevel(self.frame)
        progress_window.title("جارٍ مزامنة الأصناف...")
        progress_window.geometry("300x100")
        ttk.Label(progress_window, text="جارٍ مزامنة الأصناف، الرجاء الانتظار...").pack(pady=20)
        progress_bar = ttk.Progressbar(progress_window, mode='determinate', maximum=27971)
        progress_bar.pack(pady=10)

        def sync_task():
            logging.info("Sync task thread started")  # تأكيد بدء الخيط
            cur_main = None
            cur_dis = None
            try:
                connection_dis.begin()
                cur_main = connection_main.cursor()
                cur_dis = connection_dis.cursor()

                cur_dis.execute("""
                    SELECT a.PROD_ID, a.SALE_DISCOUNT, a.MAIN_MARGIN, t.TAX_RATE
                    FROM ADDITIONS a
                    LEFT JOIN ITEM_TAXES it ON a.PROD_ID = it.PROD_ID
                    LEFT JOIN TAXES t ON it.TAX_ID = t.TAX_ID
                """)
                existing_additions = {
                    str(row[0]): {
                        'sale_discount': float(row[1] or 0.0),
                        'main_margin': float(row[2] or 0.0),
                        'tax_rate': float(row[3] or 0.0) / 100 if row[3] else 0.0
                    } for row in cur_dis.fetchall()
                }
                logging.info(f"Existing PROD_IDs in ADDITIONS: {len(existing_additions)} items")

                insert_query = """
                    INSERT INTO ADDITIONS (PROD_ID, COP_NAME, PROD_NAME, PRICE_1, TOTAL_QTY_ALL, DISCOUNT_COST, 
                                        TAX_QTY, SALE_PRICE, SALE_MARGIN, MAIN_MARGIN, SALE_DISCOUNT)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                update_query = """
                    UPDATE ADDITIONS
                    SET COP_NAME = ?, PROD_NAME = ?, PRICE_1 = ?, TOTAL_QTY_ALL = ?, DISCOUNT_COST = ?, 
                        TAX_QTY = ?, SALE_PRICE = ?, SALE_MARGIN = ?, MAIN_MARGIN = ?
                    WHERE PROD_ID = ?
                """

                inserted_count = 0
                updated_count = 0
                batch_size = 2000
                offset = 0

                while True:
                    main_query = f"""
                        SELECT p.PROD_ID, c.COP_NAME, p.PROD_NAME, p.PRICE_1, 
                            COALESCE(s.TOTAL_QTY_ALL, '0'), COALESCE(s.DISCOUNT_COST, '0')
                        FROM PRODUCTS p
                        LEFT JOIN COP_USERS c ON p.COP_ID = c.COP_ID
                        LEFT JOIN STOCK_STOCK s ON p.PROD_ID = s.PROD_ID
                        ROWS {offset + 1} TO {offset + batch_size}
                    """
                    cur_main.execute(main_query)
                    main_items = cur_main.fetchall()
                    if not main_items:
                        break

                    insert_values = []
                    update_values = []

                    for item in main_items:
                        prod_id, cop_name, prod_name, price_1, total_qty_all, discount_cost = item
                        prod_id_str = str(prod_id)
                        data = existing_additions.get(prod_id_str, {'sale_discount': 0.0, 'main_margin': 0.0, 'tax_rate': 0.0})
                        sale_discount = data['sale_discount'] / 100
                        main_margin = data['main_margin']
                        sale_margin = main_margin
                        tax_rate = data['tax_rate']

                        if tax_rate + 1 == 0:
                            sale_price = price_1 * (1 - sale_discount)
                        else:
                            sale_price = (price_1 * (1 - sale_discount)) / (1 + tax_rate)

                        tax_qty = sale_price * tax_rate
                        sale_price = round(sale_price, 2)
                        tax_qty = round(tax_qty, 2)

                        if prod_id_str not in existing_additions:
                            insert_values.append((prod_id_str, cop_name, prod_name, price_1, total_qty_all, discount_cost, 
                                                tax_qty, sale_price, sale_margin, main_margin, sale_discount * 100))
                        else:
                            update_values.append((cop_name, prod_name, price_1, total_qty_all, discount_cost, 
                                                tax_qty, sale_price, sale_margin, main_margin, prod_id_str))

                    if insert_values:
                        cur_dis.executemany(insert_query, insert_values)
                        inserted_count += len(insert_values)
                    if update_values:
                        cur_dis.executemany(update_query, update_values)
                        updated_count += len(update_values)

                    offset += batch_size
                    progress_bar['value'] = offset
                    progress_window.update()
                    logging.info(f"Processed {offset} items so far...")

                connection_dis.commit()
                logging.info(f"تمت المزامنة بنجاح!\nتم إدراج {inserted_count} صنف وتحديث {updated_count} صنف.")
                progress_window.destroy()
                messagebox.showinfo("نجاح", f"اكتملت المزامنة!\nتم إدراج {inserted_count} صنف وتحديث {updated_count} صنف.")

                # تحميل البيانات بعد المزامنة
                self.bonus_tab.load_bonus_data()
                self.items_list_tab.load_table()  # استبدال items_with_bonuses_tab بـ items_list_tab
                self.cash_discount_tab.load_cash_discount_data()
                self.product_type_tab.load_product_type_data()
                self.tax_tab.load_tax_data()
                self.suppliers_tab.load_suppliers_data()
                self.agreement_discount_tab.load_agreement_discount_data()

            except fdb.Error as e:
                connection_dis.rollback()
                logging.error(f"فشل مزامنة الأصناف: {str(e)}")
                progress_window.destroy()
                messagebox.showerror("خطأ", f"فشل المزامنة: {str(e)}")
            except Exception as e:
                connection_dis.rollback()
                logging.error(f"خطأ غير متوقع في sync_task: {str(e)}")
                progress_window.destroy()
                messagebox.showerror("خطأ", f"خطأ غير متوقع: {str(e)}")
            finally:
                if cur_main is not None:
                    cur_main.close()
                if cur_dis is not None:
                    cur_dis.close()
                logging.info("Sync task completed")

        threading.Thread(target=sync_task, daemon=True).start()
        logging.info("Sync thread launched")

    def update_from_excel(self):
        filepath = filedialog.askopenfilename(
            title="اختر ملف Excel",
            filetypes=[("Excel files", "*.xlsx *.xls")]
        )
        if not filepath:
            return

        try:
            df = pd.read_excel(filepath)
            required_columns = ["PROD_ID", "tax_rate"]
            if not all(col in df.columns for col in required_columns):
                messagebox.showerror("خطأ", "الملف يجب أن يحتوي على أعمدة 'PROD_ID' و 'tax_rate'")
                return

            _, _, connection_dis = self.get_connections()
            if not connection_dis:
                messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
                return

            cur = connection_dis.cursor()
            cur.execute("SELECT TAX_ID, TAX_RATE FROM TAXES")
            tax_mapping = {row[1]: row[0] for row in cur.fetchall()}
            cur.execute("SELECT PROD_ID FROM ADDITIONS")
            valid_prod_ids = {row[0] for row in cur.fetchall()}

            updated_count = 0
            skipped_count = 0
            for _, row in df.iterrows():
                prod_id = row["PROD_ID"]
                tax_rate = row["tax_rate"]
                if prod_id not in valid_prod_ids:
                    logging.warning(f"PROD_ID {prod_id} غير موجود في جدول ADDITIONS، سيتم تخطيه")
                    skipped_count += 1
                    continue
                tax_id = tax_mapping.get(tax_rate)
                if tax_id is None:
                    logging.warning(f"Tax rate {tax_rate} غير موجود في جدول TAXES، سيتم تخطيه لـ PROD_ID: {prod_id}")
                    skipped_count += 1
                    continue
                cur.execute("DELETE FROM ITEM_TAXES WHERE PROD_ID = ?", ꉂ(prod_id,))
                cur.execute("INSERT INTO ITEM_TAXES (PROD_ID, TAX_ID) VALUES (?, ?)", (prod_id, tax_id))
                updated_count += 1

            connection_dis.commit()
            logging.info(f"تم تحديث {updated_count} صنف من ملف Excel: {filepath}")
            logging.info(f"تم تخطي {skipped_count} صنف بسبب بيانات غير صالحة")
            messagebox.showinfo("نجاح", f"تم تحديث {updated_count} صنف بنجاح!\nتم تخطي {skipped_count} صنف.")
            self.items_with_bonuses_tab.load_items_with_bonuses()

        except Exception as e:
            connection_dis.rollback()
            error_msg = f"فشل تحديث البيانات من Excel: {str(e)}"
            logging.error(error_msg)
            messagebox.showerror("خطأ", error_msg)
        finally:
            if 'cur' in locals():
                cur.close()

    def get_connections(self):
        """إرجاع اتصالات قواعد البيانات الثلاثة."""
        return self.connection_stores, self.connection_main, self.connection_dis

    def setup_main_db(self):
        try:
            cur = self.connection_main.cursor()
            cur.execute("SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'ADDITIONS'")
            exists = cur.fetchone()
            if not exists:
                cur.execute("""
                    CREATE TABLE ADDITIONS (
                        PROD_ID VARCHAR(50) PRIMARY KEY,
                        QTY DECIMAL(10,2)
                    )
                """)
                logging.info("Created table ADDITIONS in main_db")
            else:
                logging.info("Table ADDITIONS already exists in main_db, skipping creation")
            self.connection_main.commit()
            cur.close()
        except fdb.Error as e:
            logging.error(f"Error setting up ADDITIONS table in main_db: {str(e)}")
            self.connection_main.rollback()
            messagebox.showerror("خطأ", f"فشل إعداد جدول ADDITIONS في قاعدة البيانات الرئيسية: {str(e)}")

    def setup_dis_db(self):
        try:
            cur = self.connection_dis.cursor()
            tables = {
                "ADDITIONS": """
                    CREATE TABLE ADDITIONS (
                        PROD_ID VARCHAR(50) PRIMARY KEY,
                        PROD_NAME VARCHAR(100),
                        PRICE DECIMAL(10,2),
                        TAX_QTY DECIMAL(10,2),
                        SALE_PRICE DECIMAL(10,2),
                        SALE_MARGIN DECIMAL(10,2),
                        MAIN_MARGIN DECIMAL(10,2),
                        SALE_DISCOUNT DECIMAL(10,2),
                        COP_NAME VARCHAR(100),
                        PRICE_1 DECIMAL(10,2),
                        TOTAL_QTY_ALL DECIMAL(10,2),
                        DISCOUNT_COST DECIMAL(10,2)
                    )
                """,
                "SUPPLIERS": """
                    CREATE TABLE SUPPLIERS (
                        SUPPLIER_ID VARCHAR(50) PRIMARY KEY,
                        SUPPLIER_NAME VARCHAR(100),
                        CASH_DISCOUNT_ID VARCHAR(50),
                        AGREEMENT_DISCOUNT_ID VARCHAR(50),
                        FOREIGN KEY (CASH_DISCOUNT_ID) REFERENCES CASH_DISCOUNTS(CASH_DISCOUNT_ID),
                        FOREIGN KEY (AGREEMENT_DISCOUNT_ID) REFERENCES AGREEMENT_DISCOUNTS(AGREEMENT_DISCOUNT_ID)
                    )
                """,
                "CASH_DISCOUNTS": """
                    CREATE TABLE CASH_DISCOUNTS (
                        CASH_DISCOUNT_ID VARCHAR(50) PRIMARY KEY,
                        CASH_DISCOUNT_NAME VARCHAR(100)
                    )
                """,
                "SUPPLIER_CASH_DISCOUNTS": """
                    CREATE TABLE SUPPLIER_CASH_DISCOUNTS (
                        SUPPLIER_ID VARCHAR(50),
                        CASH_DISCOUNT_ID VARCHAR(50),
                        DISCOUNT_RATE DECIMAL(10,2),
                        PRIMARY KEY (SUPPLIER_ID, CASH_DISCOUNT_ID),
                        FOREIGN KEY (SUPPLIER_ID) REFERENCES SUPPLIERS(SUPPLIER_ID),
                        FOREIGN KEY (CASH_DISCOUNT_ID) REFERENCES CASH_DISCOUNTS(CASH_DISCOUNT_ID)
                    )
                """,
                "BONUSES": """
                    CREATE TABLE BONUSES (
                        BONUS_ID INTEGER NOT NULL PRIMARY KEY,
                        BASE_QTY INTEGER NOT NULL,
                        BONUS_QTY INTEGER NOT NULL,
                        DISCOUNT_RATE DECIMAL(10,6)
                    )
                """,
                "PRODUCT_BONUS_LINK": """
                    CREATE TABLE PRODUCT_BONUS_LINK (
                        PROD_ID VARCHAR(50),
                        BONUS_ID INTEGER,
                        PRIMARY KEY (PROD_ID, BONUS_ID),
                        FOREIGN KEY (PROD_ID) REFERENCES ADDITIONS(PROD_ID),
                        FOREIGN KEY (BONUS_ID) REFERENCES BONUSES(BONUS_ID)
                    )
                """,
                "PRODUCT_TYPES": """
                    CREATE TABLE PRODUCT_TYPES (
                        PRODUCT_TYPE_ID VARCHAR(50) PRIMARY KEY,
                        PRODUCT_TYPE_NAME VARCHAR(100)
                    )
                """,
                "TAXES": """
                    CREATE TABLE TAXES (
                        TAX_ID VARCHAR(50) PRIMARY KEY,
                        TAX_RATE DECIMAL(10,2)
                    )
                """,
                "ITEM_CASH_DISCOUNTS": """
                    CREATE TABLE ITEM_CASH_DISCOUNTS (
                        PROD_ID VARCHAR(50) NOT NULL,
                        CASH_DISCOUNT_ID VARCHAR(50) NOT NULL,
                        PRIMARY KEY (PROD_ID, CASH_DISCOUNT_ID),
                        FOREIGN KEY (PROD_ID) REFERENCES ADDITIONS(PROD_ID),
                        FOREIGN KEY (CASH_DISCOUNT_ID) REFERENCES CASH_DISCOUNTS(CASH_DISCOUNT_ID)
                    )
                """,
                "ITEM_PRODUCT_TYPES": """
                    CREATE TABLE ITEM_PRODUCT_TYPES (
                        PROD_ID VARCHAR(50) NOT NULL,
                        PRODUCT_TYPE_ID VARCHAR(50) NOT NULL,
                        PRIMARY KEY (PROD_ID, PRODUCT_TYPE_ID),
                        FOREIGN KEY (PROD_ID) REFERENCES ADDITIONS(PROD_ID),
                        FOREIGN KEY (PRODUCT_TYPE_ID) REFERENCES PRODUCT_TYPES(PRODUCT_TYPE_ID)
                    )
                """,
                "ITEM_TAXES": """
                    CREATE TABLE ITEM_TAXES (
                        PROD_ID VARCHAR(50) NOT NULL,
                        TAX_ID VARCHAR(50) NOT NULL,
                        PRIMARY KEY (PROD_ID, TAX_ID),
                        FOREIGN KEY (PROD_ID) REFERENCES ADDITIONS(PROD_ID),
                        FOREIGN KEY (TAX_ID) REFERENCES TAXES(TAX_ID)
                    )
                """,
                "AGREEMENT_DISCOUNTS": """
                    CREATE TABLE AGREEMENT_DISCOUNTS (
                        AGREEMENT_DISCOUNT_ID VARCHAR(50) PRIMARY KEY,
                        DISCOUNT_RATE DECIMAL(10,2)
                    )
                """
            }

            # إنشاء الجداول إذا لم تكن موجودة
            for table_name, create_stmt in tables.items():
                try:
                    cur.execute(f"SELECT 1 FROM {table_name} WHERE 1=0")
                    logging.info(f"Table {table_name} already exists in dis_db, skipping creation")
                except fdb.Error:
                    cur.execute(create_stmt)
                    self.connection_dis.commit()
                    logging.info(f"Created table {table_name} in dis_db")

            # دالة مساعدة للتحقق من وجود عمود في جدول
            def column_exists(table, column):
                cur.execute("""
                    SELECT 1
                    FROM RDB$RELATION_FIELDS
                    WHERE RDB$RELATION_NAME = ? AND RDB$FIELD_NAME = ?
                """, (table.upper(), column.upper()))
                return cur.fetchone() is not None

            # تحديث جدول SUPPLIERS
            if not column_exists("SUPPLIERS", "CASH_DISCOUNT_ID"):
                cur.execute("ALTER TABLE SUPPLIERS ADD CASH_DISCOUNT_ID VARCHAR(50)")
                cur.execute("ALTER TABLE SUPPLIERS ADD FOREIGN KEY (CASH_DISCOUNT_ID) REFERENCES CASH_DISCOUNTS(CASH_DISCOUNT_ID)")
                self.connection_dis.commit()
                logging.info("Added column CASH_DISCOUNT_ID to SUPPLIERS")
            else:
                logging.info("Column CASH_DISCOUNT_ID already exists in SUPPLIERS")

            if not column_exists("SUPPLIERS", "AGREEMENT_DISCOUNT_ID"):
                cur.execute("ALTER TABLE SUPPLIERS ADD AGREEMENT_DISCOUNT_ID VARCHAR(50)")
                cur.execute("ALTER TABLE SUPPLIERS ADD FOREIGN KEY (AGREEMENT_DISCOUNT_ID) REFERENCES AGREEMENT_DISCOUNTS(AGREEMENT_DISCOUNT_ID)")
                self.connection_dis.commit()
                logging.info("Added column AGREEMENT_DISCOUNT_ID to SUPPLIERS")
            else:
                logging.info("Column AGREEMENT_DISCOUNT_ID already exists in SUPPLIERS")

            # تحديث جدول ADDITIONS للأعمدة الإضافية
            new_columns = {
                "COP_NAME": "VARCHAR(100)",
                "PRICE_1": "DECIMAL(10,2)",
                "TOTAL_QTY_ALL": "DECIMAL(10,2)",
                "DISCOUNT_COST": "DECIMAL(10,2)",
                "PHARMA_CODE" : "VARCHAR(50)"
            }
            for col_name, col_type in new_columns.items():
                if not column_exists("ADDITIONS", col_name):
                    cur.execute(f"ALTER TABLE ADDITIONS ADD {col_name} {col_type}")
                    self.connection_dis.commit()
                    logging.info(f"Added column {col_name} to ADDITIONS")
                else:
                    logging.info(f"Column {col_name} already exists in ADDITIONS")

            self.connection_dis.commit()
            logging.info("Successfully set up dis_db")

        except fdb.Error as e:
            self.connection_dis.rollback()
            logging.error(f"Failed to set up dis_db: {str(e)}")
            messagebox.showerror("خطأ", f"فشل إعداد قاعدة البيانات dis_db: {str(e)}")
        finally:
            if cur is not None:
                cur.close()

    def sync_additions(self):
        _, connection_main, connection_dis = self.get_connections()
        if not all([connection_main, connection_dis]):
            logging.error("لا يمكن المزامنة: أحد الاتصالات مفقود!")
            return

        try:
            connection_dis.begin()
            cur_main = connection_main.cursor()
            cur_dis = connection_dis.cursor()

            # استعلام موحد لجلب كل البيانات المطلوبة من dis_db
            cur_dis.execute("""
                SELECT a.PROD_ID, a.SALE_DISCOUNT, a.MAIN_MARGIN, t.TAX_RATE
                FROM ADDITIONS a
                LEFT JOIN ITEM_TAXES it ON a.PROD_ID = it.PROD_ID
                LEFT JOIN TAXES t ON it.TAX_ID = t.TAX_ID
            """)
            existing_additions = {
                str(row[0]): {
                    'sale_discount': float(row[1] or 0.0),
                    'main_margin': float(row[2] or 0.0),
                    'tax_rate': float(row[3] or 0.0) / 100 if row[3] else 0.0
                } for row in cur_dis.fetchall()
            }
            logging.info(f"Existing PROD_IDs in ADDITIONS: {len(existing_additions)} items")

            insert_query = """
                INSERT INTO ADDITIONS (PROD_ID, COP_NAME, PROD_NAME, PRICE_1, TOTAL_QTY_ALL, DISCOUNT_COST, 
                                    TAX_QTY, SALE_PRICE, SALE_MARGIN, MAIN_MARGIN, SALE_DISCOUNT)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            update_query = """
                UPDATE ADDITIONS
                SET COP_NAME = ?, PROD_NAME = ?, PRICE_1 = ?, TOTAL_QTY_ALL = ?, DISCOUNT_COST = ?, 
                    TAX_QTY = ?, SALE_PRICE = ?, SALE_MARGIN = ?, MAIN_MARGIN = ?
                WHERE PROD_ID = ?
            """

            inserted_count = 0
            updated_count = 0
            batch_size = 500
            offset = 0

            while True:
                main_query = f"""
                    SELECT p.PROD_ID, c.COP_NAME, p.PROD_NAME, p.PRICE_1, 
                        COALESCE(s.TOTAL_QTY_ALL, '0'), COALESCE(s.DISCOUNT_COST, '0')
                    FROM PRODUCTS p
                    LEFT JOIN COP_USERS c ON p.COP_ID = c.COP_ID
                    LEFT JOIN STOCK_STOCK s ON p.PROD_ID = s.PROD_ID
                    ROWS {offset + 1} TO {offset + batch_size}
                """
                cur_main.execute(main_query)
                main_items = cur_main.fetchall()
                if not main_items:
                    break

                insert_values = []
                update_values = []

                for item in main_items:
                    prod_id, cop_name, prod_name, price_1, total_qty_all, discount_cost = item
                    prod_id_str = str(prod_id)

                    data = existing_additions.get(prod_id_str, {'sale_discount': 0.0, 'main_margin': 0.0, 'tax_rate': 0.0})
                    sale_discount = data['sale_discount'] / 100
                    main_margin = data['main_margin']
                    sale_margin = main_margin
                    tax_rate = data['tax_rate']

                    if tax_rate + 1 == 0:
                        sale_price = price_1 * (1 - sale_discount)
                    else:
                        sale_price = (price_1 * (1 - sale_discount)) / (1 + tax_rate)

                    tax_qty = sale_price * tax_rate
                    sale_price = round(sale_price, 2)
                    tax_qty = round(tax_qty, 2)

                    if prod_id_str not in existing_additions:
                        insert_values.append((prod_id_str, cop_name, prod_name, price_1, total_qty_all, discount_cost, 
                                            tax_qty, sale_price, sale_margin, main_margin, sale_discount * 100))
                    else:
                        update_values.append((cop_name, prod_name, price_1, total_qty_all, discount_cost, 
                                            tax_qty, sale_price, sale_margin, main_margin, prod_id_str))

                if insert_values:
                    cur_dis.executemany(insert_query, insert_values)
                    inserted_count += len(insert_values)
                if update_values:
                    cur_dis.executemany(update_query, update_values)
                    updated_count += len(update_values)

                offset += batch_size
                logging.info(f"Processed {offset} items so far...")

            connection_dis.commit()
            logging.info(f"تمت المزامنة بنجاح!\nتم إدراج {inserted_count} صنف وتحديث {updated_count} صنف.")

        except fdb.Error as e:
            connection_dis.rollback()
            logging.error(f"فشل مزامنة الأصناف: {str(e)}")
        finally:
            cur_main.close()
            cur_dis.close()

    def test_database(self):
        """اختبار الاتصال بجميع قواعد البيانات."""
        connections = [self.connection_stores, self.connection_main, self.connection_dis]
        for conn, name in zip(connections, ["المخازن", "الرئيسية", "dis_db"]):
            if conn is not None:
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE")
                    result = cur.fetchone()
                    logging.info(f"اتصال قاعدة بيانات {name} ناجح: {result}")
                    cur.close()
                except fdb.Error as e:
                    logging.error(f"فشل اختبار قاعدة بيانات {name}: {str(e)}")
            else:
                logging.warning(f"قاعدة بيانات {name} غير متصلة")      

    def load_stores(self):
        """تحميل قائمة المخازن"""
        try:
            cur = self.connection_main.cursor()
            query = """
                SELECT DISTINCT s.STORE_ID, d.DATA_NAME
                FROM STOCK_STOCK s
                LEFT JOIN DATA_COMP d ON s.STORE_ID = d.DATA_ID
                WHERE s.STORE_ID != 0
            """
            cur.execute(query)
            stores = [(str(row[0]), row[1] or f"مخزن #{str(row[0])}") for row in cur.fetchall()]
            self.items_list_tab.update_stores(stores)
            cur.close()
        except Exception as e:
            logging.error(f"Error loading stores: {str(e)}")
            messagebox.showerror("خطأ", f"فشل تحميل قائمة المخازن: {str(e)}")

    def close_connections(self):
        for conn in [self.connection_stores, self.connection_main, self.connection_dis]:
            if conn:
                conn.close()
class AddEditTab:
    def __init__(self, notebook, get_connections):
        self.frame = ttk.Frame(notebook)
        notebook.add(self.frame, text="إضافة وتعديل")
        self.get_connections = get_connections

        # إنشاء Notebook داخلي للتبويبات الفرعية
        self.inner_notebook = ttk.Notebook(self.frame)
        self.inner_notebook.pack(fill='both', expand=True, padx=10, pady=10)

        # إضافة التبويبات الفرعية
        self.bonus_tab = BonusTab(self.inner_notebook, self.get_connections)
        self.cash_discount_tab = CashDiscountTab(self.inner_notebook, self.get_connections)
        self.product_type_tab = ProductTypeTab(self.inner_notebook, self.get_connections)
        self.tax_tab = TaxTab(self.inner_notebook, self.get_connections)
        self.suppliers_tab = SuppliersTab(self.inner_notebook, self.get_connections)
        self.agreement_discount_tab = AgreementDiscountTab(self.inner_notebook, self.get_connections)
  
        

class BonusTab:
    def __init__(self, notebook, get_connections):
        self.frame = ttk.Frame(notebook)
        notebook.add(self.frame, text="بونص")
        self.get_connections = get_connections
        self.current_data = []
        self.sort_column = None
        self.sort_reverse = False

        self.style = ttk.Style()
        self.style.theme_use('clam')
        self.style.configure("Add.TButton", background="#005f99", foreground="white", font=('Segoe UI', 11))
        self.style.map("Add.TButton", background=[('active', '#004d80'), ('pressed', '#003d66')], foreground=[('active', 'white'), ('pressed', 'white')])
        self.style.configure("Search.TButton", background="#005f99", foreground="white", font=('Segoe UI', 11))
        self.style.map("Search.TButton", background=[('active', '#004d80'), ('pressed', '#003d66')], foreground=[('active', 'white'), ('pressed', 'white')])
        self.style.configure("Edit.TButton", background="#005f99", foreground="white", font=('Segoe UI', 11))
        self.style.map("Edit.TButton", background=[('active', '#004d80'), ('pressed', '#003d66')], foreground=[('active', 'white'), ('pressed', 'white')])
        self.style.configure("Reload.TButton", background="#005f99", foreground="white", font=('Segoe UI', 11))
        self.style.map("Reload.TButton", background=[('active', '#004d80'), ('pressed', '#003d66')], foreground=[('active', 'white'), ('pressed', 'white')])
        self.style.configure("Save.TButton", background="#005f99", foreground="white", font=('Segoe UI', 11))
        self.style.map("Save.TButton", background=[('active', '#004d80'), ('pressed', '#003d66')], foreground=[('active', 'white'), ('pressed', 'white')])
        self.style.configure("Cancel.TButton", background="#ff4d4d", foreground="white", font=('Segoe UI', 11))
        self.style.map("Cancel.TButton", background=[('active', '#cc0000'), ('pressed', '#990000')], foreground=[('active', 'white'), ('pressed', 'white')])
        self.style.configure("Delete.TButton", background="#cc0000", foreground="white", font=('Segoe UI', 11))
        self.style.map("Delete.TButton", background=[('active', '#990000'), ('pressed', '#800000')], foreground=[('active', 'white'), ('pressed', 'white')])
        self.style.configure("Export.TButton", background="#28a745", foreground="white", font=('Segoe UI', 11))
        self.style.map("Export.TButton", background=[('active', '#218838'), ('pressed', '#1e7e34')], foreground=[('active', 'white'), ('pressed', 'white')])

        main_frame = ttk.Frame(self.frame)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)

        title_label = ttk.Label(main_frame, text="إدارة البوانص", font=('Segoe UI', 13, 'bold'))
        title_label.pack(pady=(0, 10))

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill='x', pady=5)

        add_button = ttk.Button(button_frame, text="إضافة مزيد من البوانص", command=self.open_add_bonus_window, style="Add.TButton")
        add_button.pack(side='left', padx=5)

        search_button = ttk.Button(button_frame, text="البحث عن الكمية", command=self.open_search_quantity_window, style="Search.TButton")
        search_button.pack(side='left', padx=5)

        edit_button = ttk.Button(button_frame, text="تعديل", command=self.open_edit_bonus_window, style="Edit.TButton")
        edit_button.pack(side='left', padx=5)

        delete_button = ttk.Button(button_frame, text="حذف", command=self.delete_bonus, style="Delete.TButton")
        delete_button.pack(side='left', padx=5)

        reload_button = ttk.Button(button_frame, text="إعادة تحميل", command=self.load_bonus_data, style="Reload.TButton")
        reload_button.pack(side='left', padx=5)

        export_button = ttk.Button(button_frame, text="حفظ كـ Excel", command=self.export_to_excel, style="Export.TButton")
        export_button.pack(side='left', padx=5)

        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill='both', expand=True)

        columns = ("bonus_id", "base_qty", "bonus_qty", "discount_rate")
        self.bonus_tree = ttk.Treeview(tree_frame, columns=columns, show='headings')
        self.bonus_tree.heading("bonus_id", text="كود البونص", anchor='center', command=lambda: self.sort_by_column("bonus_id"))
        self.bonus_tree.heading("base_qty", text="الكمية الأساسية", anchor='center', command=lambda: self.sort_by_column("base_qty"))
        self.bonus_tree.heading("bonus_qty", text="كمية البونص", anchor='center', command=lambda: self.sort_by_column("bonus_qty"))
        self.bonus_tree.heading("discount_rate", text="نسبة الخصم", anchor='center', command=lambda: self.sort_by_column("discount_rate"))

        self.bonus_tree.column("bonus_id", width=100, anchor='center')
        self.bonus_tree.column("base_qty", width=100, anchor='center')
        self.bonus_tree.column("bonus_qty", width=100, anchor='center')
        self.bonus_tree.column("discount_rate", width=100, anchor='center')

        scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=self.bonus_tree.yview)
        self.bonus_tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side='right', fill='y')
        self.bonus_tree.pack(fill='both', expand=True)

        self.bonus_tree.bind('<Double-1>', self.open_edit_bonus_window)

    def sort_by_column(self, col):
        self.sort_reverse = not self.sort_reverse if self.sort_column == col else False
        self.sort_column = col

        col_index_map = {'bonus_id': 0, 'base_qty': 1, 'bonus_qty': 2, 'discount_rate': 3}
        col_index = col_index_map[col]

        def sort_key(row):
            value = row[col_index]
            if value is None:
                return (0, 0)
            try:
                numeric_value = float(str(value).replace(',', ''))
                return (1, numeric_value)
            except (ValueError, AttributeError):
                return (2, str(value).lower())

        self.current_data.sort(key=sort_key, reverse=self.sort_reverse)
        self.update_treeview(self.current_data)
        logging.info(f"Sorted by column {col}, reverse={self.sort_reverse}")

    def update_treeview(self, data):
        for item in self.bonus_tree.get_children():
            self.bonus_tree.delete(item)
        for row in data:
            self.bonus_tree.insert("", "end", values=row)

    def load_bonus_data(self, base_qty=None):
        self.current_data = []
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            self.frame.after(0, lambda: messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!"))
            return
        cur = None
        try:
            cur = connection_dis.cursor()
            if base_qty is None:
                cur.execute("SELECT BONUS_ID, BASE_QTY, BONUS_QTY, DISCOUNT_RATE FROM BONUSES")
            else:
                cur.execute("SELECT BONUS_ID, BASE_QTY, BONUS_QTY, DISCOUNT_RATE FROM BONUSES WHERE BASE_QTY = ?", (base_qty,))
            rows = cur.fetchall()
            self.current_data = list(rows)
            logging.info(f"Loaded {len(self.current_data)} bonus records from BONUSES")
            self.update_treeview(self.current_data)
        except fdb.Error as e:
            error_msg = f"فشل تحميل بيانات البونص: {str(e)}"
            logging.error(error_msg)
            self.frame.after(0, lambda msg=error_msg: messagebox.showerror("خطأ", msg))
        finally:
            if cur is not None:
                cur.close()

    def export_to_excel(self):
        if not self.current_data:
            messagebox.showwarning("تحذير", "لا توجد بيانات لحفظها!")
            return

        df = pd.DataFrame(self.current_data, columns=["كود البونص", "الكمية الأساسية", "كمية البونص", "نسبة الخصم"])
        filename = f"Bonuses_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = os.path.join(os.getcwd(), filename)
        df.to_excel(filepath, index=False, engine='openpyxl')
        logging.info(f"Saved Bonuses to {filepath}")
        messagebox.showinfo("نجاح", f"تم حفظ الملف باسم {filename}")
        os.startfile(os.path.dirname(filepath))

    def open_add_bonus_window(self):
        add_window = tk.Toplevel(self.frame)
        add_window.title("إضافة بونص جديد")
        add_window.geometry("400x300")
        add_window.configure(bg="#f3f3f3")
        ttk.Label(add_window, text="إضافة بونص جديد", font=('Segoe UI', 13, 'bold')).pack(pady=10)

        _, _, connection_dis = self.get_connections()
        auto_bonus_id = 1
        if connection_dis:
            cur = None
            try:
                cur = connection_dis.cursor()
                cur.execute("SELECT MAX(BONUS_ID) FROM BONUSES")
                max_id = cur.fetchone()[0]
                auto_bonus_id = (max_id or 0) + 1 if max_id else 1
                logging.info(f"Auto-generated BONUS_ID: {auto_bonus_id}")
            except fdb.Error as e:
                logging.error(f"Error fetching max BONUS_ID: {str(e)}")
                messagebox.showerror("خطأ", f"فشل جلب القيمة الأعلى لكود البونص: {str(e)}")
            finally:
                if cur is not None:
                    cur.close()

        fields = [
            ("كود البونص:", auto_bonus_id, False),
            ("الكمية الأساسية:", 1, True),
            ("كمية البونص:", 0, True),
        ]
        entries = {}
        for label_text, value, editable in fields:
            frame = ttk.Frame(add_window)
            frame.pack(fill='x', padx=10, pady=5)
            ttk.Label(frame, text=label_text, width=15, anchor='e').pack(side='right', padx=5)
            entry = ttk.Entry(frame)
            entry.pack(side='left', fill='x', expand=True, padx=5)
            entry.insert(0, str(value) if value else "")
            if not editable:
                entry.config(state='disabled')
            entries[label_text] = entry

        def add_bonus():
            try:
                base_qty = int(entries["الكمية الأساسية:"].get())
                bonus_qty = int(entries["كمية البونص:"].get())
                if base_qty == 0:
                    messagebox.showerror("خطأ", "الكمية الأساسية لا يمكن أن تكون صفر!")
                    return

                _, _, connection_dis = self.get_connections()
                if connection_dis:
                    cur = None
                    try:
                        cur = connection_dis.cursor()
                        cur.execute("SELECT COUNT(*) FROM BONUSES WHERE BASE_QTY = ? AND BONUS_QTY = ?", (base_qty, bonus_qty))
                        count = cur.fetchone()[0]
                        if count > 0:
                            messagebox.showerror("خطأ", f"بونص بنفس الكمية الأساسية ({base_qty}) وكمية البونص ({bonus_qty}) موجود بالفعل!")
                            return
                    except fdb.Error as e:
                        logging.error(f"Error checking duplicate bonus: {str(e)}")
                        messagebox.showerror("خطأ", f"فشل التحقق من تكرار البونص: {str(e)}")
                    finally:
                        if cur is not None:
                            cur.close()

                discount_rate = bonus_qty / base_qty
                cur = connection_dis.cursor()
                cur.execute("INSERT INTO BONUSES (BONUS_ID, BASE_QTY, BONUS_QTY, DISCOUNT_RATE) VALUES (?, ?, ?, ?)",
                           (auto_bonus_id, base_qty, bonus_qty, discount_rate))
                connection_dis.commit()
                cur.close()
                self.load_bonus_data()
                messagebox.showinfo("نجاح", "تم إضافة البونص بنجاح!")
                add_window.destroy()
            except (fdb.Error, ValueError) as e:
                error_msg = f"فشل إضافة البونص: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)

        ttk.Button(add_window, text="إضافة", command=add_bonus, style="Add.TButton").pack(pady=10)

    def open_search_quantity_window(self):
        search_window = tk.Toplevel(self.frame)
        search_window.title("البحث عن الكمية")
        search_window.geometry("300x150")
        search_window.configure(bg="#f3f3f3")
        ttk.Label(search_window, text="البحث عن الكمية الأساسية", font=('Segoe UI', 13, 'bold')).pack(pady=10)
        frame = ttk.Frame(search_window)
        frame.pack(fill='x', padx=10, pady=5)
        ttk.Label(frame, text="الكمية:", width=15, anchor='e').pack(side='right', padx=5)
        quantity_entry = ttk.Entry(frame)
        quantity_entry.pack(side='left', fill='x', expand=True, padx=5)
        def search_quantity():
            try:
                base_qty = int(quantity_entry.get())
                self.load_bonus_data(base_qty=base_qty)
                search_window.destroy()
            except ValueError:
                messagebox.showerror("خطأ", "يرجى إدخال كمية صحيحة!")
        ttk.Button(search_window, text="بحث", command=search_quantity, style="Search.TButton").pack(pady=10)

    def open_edit_bonus_window(self, event=None):
        selected_item = self.bonus_tree.selection()
        if not selected_item:
            messagebox.showwarning("تحذير", "يرجى اختيار بونص لتعديله!")
            return

        item = self.bonus_tree.item(selected_item[0])
        bonus_id, base_qty, bonus_qty, discount_rate = item['values']

        edit_window = tk.Toplevel(self.frame)
        edit_window.title("تعديل البونص")
        edit_window.geometry("400x300")
        edit_window.configure(bg="#f3f3f3")

        ttk.Label(edit_window, text="تعديل بونص", font=('Segoe UI', 13, 'bold')).pack(pady=10)

        fields = [
            ("كود البونص:", bonus_id, False),
            ("الكمية الأساسية:", base_qty, True),
            ("كمية البونص:", bonus_qty, True),
        ]
        entries = {}
        for label_text, value, editable in fields:
            frame = ttk.Frame(edit_window)
            frame.pack(fill='x', padx=10, pady=5)
            ttk.Label(frame, text=label_text, width=15, anchor='e').pack(side='right', padx=5)
            entry = ttk.Entry(frame)
            entry.pack(side='left', fill='x', expand=True, padx=5)
            entry.insert(0, str(value))
            if not editable:
                entry.config(state='disabled')
            entries[label_text] = entry

        def save_changes():
            try:
                new_base_qty = int(entries["الكمية الأساسية:"].get())
                new_bonus_qty = int(entries["كمية البونص:"].get())
                if new_base_qty == 0:
                    messagebox.showerror("خطأ", "الكمية الأساسية لا يمكن أن تكون صفر!")
                    return
                new_discount_rate = new_bonus_qty / new_base_qty
                _, _, connection_dis = self.get_connections()
                if not connection_dis:
                    messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
                    return
                cur = connection_dis.cursor()
                cur.execute("""
                    UPDATE BONUSES 
                    SET BASE_QTY = ?, BONUS_QTY = ?, DISCOUNT_RATE = ?
                    WHERE BONUS_ID = ?
                """, (new_base_qty, new_bonus_qty, new_discount_rate, int(bonus_id)))
                connection_dis.commit()
                cur.close()
                self.load_bonus_data()
                messagebox.showinfo("نجاح", "تم تعديل البونص بنجاح!")
                edit_window.destroy()
            except (fdb.Error, ValueError) as e:
                error_msg = f"فشل تعديل البونص: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)

        def cancel_changes():
            edit_window.destroy()

        button_frame = ttk.Frame(edit_window)
        button_frame.pack(pady=10, fill='x')
        save_button = ttk.Button(button_frame, text="حفظ", command=save_changes, style="Save.TButton")
        save_button.pack(side='left', padx=5)
        cancel_button = ttk.Button(button_frame, text="تراجع", command=cancel_changes, style="Cancel.TButton")
        cancel_button.pack(side='left', padx=5)

    def delete_bonus(self):
        selected_item = self.bonus_tree.selection()
        if not selected_item:
            messagebox.showwarning("تحذير", "يرجى اختيار بونص للحذف!")
            return

        item = self.bonus_tree.item(selected_item[0])
        bonus_id = int(item['values'][0])

        if messagebox.askyesno("تأكيد الحذف", f"هل أنت متأكد من حذف البونص ذو الكود {bonus_id}؟"):
            _, _, connection_dis = self.get_connections()
            if not connection_dis:
                messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
            cur = None
            try:
                cur = connection_dis.cursor()
                cur.execute("DELETE FROM BONUSES WHERE BONUS_ID = ?", (bonus_id,))
                connection_dis.commit()
                logging.info(f"Deleted bonus with BONUS_ID: {bonus_id}")
                self.load_bonus_data()
                messagebox.showinfo("نجاح", "تم حذف البونص بنجاح!")
            except fdb.Error as e:
                error_msg = f"فشل حذف البونص: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)
            finally:
                if cur is not None:
                    cur.close()

    def insert_bonus_data(self):
        # ملاحظة: هذه الدالة تعتمد على أن self.get_connections هو دالة صالحة تم تعيينها مسبقًا
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            logging.error("No connection to dis_db for inserting bonus data")
            return

        cur = None
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT COUNT(*) FROM BONUSES")
            initial_count = cur.fetchone()[0]
            logging.info(f"Initial count of BONUSES: {initial_count}")
            logging.info("No default bonus data to insert, skipping insertion")
            cur.execute("SELECT COUNT(*) FROM BONUSES")
            final_count = cur.fetchone()[0]
            logging.info(f"Total count of BONUSES after operation: {final_count}")
        except fdb.Error as e:
            error_msg = f"فشل التحقق من بيانات البونص: {str(e)}"
            logging.error(error_msg)
            self.frame.after(0, lambda msg=error_msg: messagebox.showerror("خطأ", msg))
        finally:
            if cur is not None:
                cur.close()

class ItemsListTab:
    def __init__(self, notebook, get_connections):
        self.frame = ttk.Frame(notebook)
        notebook.add(self.frame, text="قائمة الأصناف")
        self.get_connections = get_connections
        self.current_data = []
        self.stores = []
        self.store_names = {}
        self.sort_column = None
        self.sort_reverse = False
        self.company_list = []

        # إعداد إطار الفلاتر
        filter_frame = tk.Frame(self.frame, bg="#f0f0f0")
        filter_frame.pack(pady=5, padx=10, fill='x')

        tk.Label(filter_frame, text="اختر المخزن:", font=('Arial', 12), bg="#f0f0f0").pack(side='right', padx=5)
        self.store_combobox = ttk.Combobox(filter_frame, font=('Arial', 12), state='readonly', justify='right')
        self.store_combobox.pack(side='right', padx=5)
        self.store_combobox.bind('<<ComboboxSelected>>', self.apply_filters)

        tk.Label(filter_frame, text="بحث بالاسم:", font=('Arial', 12), bg="#f0f0f0").pack(side='right', padx=5)
        self.search_name_entry = tk.Entry(filter_frame, font=('Arial', 12), justify='right')
        self.search_name_entry.pack(side='right', padx=5)
        self.search_name_entry.bind('<KeyRelease>', self.apply_filters)

        tk.Label(filter_frame, text="فلتر بالشركة:", font=('Arial', 12), bg="#f0f0f0").pack(side='right', padx=5)
        self.company_combobox = ttk.Combobox(filter_frame, font=('Arial', 12), state='normal', justify='right')
        self.company_combobox.pack(side='right', padx=5)
        self.company_combobox.bind('<KeyRelease>', self.filter_company_combobox)
        self.company_combobox.bind('<<ComboboxSelected>>', self.apply_filters)

        tk.Label(filter_frame, text="فلتر بالكمية:", font=('Arial', 12), bg="#f0f0f0").pack(side='right', padx=5)
        self.quantity_entry = tk.Entry(filter_frame, font=('Arial', 12), justify='right')
        self.quantity_entry.pack(side='right', padx=5)
        self.quantity_entry.bind('<KeyRelease>', self.apply_filters)

        self.quantity_filter_var = tk.StringVar(value="all")
        tk.Label(filter_frame, text="عرض الكمية:", font=('Arial', 12), bg="#f0f0f0").pack(side='right', padx=5)
        tk.Radiobutton(filter_frame, text="الكل", variable=self.quantity_filter_var, value="all", bg="#f0f0f0", command=self.apply_filters).pack(side='right')
        tk.Radiobutton(filter_frame, text="صفر", variable=self.quantity_filter_var, value="zero", bg="#f0f0f0", command=self.apply_filters).pack(side='right')
        tk.Radiobutton(filter_frame, text="أكبر من صفر", variable=self.quantity_filter_var, value="greater", bg="#f0f0f0", command=self.apply_filters).pack(side='right')

        # إضافة فلتر البونص
        self.bonus_filter_var = tk.StringVar(value="all")
        tk.Label(filter_frame, text="فلترة حسب البونص:", font=('Arial', 12), bg="#f0f0f0").pack(side='right', padx=5)
        tk.Radiobutton(filter_frame, text="عرض الكل", variable=self.bonus_filter_var, value="all", bg="#f0f0f0", command=self.apply_filters).pack(side='right')
        tk.Radiobutton(filter_frame, text="ذات بونص فقط", variable=self.bonus_filter_var, value="with_bonus", bg="#f0f0f0", command=self.apply_filters).pack(side='right')

        # إعداد الجدول
        table_frame = tk.Frame(self.frame, bg="#f0f0f0")
        table_frame.pack(pady=10, padx=10, fill='both', expand=True)
        scroll_y = ttk.Scrollbar(table_frame, orient='vertical')
        scroll_x = ttk.Scrollbar(table_frame, orient='horizontal')
        scroll_y.pack(side='left', fill='y')
        scroll_x.pack(side='bottom', fill='x')

        # تحديث الأعمدة لتشمل كل الحقول
        self.columns = (
            'Prod_ID', 'Cop_Name', 'Prod_Name', 'Price_1', 'Total_Qty_All', 'Discount_Cost',
            'Tax_Qty', 'Sale_Price', 'Sale_Margin', 'Main_Margin', 'Sale_Discount',
            'Base_Qty', 'Bonus_Qty', 'Discount_Rate', 'Cash_Discount_Name', 'Product_Type_Name', 'Tax_Rate'
        )
        self.tree = ttk.Treeview(table_frame, columns=self.columns, show='headings', yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self.tree.pack(fill='both', expand=True)
        scroll_y.config(command=self.tree.yview)
        scroll_x.config(command=self.tree.xview)

        self.setup_tree_columns()

        # إعداد الأزرار
        button_frame = ttk.Frame(self.frame)
        button_frame.pack(fill='x', pady=5)
        tk.Button(button_frame, text="تحميل الجدول", command=self.load_table, bg="#2196F3", fg="white", font=('Arial', 12)).pack(side='right', padx=5)
        ttk.Button(button_frame, text="حفظ كـ Excel", command=self.export_to_excel, style="Export.TButton").pack(side='right', padx=5)

        self.tree.bind('<Double-1>', self.open_edit_window)

        style = ttk.Style()
        style.configure("Export.TButton", background="#28a745", foreground="white", font=('Segoe UI', 11))
        style.map("Export.TButton", background=[('active', '#218838'), ('pressed', '#1e7e34')], foreground=[('active', 'white'), ('pressed', 'white')])

    def setup_tree_columns(self):
        columns = {
            'Prod_ID': 'كود الصنف',
            'Cop_Name': 'الشركة',
            'Prod_Name': 'اسم الصنف',
            'Price_1': 'السعر',
            'Total_Qty_All': 'الكمية',
            'Discount_Cost': 'المرجح',
            'Tax_Qty': 'قيمة مضافة',
            'Sale_Price': 'سعر صيدلي',
            'Sale_Margin': 'هامش صيدلي',
            'Main_Margin': 'هامش موزع',
            'Sale_Discount': 'خصم البيع',
            'Base_Qty': 'الكمية الأساسية',
            'Bonus_Qty': 'كمية البونص',
            'Discount_Rate': 'نسبة الخصم',
            'Cash_Discount_Name': 'اسم الخصم النقدي',
            'Product_Type_Name': 'نوع المنتج',
            'Tax_Rate': 'نسبة الضريبة'
        }
        for col, text in reversed(list(columns.items())):
            self.tree.heading(col, text=text, anchor='e', command=lambda c=col: self.sort_by_column(c))
            self.tree.column(col, anchor='e', width=100, minwidth=50)
        self.tree.tag_configure('even', background='#E8F5E9')
        self.tree.tag_configure('odd', background='#FFFFFF')

    def load_table(self):
        logging.info("Starting to load table")
        connection_stores, connection_main, connection_dis = self.get_connections()
        if not all([connection_main, connection_dis]):
            messagebox.showwarning("تحذير", "يرجى الاتصال بقاعدة البيانات أولاً!")
            return
        
        # إنشاء نافذة تقدم
        progress_window = tk.Toplevel(self.frame)
        progress_window.title("جارٍ تحميل البيانات...")
        progress_window.geometry("300x100")
        ttk.Label(progress_window, text="جارٍ تحميل قائمة الأصناف، الرجاء الانتظار...").pack(pady=20)
        progress_bar = ttk.Progressbar(progress_window, mode='determinate', maximum=100)
        progress_bar.pack(pady=10)

        def load_task():
            try:
                cur_main = connection_main.cursor()
                cur_dis = connection_dis.cursor()

                # جلب أسماء الشركات
                cur_main.execute("SELECT DISTINCT COP_NAME FROM COP_USERS WHERE COP_NAME IS NOT NULL")
                self.company_list = [row[0] for row in cur_main.fetchall()]
                self.company_combobox['values'] = ["الكل"] + self.company_list
                self.company_combobox.set("الكل")

                # الخطوة 1: جلب البيانات من ORGA.GDB
                query_main = """
                    SELECT
                        p.PROD_ID,
                        c.COP_NAME,
                        p.PROD_NAME,
                        p.PRICE_1,
                        COALESCE(s.TOTAL_QTY_ALL, '0') AS TOTAL_QTY_ALL,
                        COALESCE(s.DISCOUNT_COST, '0') AS DISCOUNT_COST,
                        s.STORE_ID
                    FROM PRODUCTS p
                    LEFT JOIN COP_USERS c ON p.COP_ID = c.COP_ID
                    LEFT JOIN STOCK_STOCK s ON p.PROD_ID = s.PROD_ID
                """
                cur_main.execute(query_main)
                main_data = {row[0]: row for row in cur_main.fetchall()}
                progress_bar['value'] = 50  # تحديث التقدم بعد الاستعلام الأول
                progress_window.update()

                # الخطوة 2: جلب البيانات من DIST_DB
                query_dis = """
                    SELECT
                        a.PROD_ID,
                        a.TAX_QTY,
                        a.SALE_PRICE,
                        a.SALE_MARGIN,
                        a.MAIN_MARGIN,
                        a.SALE_DISCOUNT,
                        b.BASE_QTY,
                        b.BONUS_QTY,
                        b.DISCOUNT_RATE,
                        cd.CASH_DISCOUNT_NAME,
                        pt.PRODUCT_TYPE_NAME,
                        t.TAX_RATE
                    FROM ADDITIONS a
                    LEFT JOIN PRODUCT_BONUS_LINK pbl ON a.PROD_ID = pbl.PROD_ID
                    LEFT JOIN BONUSES b ON pbl.BONUS_ID = b.BONUS_ID
                    LEFT JOIN ITEM_CASH_DISCOUNTS icd ON a.PROD_ID = icd.PROD_ID
                    LEFT JOIN CASH_DISCOUNTS cd ON icd.CASH_DISCOUNT_ID = cd.CASH_DISCOUNT_ID
                    LEFT JOIN ITEM_PRODUCT_TYPES ipt ON a.PROD_ID = ipt.PROD_ID
                    LEFT JOIN PRODUCT_TYPES pt ON ipt.PRODUCT_TYPE_ID = pt.PRODUCT_TYPE_ID
                    LEFT JOIN ITEM_TAXES it ON a.PROD_ID = it.PROD_ID
                    LEFT JOIN TAXES t ON it.TAX_ID = t.TAX_ID
                """
                cur_dis.execute(query_dis)
                dis_data = {row[0]: row[1:] for row in cur_dis.fetchall()}
                progress_bar['value'] = 75  # تحديث التقدم بعد الاستعلام الثاني
                progress_window.update()

                # الخطوة 3: دمج البيانات
                self.current_data = []
                for prod_id, main_row in main_data.items():
                    dis_row = dis_data.get(prod_id, (None,) * 11)
                    self.current_data.append(
                        (
                            main_row[0],  # Prod_ID
                            main_row[1] or "",  # Cop_Name
                            main_row[2] or "",  # Prod_Name
                            self._to_float(main_row[3]),  # Price_1
                            self._to_float(main_row[4]),  # Total_Qty_All
                            self._to_float(main_row[5]),  # Discount_Cost
                            self._to_float(dis_row[0]),  # Tax_Qty
                            self._to_float(dis_row[1]),  # Sale_Price
                            self._to_float(dis_row[2]),  # Sale_Margin
                            self._to_float(dis_row[3]),  # Main_Margin
                            self._to_float(dis_row[4]),  # Sale_Discount
                            dis_row[5] if dis_row[5] is not None else 0,  # Base_Qty
                            dis_row[6] if dis_row[6] is not None else 0,  # Bonus_Qty
                            self._to_float(dis_row[7]),  # Discount_Rate
                            dis_row[8] if dis_row[8] is not None else "غير محدد",  # Cash_Discount_Name
                            dis_row[9] if dis_row[9] is not None else "غير محدد",  # Product_Type_Name
                            self._to_float(dis_row[10]),  # Tax_Rate
                            main_row[6] or ""  # STORE_ID
                        )
                    )

                logging.info(f"Query returned {len(self.current_data)} items")

                if not self.current_data:
                    self.frame.after(0, lambda: messagebox.showwarning("تحذير", "لا توجد بيانات لعرضها!"))
                else:
                    self.frame.after(0, self.apply_filters)  # تشغيل الفلترة في الـ GUI thread

                cur_main.close()
                cur_dis.close()
                self.frame.after(0, lambda: self.test_relationships(connection_main))
                progress_bar['value'] = 100
                progress_window.update()
                self.frame.after(100, progress_window.destroy)  # إغلاق نافذة التقدم

            except Exception as e:
                logging.error(f"Error loading table: {str(e)}")
                self.frame.after(0, lambda: messagebox.showerror("خطأ", f"فشل تحميل الجدول: {str(e)}"))

        # تشغيل التحميل في خيط منفصل
        threading.Thread(target=load_task, daemon=True).start()

    def _to_float(self, value):
        if value is None:
            return 0.0
        try:
            return float(str(value).replace(',', ''))
        except (ValueError, AttributeError):
            return 0.0

    def update_treeview(self, data):
        # مسح الجدول بسرعة
        self.tree.delete(*self.tree.get_children())
        
        # إضافة البيانات في دفعات لتحسين الأداء
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            for j, row in enumerate(batch):
                tag = 'even' if (i + j) % 2 == 0 else 'odd'
                self.tree.insert("", "end", values=row[:-1], tags=(tag,))
            self.tree.update()  # تحديث الـ GUI بين الدفعات
        
        self.adjust_column_widths()

    def adjust_column_widths(self):
        for col in self.tree['columns']:
            max_width = len(self.tree.heading(col)['text']) * 10
            for item in self.tree.get_children():
                value = str(self.tree.item(item, 'values')[self.tree['columns'].index(col)])
                max_width = max(max_width, len(value) * 10)
            self.tree.column(col, width=max_width, minwidth=50)

    def sort_by_column(self, col):
        self.sort_reverse = not self.sort_reverse if self.sort_column == col else False
        self.sort_column = col

        col_index_map = {
            'Prod_ID': 0, 'Cop_Name': 1, 'Prod_Name': 2, 'Price_1': 3, 'Total_Qty_All': 4, 'Discount_Cost': 5,
            'Tax_Qty': 6, 'Sale_Price': 7, 'Sale_Margin': 8, 'Main_Margin': 9, 'Sale_Discount': 10,
            'Base_Qty': 11, 'Bonus_Qty': 12, 'Discount_Rate': 13, 'Cash_Discount_Name': 14, 'Product_Type_Name': 15, 'Tax_Rate': 16
        }
        col_index = col_index_map[col]

        def sort_key(row):
            value = row[col_index]
            if value is None:
                return (0, 0)
            try:
                if col in ['Prod_ID', 'Total_Qty_All', 'Price_1', 'Discount_Cost', 'Tax_Qty', 'Sale_Price', 'Sale_Margin', 'Main_Margin', 'Sale_Discount', 'Base_Qty', 'Bonus_Qty', 'Discount_Rate', 'Tax_Rate']:
                    numeric_value = float(str(value).replace(',', ''))
                    return (1, numeric_value)
                return (2, str(value).lower())
            except (ValueError, AttributeError):
                return (3, str(value).lower())

        self.current_data.sort(key=sort_key, reverse=self.sort_reverse)
        self.apply_filters()
        logging.info(f"Sorted by column {col}, reverse={self.sort_reverse}")

    def apply_filters(self, event=None):
        store_id = next((k for k, v in self.store_names.items() if v == self.store_combobox.get()), None) if self.store_combobox.get() else None
        search_name = self.search_name_entry.get().strip().lower()
        company = self.company_combobox.get()
        quantity_filter = self.quantity_entry.get().strip()
        quantity_mode = self.quantity_filter_var.get()
        bonus_filter = self.bonus_filter_var.get()

        filtered_data = self.current_data

        if store_id:
            filtered_data = [row for row in filtered_data if str(row[17]) == store_id]  # STORE_ID في المؤشر 17
        if search_name:
            filtered_data = [row for row in filtered_data if search_name in str(row[2]).lower()]  # Prod_Name
        if company and company != "الكل":
            filtered_data = [row for row in filtered_data if row[1] == company]  # Cop_Name
        if quantity_filter:
            try:
                quantity = float(quantity_filter)
                filtered_data = [row for row in filtered_data if row[4] == quantity]  # Total_Qty_All
            except ValueError:
                pass
        if quantity_mode == "zero":
            filtered_data = [row for row in filtered_data if row[4] == 0.0]  # Total_Qty_All
        elif quantity_mode == "greater":
            filtered_data = [row for row in filtered_data if row[4] > 0.0]  # Total_Qty_All
        if bonus_filter == "with_bonus":
            filtered_data = [row for row in filtered_data if row[11] > 0 or row[12] > 0]  # Base_Qty أو Bonus_Qty

        self.update_treeview(filtered_data)
        logging.info(f"Filtered to {len(filtered_data)} items")

    def export_to_excel(self):
        if not self.current_data:
            messagebox.showwarning("تحذير", "لا توجد بيانات لحفظها!")
            return

        df = pd.DataFrame([row[:-1] for row in self.current_data], columns=[
            "كود الصنف", "الشركة", "اسم الصنف", "السعر", "الكمية", "المرجح",
            "قيمة مضافة", "سعر صيدلي", "هامش صيدلي", "هامش موزع", "خصم البيع",
            "الكمية الأساسية", "كمية البونص", "نسبة الخصم", "اسم الخصم النقدي", "نوع المنتج", "نسبة الضريبة"
        ])
        filename = f"ItemsList_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = os.path.join(os.getcwd(), filename)
        df.to_excel(filepath, index=False, engine='openpyxl')
        logging.info(f"Saved ItemsList to {filepath}")
        messagebox.showinfo("نجاح", f"تم حفظ الملف باسم {filename}")
        os.startfile(os.path.dirname(filepath))

    def open_edit_window(self, event):
        if hasattr(self, 'edit_window') and self.edit_window.winfo_exists():
            self.edit_window.destroy()

        selected_item = self.tree.selection()
        if not selected_item:
            logging.warning("No item selected in treeview")
            return

        item = self.tree.item(selected_item[0])
        discount_cost, total_qty, price, prod_name, cop_name, prod_id = item['values']
        prod_id_str = str(prod_id)
        logging.info(f"Opening edit window for PROD_ID: {prod_id_str}, PROD_NAME: {prod_name}, PRICE: {price}")

        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            logging.error("No connection to dis_db available")
            messagebox.showwarning("تحذير", "يرجى الاتصال بقاعدة البيانات dis_db أولاً!")
            return

        try:
            cur = connection_dis.cursor()
            cur.execute("""
                SELECT TAX_QTY, SALE_PRICE, SALE_MARGIN, MAIN_MARGIN, SALE_DISCOUNT 
                FROM ADDITIONS 
                WHERE PROD_ID = ?
            """, (prod_id_str,))
            result = cur.fetchone()
            if result:
                tax_qty, sale_price, sale_margin, main_margin, sale_discount = result
                logging.info(f"Found data in ADDITIONS: {result}")
            else:
                tax_qty, sale_price, sale_margin, main_margin, sale_discount = 0.0, 0.0, 0.0, 0.0, 0.0
                logging.warning(f"No record found in ADDITIONS for PROD_ID: {prod_id_str}, using default values")
            cur.close()
        except fdb.Error as e:
            logging.error(f"Error fetching data from ADDITIONS: {str(e)}")
            messagebox.showerror("خطأ", f"فشل جلب بيانات الصنف: {str(e)}")
            return

        linked_bonuses = []
        try:
            cur = connection_dis.cursor()
            cur.execute("""
                SELECT b.BONUS_ID, b.BASE_QTY, b.BONUS_QTY, b.DISCOUNT_RATE
                FROM PRODUCT_BONUS_LINK pbl
                JOIN BONUSES b ON pbl.BONUS_ID = b.BONUS_ID
                WHERE pbl.PROD_ID = ?
            """, (prod_id_str,))
            linked_bonuses = cur.fetchall()
            logging.info(f"Found {len(linked_bonuses)} linked bonuses for PROD_ID: {prod_id_str}")
            cur.close()
        except fdb.Error as e:
            logging.error(f"Error fetching linked bonuses: {str(e)}")
            messagebox.showerror("خطأ", f"فشل جلب البونصات المرتبطة: {str(e)}")
            return

        # جلب البيانات للقوائم المنسدلة
        cash_discounts, product_types, taxes = [], [], []
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT CASH_DISCOUNT_ID, CASH_DISCOUNT_NAME FROM CASH_DISCOUNTS")
            cash_discounts = [(row[0], row[1]) for row in cur.fetchall()]
            cur.execute("SELECT PRODUCT_TYPE_ID, PRODUCT_TYPE_NAME FROM PRODUCT_TYPES")
            product_types = [(row[0], row[1]) for row in cur.fetchall()]
            cur.execute("SELECT TAX_ID, TAX_RATE FROM TAXES")
            taxes = [(row[0], str(row[1])) for row in cur.fetchall()]
            cur.close()
        except fdb.Error as e:
            logging.error(f"Error loading dropdown data: {str(e)}")
            messagebox.showerror("خطأ", f"فشل جلب بيانات القوائم المنسدلة: {str(e)}")
            return

        # جلب العلاقات الحالية للمنتج
        current_cash_discounts = []
        current_product_types = []
        current_taxes = []
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT CASH_DISCOUNT_ID FROM ITEM_CASH_DISCOUNTS WHERE PROD_ID = ?", (prod_id_str,))
            current_cash_discounts = [row[0] for row in cur.fetchall()]
            cur.execute("SELECT PRODUCT_TYPE_ID FROM ITEM_PRODUCT_TYPES WHERE PROD_ID = ?", (prod_id_str,))
            current_product_types = [row[0] for row in cur.fetchall()]
            cur.execute("SELECT TAX_ID FROM ITEM_TAXES WHERE PROD_ID = ?", (prod_id_str,))
            current_taxes = [row[0] for row in cur.fetchall()]
            cur.close()
        except fdb.Error as e:
            logging.error(f"Error loading current relations: {str(e)}")
            messagebox.showerror("خطأ", f"فشل جلب العلاقات الحالية: {str(e)}")
            return

        self.edit_window = tk.Toplevel(self.frame)
        self.edit_window.title("تعديل الصنف")
        self.edit_window.geometry("650x900")
        self.edit_window.configure(bg="#f3f3f3")

        style = ttk.Style()
        style.theme_use('clam')
        style.configure("TLabel", font=('Segoe UI', 11, 'bold'), background="#f3f3f3", foreground="#1f252a")
        style.configure("TEntry", font=('Segoe UI', 11), justify='right', padding=5)
        style.configure("TButton", font=('Segoe UI', 11), padding=5)

        title_label = ttk.Label(self.edit_window, text="تعديل بيانات الصنف", font=('Segoe UI', 13, 'bold'), foreground="#1f252a")
        title_label.pack(pady=(10, 5))

        main_frame = ttk.Frame(self.edit_window, padding="10")
        main_frame.pack(pady=10, padx=15, fill='both', expand=True)

        fields = [
            ("كود الصنف:", str(prod_id)),
            ("اسم الصنف:", prod_name),
            ("السعر:", str(price)),
            ("ض ق:", str(float(tax_qty))),
            ("سعر ص:", str(float(sale_price))),
            ("هامش ص:", str(float(sale_margin))),
            ("هامش م:", str(float(main_margin))),
            ("خ ص:", str(float(sale_discount))),
        ]

        entries = {}
        for i, (label_text, value) in enumerate(fields):
            field_frame = ttk.Frame(main_frame)
            field_frame.pack(fill='x', padx=5, pady=5)
            label = ttk.Label(field_frame, text=label_text, width=15, anchor='e')
            label.pack(side='right', padx=5)
            entry = ttk.Entry(field_frame)
            entry.pack(side='left', fill='x', expand=True, padx=5)
            entry.insert(0, value.strip())
            entries[label_text] = entry
            if i < 3:
                entry.configure(state='disabled')

        bonus_frame = ttk.LabelFrame(main_frame, text="البوانص المرتبطة", padding="10")
        bonus_frame.pack(fill='both', expand=True, pady=10)

        bonus_tree = ttk.Treeview(bonus_frame, columns=("bonus_id", "base_qty", "bonus_qty", "discount_rate"), show='headings', height=5)
        bonus_tree.heading("bonus_id", text="كود البونص", anchor='center')
        bonus_tree.heading("base_qty", text="الكمية الأساسية", anchor='center')
        bonus_tree.heading("bonus_qty", text="كمية البونص", anchor='center')
        bonus_tree.heading("discount_rate", text="نسبة الخصم", anchor='center')
        bonus_tree.column("bonus_id", width=100, anchor='center')
        bonus_tree.column("base_qty", width=100, anchor='center')
        bonus_tree.column("bonus_qty", width=100, anchor='center')
        bonus_tree.column("discount_rate", width=100, anchor='center')
        bonus_tree.pack(fill='both', expand=True)

        for bonus in linked_bonuses:
            bonus_tree.insert("", "end", values=bonus)

        # قوائم منسدلة للربط
        relations_frame = ttk.LabelFrame(main_frame, text="العلاقات", padding="10")
        relations_frame.pack(fill='both', expand=True, pady=10)

        ttk.Label(relations_frame, text="خصومات النقدية:").pack(pady=5)
        cash_discount_combo = ttk.Combobox(relations_frame, values=[f"{id} - {name}" for id, name in cash_discounts], state="readonly")
        cash_discount_combo.pack(fill='x', padx=10)
        if current_cash_discounts:
            cash_discount_combo.set(f"{current_cash_discounts[0]} - {next((name for id, name in cash_discounts if id == current_cash_discounts[0]), '')}")

        ttk.Label(relations_frame, text="نوع المنتج:").pack(pady=5)
        product_type_combo = ttk.Combobox(relations_frame, values=[f"{id} - {name}" for id, name in product_types], state="readonly")
        product_type_combo.pack(fill='x', padx=10)
        if current_product_types:
            product_type_combo.set(f"{current_product_types[0]} - {next((name for id, name in product_types if id == current_product_types[0]), '')}")

        ttk.Label(relations_frame, text="الضرائب:").pack(pady=5)
        tax_combo = ttk.Combobox(relations_frame, values=[f"{id} - {rate}" for id, rate in taxes], state="readonly")
        tax_combo.pack(fill='x', padx=10)
        if current_taxes:
            tax_combo.set(f"{current_taxes[0]} - {next((rate for id, rate in taxes if id == current_taxes[0]), '')}")

        def open_bonus_selection():
            bonus_window = tk.Toplevel(self.edit_window)
            bonus_window.title("اختيار البونص")
            bonus_window.geometry("500x400")

            available_bonuses_tree = ttk.Treeview(bonus_window, columns=("bonus_id", "base_qty", "bonus_qty"), show='headings')
            available_bonuses_tree.heading("bonus_id", text="كود البونص", anchor='center', command=lambda: sort_bonus_selection("bonus_id"))
            available_bonuses_tree.heading("base_qty", text="الكمية الأساسية", anchor='center', command=lambda: sort_bonus_selection("base_qty"))
            available_bonuses_tree.heading("bonus_qty", text="كمية البونص", anchor='center', command=lambda: sort_bonus_selection("bonus_qty"))
            available_bonuses_tree.column("bonus_id", width=100, anchor='center')
            available_bonuses_tree.column("base_qty", width=100, anchor='center')
            available_bonuses_tree.column("bonus_qty", width=100, anchor='center')
            available_bonuses_tree.pack(fill='both', expand=True, padx=10, pady=10)

            available_bonuses = []
            try:
                cur = connection_dis.cursor()
                cur.execute("SELECT BONUS_ID, BASE_QTY, BONUS_QTY FROM BONUSES")
                all_bonuses = cur.fetchall()
                for bonus in all_bonuses:
                    if bonus[0] not in [b[0] for b in linked_bonuses]:
                        available_bonuses.append(bonus)
                        available_bonuses_tree.insert("", "end", values=bonus)
                cur.close()
            except fdb.Error as e:
                logging.error(f"Error fetching bonuses: {str(e)}")
                messagebox.showerror("خطأ", f"فشل جلب البونصات: {str(e)}")

            sort_column = None
            sort_reverse = False

            def sort_bonus_selection(col):
                nonlocal sort_column, sort_reverse
                sort_reverse = not sort_reverse if sort_column == col else False
                sort_column = col

                col_index_map = {"bonus_id": 0, "base_qty": 1, "bonus_qty": 2}
                col_index = col_index_map[col]

                def sort_key(row):
                    value = row[col_index]
                    if value is None:
                        return (0, 0)
                    try:
                        numeric_value = float(str(value).replace(',', ''))
                        return (1, numeric_value)
                    except (ValueError, AttributeError):
                        return (2, str(value).lower())

                available_bonuses.sort(key=sort_key, reverse=sort_reverse)
                for item in available_bonuses_tree.get_children():
                    available_bonuses_tree.delete(item)
                for bonus in available_bonuses:
                    available_bonuses_tree.insert("", "end", values=bonus)
                logging.info(f"Sorted bonus selection by {col}, reverse={sort_reverse}")

            def add_selected_bonus():
                selected = available_bonuses_tree.selection()
                if not selected:
                    messagebox.showwarning("تحذير", "يرجى اختيار بونص!")
                    return
                bonus_values = available_bonuses_tree.item(selected[0])['values']
                bonus_id = bonus_values[0]
                bonus_tree.insert("", "end", values=(bonus_id, bonus_values[1], bonus_values[2], bonus_values[1] / bonus_values[2] if bonus_values[2] else 0))
                for bonus in available_bonuses[:]:
                    if bonus[0] == bonus_id:
                        available_bonuses.remove(bonus)
                        break
                available_bonuses_tree.delete(selected[0])

            ttk.Button(bonus_window, text="إضافة", command=add_selected_bonus).pack(pady=5)
            ttk.Button(bonus_window, text="إغلاق", command=bonus_window.destroy).pack(pady=5)

        def remove_selected_bonus():
            selected = bonus_tree.selection()
            if selected:
                bonus_tree.delete(selected[0])

        ttk.Button(bonus_frame, text="إضافة بونص", command=open_bonus_selection).pack(side='left', padx=5, pady=5)
        ttk.Button(bonus_frame, text="حذف بونص", command=remove_selected_bonus).pack(side='left', padx=5, pady=5)

        def save_changes():
            try:
                cur = connection_dis.cursor()
                cur.execute("""
                    UPDATE ADDITIONS 
                    SET TAX_QTY = ?, SALE_PRICE = ?, SALE_MARGIN = ?, MAIN_MARGIN = ?, SALE_DISCOUNT = ?
                    WHERE PROD_ID = ?
                """, (
                    float(entries["ض ق:"].get()),
                    float(entries["سعر ص:"].get()),
                    float(entries["هامش ص:"].get()),
                    float(entries["هامش م:"].get()),
                    float(entries["خ ص:"].get()),
                    prod_id_str
                ))

                # حفظ البونصات
                cur.execute("DELETE FROM PRODUCT_BONUS_LINK WHERE PROD_ID = ?", (prod_id_str,))
                for item in bonus_tree.get_children():
                    bonus_id = int(bonus_tree.item(item)['values'][0])
                    cur.execute("INSERT INTO PRODUCT_BONUS_LINK (PROD_ID, BONUS_ID) VALUES (?, ?)", (prod_id_str, bonus_id))

                # حفظ الخصومات النقدية
                cur.execute("DELETE FROM ITEM_CASH_DISCOUNTS WHERE PROD_ID = ?", (prod_id_str,))
                selected_cash_discount = cash_discount_combo.get()
                if selected_cash_discount:
                    cash_discount_id = selected_cash_discount.split(" - ")[0]
                    cur.execute("INSERT INTO ITEM_CASH_DISCOUNTS (PROD_ID, CASH_DISCOUNT_ID) VALUES (?, ?)", (prod_id_str, cash_discount_id))

                # حفظ أنواع المنتجات
                cur.execute("DELETE FROM ITEM_PRODUCT_TYPES WHERE PROD_ID = ?", (prod_id_str,))
                selected_product_type = product_type_combo.get()
                if selected_product_type:
                    product_type_id = selected_product_type.split(" - ")[0]
                    cur.execute("INSERT INTO ITEM_PRODUCT_TYPES (PROD_ID, PRODUCT_TYPE_ID) VALUES (?, ?)", (prod_id_str, product_type_id))

                # حفظ الضرائب
                cur.execute("DELETE FROM ITEM_TAXES WHERE PROD_ID = ?", (prod_id_str,))
                selected_tax = tax_combo.get()
                if selected_tax:
                    tax_id = selected_tax.split(" - ")[0]
                    cur.execute("INSERT INTO ITEM_TAXES (PROD_ID, TAX_ID) VALUES (?, ?)", (prod_id_str, tax_id))

                connection_dis.commit()
                cur.close()
                logging.info(f"Saved changes for PROD_ID: {prod_id_str}")
                messagebox.showinfo("نجاح", "تم حفظ التعديلات بنجاح!")
                self.edit_window.destroy()
            except (fdb.Error, ValueError) as e:
                logging.error(f"Error saving changes: {str(e)}")
                messagebox.showerror("خطأ", f"فشل حفظ التعديلات: {str(e)}")

        def cancel_changes():
            self.edit_window.destroy()

        button_frame = ttk.Frame(self.edit_window, padding="10")
        button_frame.pack(pady=10, fill='x')
        style.configure("Save.TButton", background="#0078d4", foreground="white")
        save_button = ttk.Button(button_frame, text="حفظ", command=save_changes, style="Save.TButton")
        save_button.pack(side='left', padx=5)
        style.configure("Cancel.TButton", background="#d13438", foreground="white")
        cancel_button = ttk.Button(button_frame, text="تراجع", command=cancel_changes, style="Cancel.TButton")
        cancel_button.pack(side='left', padx=5)

    def filter_company_combobox(self, event):
        cursor_pos = self.company_combobox.index(tk.INSERT)
        typed = self.company_combobox.get().strip().lower()
        filtered = ["الكل"] + [company for company in self.company_list if typed in company.lower()] if typed else ["الكل"] + self.company_list
        self.company_combobox['values'] = filtered
        if event.keysym not in ["Down", "Up"]:
            self.company_combobox.icursor(cursor_pos)

    def update_stores(self, stores):
        self.stores = [store[0] for store in stores]
        self.store_names = {store[0]: store[1] for store in stores}
        self.store_combobox['values'] = [self.store_names[store] for store in self.stores]
        self.store_combobox.set(self.store_names[self.stores[0]] if self.stores else "")
        if not self.stores:
            messagebox.showwarning("تحذير", "لا توجد مخازن في جدول STOCK_STOCK!")

    def test_relationships(self, connection_main):
        try:
            cur = connection_main.cursor()
            cur.execute("""
                SELECT p.PROD_ID, c.COP_NAME
                FROM PRODUCTS p
                LEFT JOIN COP_USERS c ON p.COP_ID = c.COP_ID
            """)
            cop_items = cur.fetchall()
            cop_matched = sum(1 for item in cop_items if item[1] is not None)

            cur.execute("""
                SELECT p.PROD_ID, s.TOTAL_QTY_ALL
                FROM PRODUCTS p
                LEFT JOIN STOCK_STOCK s ON p.PROD_ID = s.PROD_ID
            """)
            stock_items = cur.fetchall()
            stock_matched = sum(1 for item in stock_items if item[1] is not None)

            messagebox.showinfo("اختبار العلاقات",
                              f"عدد السجلات المتطابقة:\n"
                              f"مع COP_USERS: {cop_matched}/{len(cop_items)}\n"
                              f"مع STOCK_STOCK: {stock_matched}/{len(stock_items)}")
            cur.close()
        except Exception as e:
            logging.error(f"Error testing relationships: {str(e)}")
            messagebox.showerror("خطأ", f"فشل اختبار العلاقات: {str(e)}")




class CashDiscountTab:
    def __init__(self, notebook, get_connections):
        self.frame = ttk.Frame(notebook)
        notebook.add(self.frame, text="خصومات نقدية")
        self.get_connections = get_connections
        self.current_data = []
        self.sort_column = None
        self.sort_reverse = False

        main_frame = ttk.Frame(self.frame)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)

        title_label = ttk.Label(main_frame, text="إدارة الخصومات النقدية", font=('Segoe UI', 13, 'bold'))
        title_label.pack(pady=(0, 10))

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill='x', pady=5)

        add_button = ttk.Button(button_frame, text="إضافة خصم نقدي", command=self.open_add_cash_discount_window, style="Add.TButton")
        add_button.pack(side='left', padx=5)

        edit_button = ttk.Button(button_frame, text="تعديل", command=self.open_edit_cash_discount_window, style="Edit.TButton")
        edit_button.pack(side='left', padx=5)

        delete_button = ttk.Button(button_frame, text="حذف", command=self.delete_cash_discount, style="Delete.TButton")
        delete_button.pack(side='left', padx=5)

        reload_button = ttk.Button(button_frame, text="إعادة تحميل", command=self.load_cash_discount_data, style="Reload.TButton")
        reload_button.pack(side='left', padx=5)

        export_button = ttk.Button(button_frame, text="حفظ كـ Excel", command=self.export_to_excel, style="Export.TButton")
        export_button.pack(side='left', padx=5)

        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill='both', expand=True)

        columns = ("cash_discount_id", "cash_discount_name")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings')
        self.tree.heading("cash_discount_id", text="كود الخصم", anchor='center', command=lambda: self.sort_by_column("cash_discount_id"))
        self.tree.heading("cash_discount_name", text=" الخصم النقدي", anchor='center', command=lambda: self.sort_by_column("cash_discount_name"))

        self.tree.column("cash_discount_id", width=100, anchor='center')
        self.tree.column("cash_discount_name", width=200, anchor='center')

        scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side='right', fill='y')
        self.tree.pack(fill='both', expand=True)

        self.tree.bind('<Double-1>', self.open_edit_cash_discount_window)

    def sort_by_column(self, col):
        self.sort_reverse = not self.sort_reverse if self.sort_column == col else False
        self.sort_column = col

        col_index_map = {"cash_discount_id": 0, "cash_discount_name": 1}
        col_index = col_index_map[col]

        def sort_key(row):
            value = row[col_index]
            if value is None:
                return (0, 0)
            try:
                numeric_value = float(str(value).replace(',', ''))
                return (1, numeric_value)
            except (ValueError, AttributeError):
                return (2, str(value).lower())

        self.current_data.sort(key=sort_key, reverse=self.sort_reverse)
        self.update_treeview()
        logging.info(f"Sorted by column {col}, reverse={self.sort_reverse}")

    def update_treeview(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.current_data:
            self.tree.insert("", "end", values=row)

    def load_cash_discount_data(self):
        self.current_data = []
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            self.frame.after(0, lambda: messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!"))
            return
        cur = None
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT CASH_DISCOUNT_ID, CASH_DISCOUNT_NAME FROM CASH_DISCOUNTS")
            rows = cur.fetchall()
            self.current_data = list(rows)
            logging.info(f"Loaded {len(self.current_data)} cash discount records")
            self.update_treeview()
        except fdb.Error as e:
            error_msg = f"فشل تحميل بيانات الخصومات النقدية: {str(e)}"
            logging.error(error_msg)
            self.frame.after(0, lambda msg=error_msg: messagebox.showerror("خطأ", msg))
        finally:
            if cur is not None:
                cur.close()

    def export_to_excel(self):
        if not self.current_data:
            messagebox.showwarning("تحذير", "لا توجد بيانات لحفظها!")
            return

        df = pd.DataFrame(self.current_data, columns=["كود الخصم", "الخصم النقدي"])
        filename = f"CashDiscounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = os.path.join(os.getcwd(), filename)
        df.to_excel(filepath, index=False, engine='openpyxl')
        logging.info(f"Saved Cash Discounts to {filepath}")
        messagebox.showinfo("نجاح", f"تم حفظ الملف باسم {filename}")
        os.startfile(os.path.dirname(filepath))

    def open_add_cash_discount_window(self):
        add_window = tk.Toplevel(self.frame)
        add_window.title("إضافة خصم نقدي جديد")
        add_window.geometry("400x200")
        add_window.configure(bg="#f3f3f3")
        ttk.Label(add_window, text="إضافة خصم نقدي جديد", font=('Segoe UI', 13, 'bold')).pack(pady=10)

        fields = [
            ("كود الخصم:", ""),
            ("اسم الخصم:", "")
        ]
        entries = {}
        for label_text, value in fields:
            frame = ttk.Frame(add_window)
            frame.pack(fill='x', padx=10, pady=5)
            ttk.Label(frame, text=label_text, width=15, anchor='e').pack(side='right', padx=5)
            entry = ttk.Entry(frame)
            entry.pack(side='left', fill='x', expand=True, padx=5)
            entry.insert(0, value)
            entries[label_text] = entry

        def add_cash_discount():
            try:
                cash_discount_id = entries["كود الخصم:"].get()
                cash_discount_name = entries["اسم الخصم:"].get()
                if not cash_discount_id or not cash_discount_name:
                    messagebox.showerror("خطأ", "يرجى ملء جميع الحقول!")
                    return

                _, _, connection_dis = self.get_connections()
                if connection_dis:
                    cur = connection_dis.cursor()
                    cur.execute("INSERT INTO CASH_DISCOUNTS (CASH_DISCOUNT_ID, CASH_DISCOUNT_NAME) VALUES (?, ?)",
                               (cash_discount_id, cash_discount_name))
                    connection_dis.commit()
                    cur.close()
                    self.load_cash_discount_data()
                    messagebox.showinfo("نجاح", "تم إضافة الخصم النقدي بنجاح!")
                    add_window.destroy()
            except fdb.Error as e:
                error_msg = f"فشل إضافة الخصم النقدي: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)

        ttk.Button(add_window, text="إضافة", command=add_cash_discount, style="Add.TButton").pack(pady=10)

    def open_edit_cash_discount_window(self, event=None):
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("تحذير", "يرجى اختيار خصم نقدي لتعديله!")
            return

        item = self.tree.item(selected_item[0])
        cash_discount_id, cash_discount_name = item['values']

        edit_window = tk.Toplevel(self.frame)
        edit_window.title("تعديل الخصم النقدي")
        edit_window.geometry("400x200")
        edit_window.configure(bg="#f3f3f3")

        ttk.Label(edit_window, text="تعديل خصم نقدي", font=('Segoe UI', 13, 'bold')).pack(pady=10)

        fields = [
            ("كود الخصم:", cash_discount_id, False),
            ("اسم الخصم:", cash_discount_name, True)
        ]
        entries = {}
        for label_text, value, editable in fields:
            frame = ttk.Frame(edit_window)
            frame.pack(fill='x', padx=10, pady=5)
            ttk.Label(frame, text=label_text, width=15, anchor='e').pack(side='right', padx=5)
            entry = ttk.Entry(frame)
            entry.pack(side='left', fill='x', expand=True, padx=5)
            entry.insert(0, str(value))
            if not editable:
                entry.config(state='disabled')
            entries[label_text] = entry

        def save_changes():
            try:
                new_name = entries["اسم الخصم:"].get()
                _, _, connection_dis = self.get_connections()
                if not connection_dis:
                    messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
                    return
                cur = connection_dis.cursor()
                cur.execute("UPDATE CASH_DISCOUNTS SET CASH_DISCOUNT_NAME = ? WHERE CASH_DISCOUNT_ID = ?",
                           (new_name, cash_discount_id))
                connection_dis.commit()
                cur.close()
                self.load_cash_discount_data()
                messagebox.showinfo("نجاح", "تم تعديل الخصم النقدي بنجاح!")
                edit_window.destroy()
            except fdb.Error as e:
                error_msg = f"فشل تعديل الخصم النقدي: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)

        ttk.Button(edit_window, text="حفظ", command=save_changes, style="Save.TButton").pack(pady=10)

    def delete_cash_discount(self):
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("تحذير", "يرجى اختيار خصم نقدي للحذف!")
            return

        item = self.tree.item(selected_item[0])
        cash_discount_id = item['values'][0]

        if messagebox.askyesno("تأكيد الحذف", f"هل أنت متأكد من حذف الخصم النقدي {cash_discount_id}؟"):
            _, _, connection_dis = self.get_connections()
            if not connection_dis:
                messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
                return
            cur = None
            try:
                cur = connection_dis.cursor()
                cur.execute("DELETE FROM CASH_DISCOUNTS WHERE CASH_DISCOUNT_ID = ?", (cash_discount_id,))
                connection_dis.commit()
                logging.info(f"Deleted cash discount with ID: {cash_discount_id}")
                self.load_cash_discount_data()
                messagebox.showinfo("نجاح", "تم حذف الخصم النقدي بنجاح!")
            except fdb.Error as e:
                error_msg = f"فشل حذف الخصم النقدي: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)
            finally:
                if cur is not None:
                    cur.close()

    def insert_cash_discount_data(self):
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            logging.error("No connection to dis_db for inserting cash discount data")
            return

        cur = None
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT COUNT(*) FROM CASH_DISCOUNTS")
            count = cur.fetchone()[0]
            if count == 0:
                default_data = [
                    ("CASH001", "خصم نقدي 5%"),
                    ("CASH002", "خصم نقدي 10%")
                ]
                cur.executemany("INSERT INTO CASH_DISCOUNTS (CASH_DISCOUNT_ID, CASH_DISCOUNT_NAME) VALUES (?, ?)", default_data)
                connection_dis.commit()
                logging.info(f"Inserted {len(default_data)} default cash discounts")
            else:
                logging.info(f"Found {count} cash discounts, skipping insertion")
        except fdb.Error as e:
            error_msg = f"فشل إدخال بيانات الخصومات النقدية: {str(e)}"
            logging.error(error_msg)
            self.frame.after(0, lambda msg=error_msg: messagebox.showerror("خطأ", msg))
        finally:
            if cur is not None:
                cur.close()


class ProductTypeTab:
    def __init__(self, notebook, get_connections):
        self.frame = ttk.Frame(notebook)
        notebook.add(self.frame, text="أنواع المنتجات")
        self.get_connections = get_connections
        self.current_data = []
        self.sort_column = None
        self.sort_reverse = False

        main_frame = ttk.Frame(self.frame)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)

        title_label = ttk.Label(main_frame, text="إدارة أنواع المنتجات", font=('Segoe UI', 13, 'bold'))
        title_label.pack(pady=(0, 10))

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill='x', pady=5)

        add_button = ttk.Button(button_frame, text="إضافة نوع منتج", command=self.open_add_product_type_window, style="Add.TButton")
        add_button.pack(side='left', padx=5)

        edit_button = ttk.Button(button_frame, text="تعديل", command=self.open_edit_product_type_window, style="Edit.TButton")
        edit_button.pack(side='left', padx=5)

        delete_button = ttk.Button(button_frame, text="حذف", command=self.delete_product_type, style="Delete.TButton")
        delete_button.pack(side='left', padx=5)

        reload_button = ttk.Button(button_frame, text="إعادة تحميل", command=self.load_product_type_data, style="Reload.TButton")
        reload_button.pack(side='left', padx=5)

        export_button = ttk.Button(button_frame, text="حفظ كـ Excel", command=self.export_to_excel, style="Export.TButton")
        export_button.pack(side='left', padx=5)

        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill='both', expand=True)

        columns = ("product_type_id", "product_type_name")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings')
        self.tree.heading("product_type_id", text="كود النوع", anchor='center', command=lambda: self.sort_by_column("product_type_id"))
        self.tree.heading("product_type_name", text="اسم النوع", anchor='center', command=lambda: self.sort_by_column("product_type_name"))

        self.tree.column("product_type_id", width=100, anchor='center')
        self.tree.column("product_type_name", width=200, anchor='center')

        scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side='right', fill='y')
        self.tree.pack(fill='both', expand=True)

        self.tree.bind('<Double-1>', self.open_edit_product_type_window)

    def sort_by_column(self, col):
        self.sort_reverse = not self.sort_reverse if self.sort_column == col else False
        self.sort_column = col

        col_index_map = {"product_type_id": 0, "product_type_name": 1}
        col_index = col_index_map[col]

        def sort_key(row):
            value = row[col_index]
            if value is None:
                return (0, 0)
            try:
                numeric_value = float(str(value).replace(',', ''))
                return (1, numeric_value)
            except (ValueError, AttributeError):
                return (2, str(value).lower())

        self.current_data.sort(key=sort_key, reverse=self.sort_reverse)
        self.update_treeview()
        logging.info(f"Sorted by column {col}, reverse={self.sort_reverse}")

    def update_treeview(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.current_data:
            self.tree.insert("", "end", values=row)

    def load_product_type_data(self):
        self.current_data = []
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            self.frame.after(0, lambda: messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!"))
            return
        cur = None
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT PRODUCT_TYPE_ID, PRODUCT_TYPE_NAME FROM PRODUCT_TYPES")
            rows = cur.fetchall()
            self.current_data = list(rows)
            logging.info(f"Loaded {len(self.current_data)} product type records")
            self.update_treeview()
        except fdb.Error as e:
            error_msg = f"فشل تحميل بيانات أنواع المنتجات: {str(e)}"
            logging.error(error_msg)
            self.frame.after(0, lambda msg=error_msg: messagebox.showerror("خطأ", msg))
        finally:
            if cur is not None:
                cur.close()

    def export_to_excel(self):
        if not self.current_data:
            messagebox.showwarning("تحذير", "لا توجد بيانات لحفظها!")
            return

        df = pd.DataFrame(self.current_data, columns=["كود النوع", "اسم النوع"])
        filename = f"ProductTypes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = os.path.join(os.getcwd(), filename)
        df.to_excel(filepath, index=False, engine='openpyxl')
        logging.info(f"Saved Product Types to {filepath}")
        messagebox.showinfo("نجاح", f"تم حفظ الملف باسم {filename}")
        os.startfile(os.path.dirname(filepath))

    def open_add_product_type_window(self):
        add_window = tk.Toplevel(self.frame)
        add_window.title("إضافة نوع منتج جديد")
        add_window.geometry("400x200")
        add_window.configure(bg="#f3f3f3")
        ttk.Label(add_window, text="إضافة نوع منتج جديد", font=('Segoe UI', 13, 'bold')).pack(pady=10)

        fields = [
            ("كود النوع:", ""),
            ("اسم النوع:", "")
        ]
        entries = {}
        for label_text, value in fields:
            frame = ttk.Frame(add_window)
            frame.pack(fill='x', padx=10, pady=5)
            ttk.Label(frame, text=label_text, width=15, anchor='e').pack(side='right', padx=5)
            entry = ttk.Entry(frame)
            entry.pack(side='left', fill='x', expand=True, padx=5)
            entry.insert(0, value)
            entries[label_text] = entry

        def add_product_type():
            try:
                product_type_id = entries["كود النوع:"].get()
                product_type_name = entries["اسم النوع:"].get()
                if not product_type_id or not product_type_name:
                    messagebox.showerror("خطأ", "يرجى ملء جميع الحقول!")
                    return

                _, _, connection_dis = self.get_connections()
                if connection_dis:
                    cur = connection_dis.cursor()
                    cur.execute("INSERT INTO PRODUCT_TYPES (PRODUCT_TYPE_ID, PRODUCT_TYPE_NAME) VALUES (?, ?)",
                               (product_type_id, product_type_name))
                    connection_dis.commit()
                    cur.close()
                    self.load_product_type_data()
                    messagebox.showinfo("نجاح", "تم إضافة نوع المنتج بنجاح!")
                    add_window.destroy()
            except fdb.Error as e:
                error_msg = f"فشل إضافة نوع المنتج: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)

        ttk.Button(add_window, text="إضافة", command=add_product_type, style="Add.TButton").pack(pady=10)

    def open_edit_product_type_window(self, event=None):
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("تحذير", "يرجى اختيار نوع منتج لتعديله!")
            return

        item = self.tree.item(selected_item[0])
        product_type_id, product_type_name = item['values']

        edit_window = tk.Toplevel(self.frame)
        edit_window.title("تعديل نوع المنتج")
        edit_window.geometry("400x200")
        edit_window.configure(bg="#f3f3f3")

        ttk.Label(edit_window, text="تعديل نوع منتج", font=('Segoe UI', 13, 'bold')).pack(pady=10)

        fields = [
            ("كود النوع:", product_type_id, False),
            ("اسم النوع:", product_type_name, True)
        ]
        entries = {}
        for label_text, value, editable in fields:
            frame = ttk.Frame(edit_window)
            frame.pack(fill='x', padx=10, pady=5)
            ttk.Label(frame, text=label_text, width=15, anchor='e').pack(side='right', padx=5)
            entry = ttk.Entry(frame)
            entry.pack(side='left', fill='x', expand=True, padx=5)
            entry.insert(0, str(value))
            if not editable:
                entry.config(state='disabled')
            entries[label_text] = entry

        def save_changes():
            try:
                new_name = entries["اسم النوع:"].get()
                _, _, connection_dis = self.get_connections()
                if not connection_dis:
                    messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
                    return
                cur = connection_dis.cursor()
                cur.execute("UPDATE PRODUCT_TYPES SET PRODUCT_TYPE_NAME = ? WHERE PRODUCT_TYPE_ID = ?",
                           (new_name, product_type_id))
                connection_dis.commit()
                cur.close()
                self.load_product_type_data()
                messagebox.showinfo("نجاح", "تم تعديل نوع المنتج بنجاح!")
                edit_window.destroy()
            except fdb.Error as e:
                error_msg = f"فشل تعديل نوع المنتج: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)

        ttk.Button(edit_window, text="حفظ", command=save_changes, style="Save.TButton").pack(pady=10)

    def delete_product_type(self):
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("تحذير", "يرجى اختيار نوع منتج للحذف!")
            return

        item = self.tree.item(selected_item[0])
        product_type_id = item['values'][0]

        if messagebox.askyesno("تأكيد الحذف", f"هل أنت متأكد من حذف نوع المنتج {product_type_id}؟"):
            _, _, connection_dis = self.get_connections()
            if not connection_dis:
                messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
                return
            cur = None
            try:
                cur = connection_dis.cursor()
                cur.execute("DELETE FROM PRODUCT_TYPES WHERE PRODUCT_TYPE_ID = ?", (product_type_id,))
                connection_dis.commit()
                logging.info(f"Deleted product type with ID: {product_type_id}")
                self.load_product_type_data()
                messagebox.showinfo("نجاح", "تم حذف نوع المنتج بنجاح!")
            except fdb.Error as e:
                error_msg = f"فشل حذف نوع المنتج: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)
            finally:
                if cur is not None:
                    cur.close()

    def insert_product_type_data(self):
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            logging.error("No connection to dis_db for inserting product type data")
            return

        cur = None
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT COUNT(*) FROM PRODUCT_TYPES")
            count = cur.fetchone()[0]
            if count == 0:
                default_data = [
                    ("TYPE001", "أدوية"),
                    ("TYPE002", "مستحضرات تجميل")
                ]
                cur.executemany("INSERT INTO PRODUCT_TYPES (PRODUCT_TYPE_ID, PRODUCT_TYPE_NAME) VALUES (?, ?)", default_data)
                connection_dis.commit()
                logging.info(f"Inserted {len(default_data)} default product types")
            else:
                logging.info(f"Found {count} product types, skipping insertion")
        except fdb.Error as e:
            error_msg = f"فشل إدخال بيانات أنواع المنتجات: {str(e)}"
            logging.error(error_msg)
            self.frame.after(0, lambda msg=error_msg: messagebox.showerror("خطأ", msg))
        finally:
            if cur is not None:
                cur.close()


class TaxTab:
    def __init__(self, notebook, get_connections):
        self.frame = ttk.Frame(notebook)
        notebook.add(self.frame, text="الضرائب")
        self.get_connections = get_connections
        self.current_data = []
        self.sort_column = None
        self.sort_reverse = False

        main_frame = ttk.Frame(self.frame)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)

        title_label = ttk.Label(main_frame, text="إدارة الضرائب", font=('Segoe UI', 13, 'bold'))
        title_label.pack(pady=(0, 10))

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill='x', pady=5)

        add_button = ttk.Button(button_frame, text="إضافة ضريبة", command=self.open_add_tax_window, style="Add.TButton")
        add_button.pack(side='left', padx=5)

        edit_button = ttk.Button(button_frame, text="تعديل", command=self.open_edit_tax_window, style="Edit.TButton")
        edit_button.pack(side='left', padx=5)

        delete_button = ttk.Button(button_frame, text="حذف", command=self.delete_tax, style="Delete.TButton")
        delete_button.pack(side='left', padx=5)

        reload_button = ttk.Button(button_frame, text="إعادة تحميل", command=self.load_tax_data, style="Reload.TButton")
        reload_button.pack(side='left', padx=5)

        export_button = ttk.Button(button_frame, text="حفظ كـ Excel", command=self.export_to_excel, style="Export.TButton")
        export_button.pack(side='left', padx=5)

        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill='both', expand=True)

        columns = ("tax_id", "tax_rate")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings')
        self.tree.heading("tax_id", text="كود الضريبة", anchor='center', command=lambda: self.sort_by_column("tax_id"))
        self.tree.heading("tax_rate", text="نسبة الضريبة", anchor='center', command=lambda: self.sort_by_column("tax_rate"))

        self.tree.column("tax_id", width=100, anchor='center')
        self.tree.column("tax_rate", width=100, anchor='center')

        scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side='right', fill='y')
        self.tree.pack(fill='both', expand=True)

        self.tree.bind('<Double-1>', self.open_edit_tax_window)

    def sort_by_column(self, col):
        self.sort_reverse = not self.sort_reverse if self.sort_column == col else False
        self.sort_column = col

        col_index_map = {"tax_id": 0, "tax_rate": 1}
        col_index = col_index_map[col]

        def sort_key(row):
            value = row[col_index]
            if value is None:
                return (0, 0)
            try:
                numeric_value = float(str(value).replace(',', ''))
                return (1, numeric_value)
            except (ValueError, AttributeError):
                return (2, str(value).lower())

        self.current_data.sort(key=sort_key, reverse=self.sort_reverse)
        self.update_treeview()
        logging.info(f"Sorted by column {col}, reverse={self.sort_reverse}")

    def update_treeview(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.current_data:
            self.tree.insert("", "end", values=row)

    def load_tax_data(self):
        self.current_data = []
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            self.frame.after(0, lambda: messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!"))
            return
        cur = None
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT TAX_ID, TAX_RATE FROM TAXES")
            rows = cur.fetchall()
            self.current_data = list(rows)
            logging.info(f"Loaded {len(self.current_data)} tax records")
            self.update_treeview()
        except fdb.Error as e:
            error_msg = f"فشل تحميل بيانات الضرائب: {str(e)}"
            logging.error(error_msg)
            self.frame.after(0, lambda msg=error_msg: messagebox.showerror("خطأ", msg))
        finally:
            if cur is not None:
                cur.close()

    def export_to_excel(self):
        if not self.current_data:
            messagebox.showwarning("تحذير", "لا توجد بيانات لحفظها!")
            return

        df = pd.DataFrame(self.current_data, columns=["كود الضريبة", "نسبة الضريبة"])
        filename = f"Taxes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = os.path.join(os.getcwd(), filename)
        df.to_excel(filepath, index=False, engine='openpyxl')
        logging.info(f"Saved Taxes to {filepath}")
        messagebox.showinfo("نجاح", f"تم حفظ الملف باسم {filename}")
        os.startfile(os.path.dirname(filepath))

    def open_add_tax_window(self):
        add_window = tk.Toplevel(self.frame)
        add_window.title("إضافة ضريبة جديدة")
        add_window.geometry("400x200")
        add_window.configure(bg="#f3f3f3")
        ttk.Label(add_window, text="إضافة ضريبة جديدة", font=('Segoe UI', 13, 'bold')).pack(pady=10)

        fields = [
            ("كود الضريبة:", ""),
            ("نسبة الضريبة:", "")
        ]
        entries = {}
        for label_text, value in fields:
            frame = ttk.Frame(add_window)
            frame.pack(fill='x', padx=10, pady=5)
            ttk.Label(frame, text=label_text, width=15, anchor='e').pack(side='right', padx=5)
            entry = ttk.Entry(frame)
            entry.pack(side='left', fill='x', expand=True, padx=5)
            entry.insert(0, value)
            entries[label_text] = entry

        def add_tax():
            try:
                tax_id = entries["كود الضريبة:"].get()
                tax_rate = float(entries["نسبة الضريبة:"].get())
                if not tax_id:
                    messagebox.showerror("خطأ", "يرجى ملء كود الضريبة!")
                    return

                _, _, connection_dis = self.get_connections()
                if connection_dis:
                    cur = connection_dis.cursor()
                    cur.execute("INSERT INTO TAXES (TAX_ID, TAX_RATE) VALUES (?, ?)",
                               (tax_id, tax_rate))
                    connection_dis.commit()
                    cur.close()
                    self.load_tax_data()
                    messagebox.showinfo("نجاح", "تم إضافة الضريبة بنجاح!")
                    add_window.destroy()
            except (fdb.Error, ValueError) as e:
                error_msg = f"فشل إضافة الضريبة: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)

        ttk.Button(add_window, text="إضافة", command=add_tax, style="Add.TButton").pack(pady=10)

    def open_edit_tax_window(self, event=None):
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("تحذير", "يرجى اختيار ضريبة لتعديلها!")
            return

        item = self.tree.item(selected_item[0])
        tax_id, tax_rate = item['values']

        edit_window = tk.Toplevel(self.frame)
        edit_window.title("تعديل الضريبة")
        edit_window.geometry("400x200")
        edit_window.configure(bg="#f3f3f3")

        ttk.Label(edit_window, text="تعديل ضريبة", font=('Segoe UI', 13, 'bold')).pack(pady=10)

        fields = [
            ("كود الضريبة:", tax_id, False),
            ("نسبة الضريبة:", tax_rate, True)
        ]
        entries = {}
        for label_text, value, editable in fields:
            frame = ttk.Frame(edit_window)
            frame.pack(fill='x', padx=10, pady=5)
            ttk.Label(frame, text=label_text, width=15, anchor='e').pack(side='right', padx=5)
            entry = ttk.Entry(frame)
            entry.pack(side='left', fill='x', expand=True, padx=5)
            entry.insert(0, str(value))
            if not editable:
                entry.config(state='disabled')
            entries[label_text] = entry

        def save_changes():
            try:
                new_rate = float(entries["نسبة الضريبة:"].get())
                _, _, connection_dis = self.get_connections()
                if not connection_dis:
                    messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
                    return
                cur = connection_dis.cursor()
                cur.execute("UPDATE TAXES SET TAX_RATE = ? WHERE TAX_ID = ?",
                           (new_rate, tax_id))
                connection_dis.commit()
                cur.close()
                self.load_tax_data()
                messagebox.showinfo("نجاح", "تم تعديل الضريبة بنجاح!")
                edit_window.destroy()
            except (fdb.Error, ValueError) as e:
                error_msg = f"فشل تعديل الضريبة: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)

        ttk.Button(edit_window, text="حفظ", command=save_changes, style="Save.TButton").pack(pady=10)

    def delete_tax(self):
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("تحذير", "يرجى اختيار ضريبة للحذف!")
            return

        item = self.tree.item(selected_item[0])
        tax_id = item['values'][0]

        if messagebox.askyesno("تأكيد الحذف", f"هل أنت متأكد من حذف الضريبة {tax_id}؟"):
            _, _, connection_dis = self.get_connections()
            if not connection_dis:
                messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
                return
            cur = None
            try:
                cur = connection_dis.cursor()
                cur.execute("DELETE FROM TAXES WHERE TAX_ID = ?", (tax_id,))
                connection_dis.commit()
                logging.info(f"Deleted tax with ID: {tax_id}")
                self.load_tax_data()
                messagebox.showinfo("نجاح", "تم حذف الضريبة بنجاح!")
            except fdb.Error as e:
                error_msg = f"فشل حذف الضريبة: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)
            finally:
                if cur is not None:
                    cur.close()

    def insert_tax_data(self):
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            logging.error("No connection to dis_db for inserting tax data")
            return

        cur = None
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT COUNT(*) FROM TAXES")
            count = cur.fetchone()[0]
            if count == 0:
                default_data = [
                    ("TAX001", 5.0),
                    ("TAX002", 10.0)
                ]
                cur.executemany("INSERT INTO TAXES (TAX_ID, TAX_RATE) VALUES (?, ?)", default_data)
                connection_dis.commit()
                logging.info(f"Inserted {len(default_data)} default taxes")
            else:
                logging.info(f"Found {count} taxes, skipping insertion")
        except fdb.Error as e:
            error_msg = f"فشل إدخال بيانات الضرائب: {str(e)}"
            logging.error(error_msg)
            self.frame.after(0, lambda msg=error_msg: messagebox.showerror("خطأ", msg))
        finally:
            if cur is not None:
                cur.close()


class SuppliersTab:
    def __init__(self, notebook, get_connections):
        self.frame = ttk.Frame(notebook)
        notebook.add(self.frame, text="الموردين")
        self.get_connections = get_connections
        self.current_data = []
        self.sort_column = None
        self.sort_reverse = False

        main_frame = ttk.Frame(self.frame)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)

        title_label = ttk.Label(main_frame, text="إدارة الموردين", font=('Segoe UI', 13, 'bold'))
        title_label.pack(pady=(0, 10))

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill='x', pady=5)

        add_button = ttk.Button(button_frame, text="إضافة مورد", command=self.open_add_supplier_window, style="Add.TButton")
        add_button.pack(side='left', padx=5)

        edit_button = ttk.Button(button_frame, text="تعديل", command=self.open_edit_supplier_window, style="Edit.TButton")
        edit_button.pack(side='left', padx=5)

        delete_button = ttk.Button(button_frame, text="حذف", command=self.delete_supplier, style="Delete.TButton")
        delete_button.pack(side='left', padx=5)

        reload_button = ttk.Button(button_frame, text="إعادة تحميل", command=self.load_suppliers_data, style="Reload.TButton")
        reload_button.pack(side='left', padx=5)

        export_button = ttk.Button(button_frame, text="حفظ كـ Excel", command=self.export_to_excel, style="Export.TButton")
        export_button.pack(side='left', padx=5)

        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill='both', expand=True)

        # إعداد Treeview مع الأعمدة الأساسية
        columns = ("supplier_id", "supplier_name", "cash_discount_id", "agreement_discount_id")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings')
        self.tree.heading("supplier_id", text="كود المورد", command=lambda: self.sort_by_column("supplier_id"))
        self.tree.heading("supplier_name", text="اسم المورد", command=lambda: self.sort_by_column("supplier_name"))
        self.tree.heading("cash_discount_id", text="كود الخصم النقدي", command=lambda: self.sort_by_column("cash_discount_id"))
        self.tree.heading("agreement_discount_id", text="كود خصم الاتفاق", command=lambda: self.sort_by_column("agreement_discount_id"))


        self.tree.column("supplier_id", width=100, anchor='center')
        self.tree.column("supplier_name", width=200, anchor='center')
        self.tree.column("cash_discount_id", width=150, anchor='center')
        self.tree.column("agreement_discount_id", width=150, anchor='center')

        scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side='right', fill='y')
        self.tree.pack(fill='both', expand=True)

        self.tree.bind('<Double-1>', self.open_edit_supplier_window)

        # تحميل البيانات عند التهيئة
        self.load_suppliers_data()

    def get_cash_discounts(self):
        """جلب قائمة معرفات الخصومات النقدية من جدول CASH_DISCOUNTS"""
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            logging.error("No connection to dis_db for fetching cash discounts")
            return []
        cur = None
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT CASH_DISCOUNT_ID FROM CASH_DISCOUNTS")
            return [row[0] for row in cur.fetchall()]
        except fdb.Error as e:
            logging.error(f"Failed to fetch cash discounts: {str(e)}")
            return []
        finally:
            if cur is not None:
                cur.close()

    def get_agreement_discounts(self):
        """جلب قائمة معرفات خصومات الاتفاق من جدول AGREEMENT_DISCOUNTS"""
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            logging.error("No connection to dis_db for fetching agreement discounts")
            return []
        cur = None
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT AGREEMENT_DISCOUNT_ID FROM AGREEMENT_DISCOUNTS")
            return [row[0] for row in cur.fetchall()]
        except fdb.Error as e:
            logging.error(f"Failed to fetch agreement discounts: {str(e)}")
            return []
        finally:
            if cur is not None:
                cur.close()

    def sort_by_column(self, col):
        """فرز البيانات حسب العمود المحدد"""
        self.sort_reverse = not self.sort_reverse if self.sort_column == col else False
        self.sort_column = col

        col_index_map = {
            "supplier_id": 0,
            "supplier_name": 1,
            "cash_discount_id": 2,
            "agreement_discount_id": 3
        }
        col_index = col_index_map[col]

        def sort_key(row):
            value = row[col_index]
            if value is None:
                return (0, 0)
            try:
                numeric_value = float(str(value).replace(',', ''))
                return (1, numeric_value)
            except (ValueError, AttributeError):
                return (2, str(value).lower())

        self.current_data.sort(key=sort_key, reverse=self.sort_reverse)
        self.update_treeview()
        logging.info(f"تم الفرز حسب العمود {col}, عكسي={self.sort_reverse}")

    def update_treeview(self):
        """تحديث عرض Treeview بالبيانات الحالية"""
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.current_data:
            self.tree.insert("", "end", values=row)

    def load_suppliers_data(self):
        """تحميل بيانات الموردين من قاعدة البيانات"""
        self.current_data = []
        if not callable(self.get_connections):  # التحقق مما إذا كان get_connections دالة
            self.frame.after(0, lambda: messagebox.showerror("خطأ", "لم يتم تهيئة اتصال قاعدة البيانات!"))
            return
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            self.frame.after(0, lambda: messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!"))
            return
        cur = None
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT SUPPLIER_ID, SUPPLIER_NAME, CASH_DISCOUNT_ID, AGREEMENT_DISCOUNT_ID FROM SUPPLIERS")
            rows = cur.fetchall()
            self.current_data = [(row[0], row[1], row[2] or "غير مرتبط", row[3] or "غير مرتبط") for row in rows]
            logging.info(f"تم تحميل {len(self.current_data)} سجل مورد")
            self.update_treeview()
        except fdb.Error as e:
            error_msg = f"فشل تحميل بيانات الموردين: {str(e)}"
            logging.error(error_msg)
            self.frame.after(0, lambda msg=error_msg: messagebox.showerror("خطأ", msg))
        finally:
            if cur is not None:
                cur.close()

    def export_to_excel(self):
        """تصدير البيانات إلى ملف Excel"""
        if not self.current_data:
            messagebox.showwarning("تحذير", "لا توجد بيانات لحفظها!")
            return

        df = pd.DataFrame(self.current_data, columns=["كود المورد", "اسم المورد", "كود الخصم النقدي", "كود خصم الاتفاق"])
        filename = f"Suppliers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = os.path.join(os.getcwd(), filename)
        df.to_excel(filepath, index=False, engine='openpyxl')
        logging.info(f"تم حفظ الموردين في {filepath}")
        messagebox.showinfo("نجاح", f"تم حفظ الملف باسم {filename}")
        os.startfile(os.path.dirname(filepath))

    def open_add_supplier_window(self):
        """فتح نافذة لإضافة مورد جديد"""
        add_window = tk.Toplevel(self.frame)
        add_window.title("إضافة مورد جديد")
        add_window.geometry("400x400")
        add_window.configure(bg="#f3f3f3")
        ttk.Label(add_window, text="إضافة مورد جديد", font=('Segoe UI', 13, 'bold')).pack(pady=10)

        def generate_supplier_id():
            _, _, connection_dis = self.get_connections()
            if not connection_dis:
                return "SUP001"
            try:
                cur = connection_dis.cursor()
                cur.execute("SELECT MAX(SUPPLIER_ID) FROM SUPPLIERS")
                max_id = cur.fetchone()[0]
                cur.close()
                if max_id and max_id.startswith("SUP"):
                    num = int(max_id.replace("SUP", "")) + 1
                    return f"SUP{num:03d}"
                return "SUP001"
            except (fdb.Error, ValueError) as e:
                logging.error(f"خطأ في توليد كود المورد: {str(e)}")
                return "SUP001"

        fields = [
            ("كود المورد:", generate_supplier_id(), False),
            ("اسم المورد:", "", True),
            ("كود الخصم النقدي:", "", True),
            ("كود خصم الاتفاق:", "", True)
        ]
        entries = {}
        cash_discounts = self.get_cash_discounts()
        agreement_discounts = self.get_agreement_discounts()

        for label_text, value, editable in fields:
            frame = ttk.Frame(add_window)
            frame.pack(fill='x', padx=10, pady=5)
            ttk.Label(frame, text=label_text, width=15, anchor='e').pack(side='right', padx=5)
            if label_text == "كود الخصم النقدي:":
                entry = ttk.Combobox(frame, values=cash_discounts)
            elif label_text == "كود خصم الاتفاق:":
                entry = ttk.Combobox(frame, values=agreement_discounts)
            else:
                entry = ttk.Entry(frame)
            entry.pack(side='left', fill='x', expand=True, padx=5)
            entry.insert(0, value)
            if not editable:
                entry.config(state='disabled')
            entries[label_text] = entry

        ttk.Label(add_window, text="اختر الخصم النقدي:").pack(pady=5)
        cash_discount_combo = ttk.Combobox(add_window, state="readonly")
        cash_discount_combo.pack(fill='x', padx=10, pady=5)

        ttk.Label(add_window, text="نسبة الخصم:").pack(pady=5)
        discount_rate_entry = ttk.Entry(add_window)
        discount_rate_entry.pack(fill='x', padx=10, pady=5)
        discount_rate_entry.insert(0, "0.0")

        _, _, connection_dis = self.get_connections()
        if connection_dis:
            try:
                cur = connection_dis.cursor()
                cur.execute("SELECT CASH_DISCOUNT_ID, CASH_DISCOUNT_NAME FROM CASH_DISCOUNTS")
                cash_discounts = [(row[0], row[1]) for row in cur.fetchall()]
                cash_discount_combo['values'] = [f"{id} - {name}" for id, name in cash_discounts]
                cur.close()
            except fdb.Error as e:
                logging.error(f"خطأ في جلب الخصومات النقدية: {str(e)}")
                messagebox.showerror("خطأ", f"فشل جلب الخصومات النقدية: {str(e)}")

        def add_supplier():
            try:
                supplier_id = entries["كود المورد:"].get()
                supplier_name = entries["اسم المورد:"].get()
                cash_discount_id = entries["كود الخصم النقدي:"].get() or None
                agreement_discount_id = entries["كود خصم الاتفاق:"].get() or None
                if not supplier_id or not supplier_name:
                    messagebox.showerror("خطأ", "يرجى ملء كود المورد واسم المورد!")
                    return

                _, _, connection_dis = self.get_connections()
                if connection_dis:
                    cur = connection_dis.cursor()
                    cur.execute("INSERT INTO SUPPLIERS (SUPPLIER_ID, SUPPLIER_NAME, CASH_DISCOUNT_ID, AGREEMENT_DISCOUNT_ID) VALUES (?, ?, ?, ?)",
                               (supplier_id, supplier_name, cash_discount_id, agreement_discount_id))
                    connection_dis.commit()
                    cur.close()
                    self.load_suppliers_data()
                    messagebox.showinfo("نجاح", "تم إضافة المورد بنجاح!")
                    add_window.destroy()
            except fdb.Error as e:
                error_msg = f"فشل إضافة المورد: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)

        ttk.Button(add_window, text="إضافة", command=add_supplier, style="Add.TButton").pack(pady=10)

    def open_edit_supplier_window(self, event=None):
        """فتح نافذة لتعديل مورد موجود"""
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("تحذير", "يرجى اختيار مورد لتعديله!")
            return

        item = self.tree.item(selected_item[0])
        supplier_id, supplier_name, cash_discount_id, agreement_discount_id = item['values']

        edit_window = tk.Toplevel(self.frame)
        edit_window.title("تعديل المورد")
        edit_window.geometry("400x450")
        edit_window.configure(bg="#f3f3f3")

        ttk.Label(edit_window, text="تعديل المورد", font=('Segoe UI', 13, 'bold')).pack(pady=10)

        fields = [
            ("كود المورد:", supplier_id, False),
            ("اسم المورد:", supplier_name, True),
            ("كود الخصم النقدي:", cash_discount_id or "", True),
            ("كود خصم الاتفاق:", agreement_discount_id or "", True)
        ]
        entries = {}
        cash_discounts = self.get_cash_discounts()
        agreement_discounts = self.get_agreement_discounts()

        for label_text, value, editable in fields:
            frame = ttk.Frame(edit_window)
            frame.pack(fill='x', padx=10, pady=5)
            ttk.Label(frame, text=label_text, width=15, anchor='e').pack(side='right', padx=5)
            if label_text == "كود الخصم النقدي:":
                entry = ttk.Combobox(frame, values=cash_discounts)
            elif label_text == "كود خصم الاتفاق:":
                entry = ttk.Combobox(frame, values=agreement_discounts)
            else:
                entry = ttk.Entry(frame)
            entry.pack(side='left', fill='x', expand=True, padx=5)
            entry.insert(0, str(value))
            if not editable:
                entry.config(state='disabled')
            entries[label_text] = entry

        discount_frame = ttk.Frame(edit_window)
        discount_frame.pack(fill='both', padx=10, pady=10)

        ttk.Label(discount_frame, text="الخصومات الحالية:").pack(pady=5)
        discount_listbox = tk.Listbox(discount_frame, height=5)
        discount_listbox.pack(fill='x', pady=5)

        # جلب الخصومات الحالية للمورد
        existing_discounts = []
        _, _, connection_dis = self.get_connections()
        if connection_dis:
            try:
                cur = connection_dis.cursor()
                cur.execute("""
                    SELECT scd.CASH_DISCOUNT_ID, scd.DISCOUNT_RATE, cd.CASH_DISCOUNT_NAME
                    FROM SUPPLIER_CASH_DISCOUNTS scd
                    JOIN CASH_DISCOUNTS cd ON scd.CASH_DISCOUNT_ID = cd.CASH_DISCOUNT_ID
                    WHERE scd.SUPPLIER_ID = ?
                """, (supplier_id,))
                for row in cur.fetchall():
                    cash_discount_id, discount_rate, cash_discount_name = row
                    discount_listbox.insert(tk.END, f"{cash_discount_id} - {cash_discount_name} | نسبة: {discount_rate}")
                    existing_discounts.append((cash_discount_id, discount_rate, cash_discount_name))
                cur.close()
            except fdb.Error as e:
                logging.error(f"خطأ في جلب الخصومات الحالية: {str(e)}")

        ttk.Label(discount_frame, text="اختر خصم نقدي جديد:").pack(pady=5)
        cash_discount_combo = ttk.Combobox(discount_frame, state="readonly")
        cash_discount_combo.pack(fill='x', pady=5)

        ttk.Label(discount_frame, text="نسبة الخصم الجديد:").pack(pady=5)
        discount_rate_entry = ttk.Entry(discount_frame)
        discount_rate_entry.pack(fill='x', pady=5)
        discount_rate_entry.insert(0, "0.0")

        if connection_dis:
            try:
                cur = connection_dis.cursor()
                cur.execute("SELECT CASH_DISCOUNT_ID, CASH_DISCOUNT_NAME FROM CASH_DISCOUNTS")
                cash_discounts = [(row[0], row[1]) for row in cur.fetchall()]
                cash_discount_combo['values'] = [f"{id} - {name}" for id, name in cash_discounts]
                cur.close()
            except fdb.Error as e:
                logging.error(f"خطأ في جلب الخصومات النقدية: {str(e)}")

        def add_discount():
            cash_discount_selection = cash_discount_combo.get()
            if not cash_discount_selection:
                messagebox.showwarning("تحذير", "يرجى اختيار خصم نقدي!")
                return
            cash_discount_id = cash_discount_selection.split(" - ")[0]
            cash_discount_name = cash_discount_selection.split(" - ")[1]
            try:
                discount_rate = float(discount_rate_entry.get())
                discount_listbox.insert(tk.END, f"{cash_discount_id} - {cash_discount_name} | نسبة: {discount_rate}")
                existing_discounts.append((cash_discount_id, discount_rate, cash_discount_name))
                discount_rate_entry.delete(0, tk.END)
                discount_rate_entry.insert(0, "0.0")
            except ValueError:
                messagebox.showerror("خطأ", "يرجى إدخال نسبة خصم صحيحة!")

        def remove_discount():
            selected = discount_listbox.curselection()
            if not selected:
                messagebox.showwarning("تحذير", "يرجى اختيار خصم لحذفه!")
                return
            discount_listbox.delete(selected[0])
            existing_discounts.pop(selected[0])

        ttk.Button(discount_frame, text="إضافة خصم", command=add_discount, style="Add.TButton").pack(pady=5)
        ttk.Button(discount_frame, text="حذف خصم", command=remove_discount, style="Delete.TButton").pack(pady=5)

        def save_changes():
            try:
                new_name = entries["اسم المورد:"].get()
                new_cash_discount_id = entries["كود الخصم النقدي:"].get() or None
                new_agreement_discount_id = entries["كود خصم الاتفاق:"].get() or None
                _, _, connection_dis = self.get_connections()
                if not connection_dis:
                    messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
                    return

                cur = connection_dis.cursor()
                # تحديث بيانات المورد الأساسية
                cur.execute("UPDATE SUPPLIERS SET SUPPLIER_NAME = ?, CASH_DISCOUNT_ID = ?, AGREEMENT_DISCOUNT_ID = ? WHERE SUPPLIER_ID = ?",
                           (new_name, new_cash_discount_id, new_agreement_discount_id, supplier_id))

                # حذف الخصومات القديمة من SUPPLIER_CASH_DISCOUNTS
                cur.execute("DELETE FROM SUPPLIER_CASH_DISCOUNTS WHERE SUPPLIER_ID = ?", (supplier_id,))
                # إضافة الخصومات الجديدة
                for cash_discount_id, discount_rate, _ in existing_discounts:
                    cur.execute("INSERT INTO SUPPLIER_CASH_DISCOUNTS (SUPPLIER_ID, CASH_DISCOUNT_ID, DISCOUNT_RATE) VALUES (?, ?, ?)",
                               (supplier_id, cash_discount_id, discount_rate))

                connection_dis.commit()
                cur.close()
                self.load_suppliers_data()
                messagebox.showinfo("نجاح", "تم تعديل المورد بنجاح!")
                edit_window.destroy()
            except fdb.Error as e:
                error_msg = f"فشل تعديل المورد: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)

        ttk.Button(edit_window, text="حفظ", command=save_changes, style="Save.TButton").pack(pady=10)

    def delete_supplier(self):
        """حذف مورد محدد"""
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("تحذير", "يرجى اختيار مورد للحذف!")
            return

        item = self.tree.item(selected_item[0])
        supplier_id = item['values'][0]

        if messagebox.askyesno("تأكيد الحذف", f"هل أنت متأكد من حذف المورد {supplier_id}؟"):
            _, _, connection_dis = self.get_connections()
            if not connection_dis:
                messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
                return
            cur = None
            try:
                cur = connection_dis.cursor()
                cur.execute("DELETE FROM SUPPLIER_CASH_DISCOUNTS WHERE SUPPLIER_ID = ?", (supplier_id,))
                cur.execute("DELETE FROM SUPPLIERS WHERE SUPPLIER_ID = ?", (supplier_id,))
                connection_dis.commit()
                logging.info(f"تم حذف المورد بكود: {supplier_id}")
                self.load_suppliers_data()
                messagebox.showinfo("نجاح", "تم حذف المورد بنجاح!")
            except fdb.Error as e:
                error_msg = f"فشل حذف المورد: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)
            finally:
                if cur is not None:
                    cur.close()

    def insert_suppliers_data(self):
        """إدخال بيانات افتراضية للموردين إذا كانت الجداول فارغة"""
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            logging.error("No connection to dis_db for inserting suppliers data")
            return

        cur = None
        try:
            cur = connection_dis.cursor()

            # التحقق من وجود الخصومات النقدية وإضافتها إذا لم تكن موجودة
            cur.execute("SELECT COUNT(*) FROM CASH_DISCOUNTS WHERE CASH_DISCOUNT_ID = 'CASH001'")
            if cur.fetchone()[0] == 0:
                cur.execute("INSERT INTO CASH_DISCOUNTS (CASH_DISCOUNT_ID, CASH_DISCOUNT_NAME) VALUES ('CASH001', 'خصم نقدي 5%')")
            cur.execute("SELECT COUNT(*) FROM CASH_DISCOUNTS WHERE CASH_DISCOUNT_ID = 'CASH002'")
            if cur.fetchone()[0] == 0:
                cur.execute("INSERT INTO CASH_DISCOUNTS (CASH_DISCOUNT_ID, CASH_DISCOUNT_NAME) VALUES ('CASH002', 'خصم نقدي 10%')")

            # التحقق من وجود خصومات الاتفاق وإضافتها إذا لم تكن موجودة
            cur.execute("SELECT COUNT(*) FROM AGREEMENT_DISCOUNTS WHERE AGREEMENT_DISCOUNT_ID = 'AGREE001'")
            if cur.fetchone()[0] == 0:
                cur.execute("INSERT INTO AGREEMENT_DISCOUNTS (AGREEMENT_DISCOUNT_ID, DISCOUNT_RATE) VALUES ('AGREE001', 15.0)")
            cur.execute("SELECT COUNT(*) FROM AGREEMENT_DISCOUNTS WHERE AGREEMENT_DISCOUNT_ID = 'AGREE002'")
            if cur.fetchone()[0] == 0:
                cur.execute("INSERT INTO AGREEMENT_DISCOUNTS (AGREEMENT_DISCOUNT_ID, DISCOUNT_RATE) VALUES ('AGREE002', 20.0)")

            # التحقق من جدول الموردين وإدخال البيانات الافتراضية
            cur.execute("SELECT COUNT(*) FROM SUPPLIERS")
            count = cur.fetchone()[0]
            if count == 0:
                default_data = [
                    ("SUP001", "مورد 1", "CASH001", "AGREE001"),
                    ("SUP002", "مورد 2", "CASH002", "AGREE002")
                ]
                cur.executemany("INSERT INTO SUPPLIERS (SUPPLIER_ID, SUPPLIER_NAME, CASH_DISCOUNT_ID, AGREEMENT_DISCOUNT_ID) VALUES (?, ?, ?, ?)", default_data)
                logging.info(f"Inserted {len(default_data)} default suppliers")
            else:
                logging.info(f"Found {count} suppliers, skipping insertion")

            connection_dis.commit()
        except fdb.Error as e:
            connection_dis.rollback()
            error_msg = f"فشل إدخال بيانات الموردين: {str(e)}"
            logging.error(error_msg)
            self.frame.after(0, lambda msg=error_msg: messagebox.showerror("خطأ", msg))
        finally:
            if cur is not None:
                cur.close() 


class ImportDataTab:
    def __init__(self, notebook, get_connections, items_with_bonuses_tab):
        self.frame = ttk.Frame(notebook)
        notebook.add(self.frame, text="استيراد البيانات")
        self.get_connections = get_connections
        self.items_with_bonuses_tab = items_with_bonuses_tab  # لتحديث تبويب الأصناف مع البونصات

        # إعداد الواجهة
        main_frame = ttk.Frame(self.frame)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)

        title_label = ttk.Label(main_frame, text="استيراد بيانات من Excel", font=('Segoe UI', 13, 'bold'))
        title_label.pack(pady=(0, 10))

        # زر لاختيار ملف Excel
        ttk.Button(main_frame, text="اختيار ملف Excel", command=self.load_excel_file, style="Add.TButton").pack(pady=5)

        # تسمية لعرض مسار الملف المختار
        self.file_path_label = ttk.Label(main_frame, text="لم يتم اختيار ملف")
        self.file_path_label.pack(pady=5)

        # إطار لتعيين أعمدة Excel (يظهر بعد اختيار الملف)
        self.mapping_frame = ttk.LabelFrame(main_frame, text="ربط أعمدة Excel:")
        self.mapping_entries = {}
        
        # الأعمدة المتاحة للتحديث
        self.available_columns = [
            "PRICE_1", "SALE_DISCOUNT", "SALE_PRICE", "TAX_RATE", "SALE_MARGIN", "MAIN_MARGIN",
            "PROD_NAME", "COP_NAME", "TOTAL_QTY_ALL", "DISCOUNT_COST", "PHARMA_CODE"
        ]
        self.column_mappings = {}  # لتخزين تعيينات الأعمدة

        # خيار لتحديث البونصات
        self.update_bonuses_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(main_frame, text="تحديث البونصات المرتبطة بالأصناف", variable=self.update_bonuses_var).pack(pady=5)

        # زر لتأكيد الاستيراد
        ttk.Button(main_frame, text="استيراد وتحديث البيانات", command=self.import_and_update, style="Save.TButton").pack(pady=10)

        # زر لحذف كل البونصات المرتبطة
        ttk.Button(main_frame, text="حذف كل البونصات المرتبطة", command=self.delete_all_bonuses, style="Delete.TButton").pack(pady=5)

    def load_excel_file(self):
        file_path = filedialog.askopenfilename(
            title="اختر ملف Excel",
            filetypes=[("Excel files", "*.xlsx *.xls")]
        )
        if file_path:
            self.file_path_label.config(text=file_path)
            self.excel_file_path = file_path
            logging.info(f"تم اختيار ملف Excel: {file_path}")
            
            # قراءة أسماء الأعمدة من ملف Excel
            try:
                df = pd.read_excel(file_path, nrows=0)  # قراءة الرأس فقط
                self.excel_columns = list(df.columns)
                self.show_column_mapping()
            except Exception as e:
                messagebox.showerror("خطأ", f"فشل قراءة أعمدة ملف Excel: {str(e)}")
                logging.error(f"فشل قراءة أعمدة ملف Excel: {str(e)}")
                self.excel_columns = []
        else:
            self.file_path_label.config(text="لم يتم اختيار ملف")
            logging.info("لم يتم اختيار ملف Excel")
            self.mapping_frame.pack_forget()

    def show_column_mapping(self):
        """عرض واجهة لتعيين أعمدة Excel"""
        self.mapping_frame.pack_forget()
        self.mapping_frame = ttk.LabelFrame(self.frame, text="ربط أعمدة Excel:")
        self.mapping_frame.pack(fill='x', padx=10, pady=5, expand=False)
        self.mapping_entries.clear()

        # تعيين PROD_ID بشكل منفصل (إلزامي)
        prod_id_frame = ttk.Frame(self.mapping_frame)
        prod_id_frame.pack(fill='x', pady=2)
        ttk.Label(prod_id_frame, text="PROD_ID (كود الصنف):", width=20, anchor='e').pack(side='right', padx=5)
        prod_id_combo = ttk.Combobox(prod_id_frame, values=self.excel_columns, state="readonly")
        prod_id_combo.pack(side='left', fill='x', expand=True, padx=5)
        prod_id_combo.set(self.excel_columns[0] if self.excel_columns else "")
        self.mapping_entries["PROD_ID"] = prod_id_combo

        # تعيين الأعمدة الأخرى المتاحة
        for col in self.available_columns:
            frame = ttk.Frame(self.mapping_frame)
            frame.pack(fill='x', pady=2)
            ttk.Label(frame, text=f"{col}:", width=20, anchor='e').pack(side='right', padx=5)
            combo = ttk.Combobox(frame, values=["(لا تعيين)"] + self.excel_columns, state="readonly")
            combo.pack(side='left', fill='x', expand=True, padx=5)
            combo.set("(لا تعيين)")
            self.mapping_entries[col] = combo

        # تعيين أعمدة البونص (اختياري)
        bonus_frame = ttk.LabelFrame(self.mapping_frame, text="أعمدة البونص:")
        bonus_frame.pack(fill='x', pady=5)
        
        base_qty_frame = ttk.Frame(bonus_frame)
        base_qty_frame.pack(fill='x', pady=2)
        ttk.Label(base_qty_frame, text="BASE_QTY (الكمية):", width=20, anchor='e').pack(side='right', padx=5)
        base_qty_combo = ttk.Combobox(base_qty_frame, values=["(لا تعيين)"] + self.excel_columns, state="readonly")
        base_qty_combo.pack(side='left', fill='x', expand=True, padx=5)
        base_qty_combo.set("(لا تعيين)")
        self.mapping_entries["BASE_QTY"] = base_qty_combo

        bonus_qty_frame = ttk.Frame(bonus_frame)
        bonus_qty_frame.pack(fill='x', pady=2)
        ttk.Label(bonus_qty_frame, text="BONUS_QTY (البونص):", width=20, anchor='e').pack(side='right', padx=5)
        bonus_qty_combo = ttk.Combobox(bonus_qty_frame, values=["(لا تعيين)"] + self.excel_columns, state="readonly")
        bonus_qty_combo.pack(side='left', fill='x', expand=True, padx=5)
        bonus_qty_combo.set("(لا تعيين)")
        self.mapping_entries["BONUS_QTY"] = bonus_qty_combo

    def delete_all_bonuses(self):
        """حذف جميع البونصات المرتبطة بالأصناف"""
        # طلب تأكيد من المستخدم
        if not messagebox.askyesno("تأكيد الحذف", "هل أنت متأكد أنك تريد حذف كل البونصات المرتبطة بالأصناف؟\nهذا الإجراء لا يمكن التراجع عنه!"):
            return

        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
            return

        cur_dis = None
        try:
            cur_dis = connection_dis.cursor()

            # حذف جميع السجلات من جدول PRODUCT_BONUS_LINK
            cur_dis.execute("DELETE FROM PRODUCT_BONUS_LINK")
            connection_dis.commit()

            # تسجيل عدد السجلات المحذوفة
            deleted_count = cur_dis.rowcount
            logging.info(f"تم حذف {deleted_count} رابط بونص من جدول PRODUCT_BONUS_LINK")

            # تحديث تبويب الأصناف مع البونصات
            self.items_with_bonuses_tab.load_items_with_bonuses()

            messagebox.showinfo("نجاح", f"تم حذف {deleted_count} رابط بونص بنجاح!")

        except fdb.Error as e:
            connection_dis.rollback()
            error_msg = f"فشل حذف البونصات: {str(e)}"
            messagebox.showerror("خطأ", error_msg)
            logging.error(error_msg)

        except Exception as e:
            error_msg = f"خطأ غير متوقع: {str(e)}"
            messagebox.showerror("خطأ", error_msg)
            logging.error(error_msg)

        finally:
            if cur_dis is not None:
                cur_dis.close()

    def import_and_update(self):
        if not hasattr(self, 'excel_file_path'):
            messagebox.showerror("خطأ", "يرجى اختيار ملف Excel أولاً!")
            return

        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
            return

        # قراءة ملف Excel بالكامل
        try:
            df = pd.read_excel(self.excel_file_path)
            logging.info(f"تم تحميل ملف Excel مع {len(df)} صف")
        except Exception as e:
            messagebox.showerror("خطأ", f"فشل قراءة ملف Excel: {str(e)}")
            logging.error(f"فشل قراءة ملف Excel: {str(e)}")
            return

        # التحقق من تعيين PROD_ID
        prod_id_mapping = self.mapping_entries["PROD_ID"].get()
        if not prod_id_mapping or prod_id_mapping not in df.columns:
            messagebox.showerror("خطأ", "يرجى تعيين عمود 'PROD_ID' في ملف Excel!")
            return

        # إعداد تعيينات الأعمدة
        self.column_mappings = {"PROD_ID": prod_id_mapping}
        selected_columns = []
        for db_col in self.available_columns:
            excel_col = self.mapping_entries[db_col].get()
            if excel_col != "(لا تعيين)" and excel_col in df.columns:
                self.column_mappings[db_col] = excel_col
                if db_col != "TAX_RATE":
                    selected_columns.append(db_col)

        update_bonuses = self.update_bonuses_var.get()
        if update_bonuses:
            base_qty_mapping = self.mapping_entries["BASE_QTY"].get()
            bonus_qty_mapping = self.mapping_entries["BONUS_QTY"].get()
            if base_qty_mapping == "(لا تعيين)" or bonus_qty_mapping == "(لا تعيين)" or \
            base_qty_mapping not in df.columns or bonus_qty_mapping not in df.columns:
                messagebox.showerror("خطأ", "يرجى تعيين أعمدة 'BASE_QTY' و 'BONUS_QTY' في ملف Excel لتحديث البونصات!")
                return
            self.column_mappings["BASE_QTY"] = base_qty_mapping
            self.column_mappings["BONUS_QTY"] = bonus_qty_mapping

        if not selected_columns and not update_bonuses and "TAX_RATE" not in self.column_mappings:
            messagebox.showerror("خطأ", "يرجى تعيين عمود واحد على الأقل للتحديث أو تحديد تحديث البونصات!")
            return

        cur_dis = None
        try:
            cur_dis = connection_dis.cursor()

            # تعريف المتغيرات العددية في البداية
            updated_additions_count = 0
            updated_bonuses_count = 0
            skipped_bonuses_count = 0
            skipped_additions_count = 0
            updated_tax_count = 0

            # جلب قائمة البونصات إذا كان تحديث البونصات مطلوبًا
            bonus_mapping = {}
            valid_prod_ids = set()
            if update_bonuses:
                cur_dis.execute("SELECT BONUS_ID, BASE_QTY, BONUS_QTY FROM BONUSES")
                bonus_mapping = {(row[1], row[2]): row[0] for row in cur_dis.fetchall()}
                logging.info(f"تم جلب {len(bonus_mapping)} بونص من جدول BONUSES")

                cur_dis.execute("SELECT PROD_ID FROM ADDITIONS")
                valid_prod_ids = {str(row[0]) for row in cur_dis.fetchall()}

            # معالجة نسبة الضريبة (TAX_RATE)
            if "TAX_RATE" in self.column_mappings:
                cur_dis.execute("SELECT PROD_ID, TAX_ID FROM ITEM_TAXES")
                prod_tax_mapping = {str(row[0]): row[1] for row in cur_dis.fetchall()}
                logging.info(f"تم جلب {len(prod_tax_mapping)} رابط ضريبة من جدول ITEM_TAXES")

                tax_updates = {}
                for _, row in df.iterrows():
                    try:
                        prod_id = str(row[prod_id_mapping])
                        if pd.isna(prod_id) or prod_id.strip() == "":
                            logging.warning(f"تخطي صف بسبب قيمة PROD_ID فارغة")
                            skipped_additions_count += 1
                            continue

                        tax_rate_value = row[self.column_mappings["TAX_RATE"]]
                        if pd.isna(tax_rate_value):
                            continue

                        tax_rate = float(tax_rate_value)
                        if tax_rate < 0:
                            logging.warning(f"تخطي صف بسبب قيمة ضريبة سلبية ({tax_rate}) لـ PROD_ID {prod_id}")
                            skipped_additions_count += 1
                            continue

                        tax_id = prod_tax_mapping.get(prod_id)
                        if tax_id is None:
                            cur_dis.execute("SELECT TAX_ID FROM TAXES WHERE TAX_RATE = ?", (tax_rate,))
                            result = cur_dis.fetchone()
                            if result:
                                tax_id = result[0]
                            else:
                                cur_dis.execute("SELECT MAX(TAX_ID) FROM TAXES")
                                max_tax_id = cur_dis.fetchone()[0]
                                if max_tax_id is None:
                                    tax_id = "TAX001"
                                else:
                                    tax_id = f"TAX{int(max_tax_id[3:]) + 1:03d}"
                                cur_dis.execute("INSERT INTO TAXES (TAX_ID, TAX_RATE) VALUES (?, ?)", (tax_id, tax_rate))
                                updated_tax_count += 1
                                logging.info(f"تم إضافة ضريبة جديدة: TAX_ID={tax_id}, TAX_RATE={tax_rate}")
                            cur_dis.execute("INSERT INTO ITEM_TAXES (PROD_ID, TAX_ID) VALUES (?, ?)", (prod_id, tax_id))
                        else:
                            if tax_id not in tax_updates:
                                tax_updates[tax_id] = tax_rate
                    except (ValueError, TypeError) as e:
                        logging.warning(f"تخطي صف بسبب قيمة غير صالحة لنسبة الضريبة لـ PROD_ID {row.get(prod_id_mapping, 'غير محدد')}: {str(e)}")
                        skipped_additions_count += 1
                        continue

                for tax_id, tax_rate in tax_updates.items():
                    cur_dis.execute("UPDATE TAXES SET TAX_RATE = ? WHERE TAX_ID = ?", (tax_rate, tax_id))
                    updated_tax_count += 1
                    logging.info(f"تم تحديث الضريبة: TAX_ID={tax_id}, TAX_RATE={tax_rate}")

            # تحديث الأعمدة الأخرى في جدول ADDITIONS
            if selected_columns:
                set_clause = ", ".join([f"{col} = ?" for col in selected_columns])
                update_query = f"UPDATE ADDITIONS SET {set_clause} WHERE PROD_ID = ?"
                values = []
                for _, row in df.iterrows():
                    try:
                        prod_id = str(row[prod_id_mapping])
                        if pd.isna(prod_id) or prod_id.strip() == "":
                            logging.warning(f"تخطي صف بسبب قيمة PROD_ID فارغة")
                            skipped_additions_count += 1
                            continue
                        row_values = []
                        for col in selected_columns:
                            value = row[self.column_mappings[col]]
                            if pd.isna(value):
                                row_values.append(None)
                            else:
                                row_values.append(value)
                        values.append(tuple(row_values + [prod_id]))
                    except (ValueError, TypeError) as e:
                        logging.warning(f"تخطي صف بسبب بيانات غير صالحة لـ PROD_ID {row.get(prod_id_mapping, 'غير محدد')}: {str(e)}")
                        skipped_additions_count += 1
                        continue
                if values:
                    cur_dis.executemany(update_query, values)
                    updated_additions_count = len(values)
                    logging.info(f"تم تحديث {updated_additions_count} سجل في جدول ADDITIONS")
                else:
                    logging.info("لم يتم تحديث أي سجلات في جدول ADDITIONS بسبب بيانات غير صالحة")

            # تحديث البونصات
            if update_bonuses:
                prod_bonus_mapping = {}
                for index, row in df.iterrows():
                    try:
                        prod_id = str(row[prod_id_mapping])
                        if pd.isna(prod_id) or prod_id.strip() == "":
                            logging.warning(f"تخطي صف {index} بسبب قيمة PROD_ID فارغة")
                            skipped_bonuses_count += 1
                            continue

                        base_qty_value = row[self.column_mappings["BASE_QTY"]]
                        bonus_qty_value = row[self.column_mappings["BONUS_QTY"]]
                        
                        if pd.isna(base_qty_value) or pd.isna(bonus_qty_value):
                            logging.warning(f"تخطي صف {index} بسبب قيم فارغة لـ PROD_ID {prod_id} في BASE_QTY أو BONUS_QTY")
                            skipped_bonuses_count += 1
                            continue

                        try:
                            base_qty = int(float(base_qty_value))
                            bonus_qty = int(float(bonus_qty_value))
                        except (ValueError, TypeError) as e:
                            logging.warning(f"تخطي صف {index} بسبب قيم غير صالحة لـ PROD_ID {prod_id} في BASE_QTY أو BONUS_QTY: {str(e)}")
                            skipped_bonuses_count += 1
                            continue

                        if prod_id not in valid_prod_ids:
                            logging.warning(f"PROD_ID {prod_id} غير موجود في جدول ADDITIONS، سيتم تخطيه (صف {index})")
                            skipped_bonuses_count += 1
                            continue

                        bonus_key = (base_qty, bonus_qty)
                        bonus_id = bonus_mapping.get(bonus_key)
                        if bonus_id is None:
                            # إنشاء BONUS_ID جديد
                            cur_dis.execute("SELECT MAX(BONUS_ID) FROM BONUSES")
                            max_bonus_id = cur_dis.fetchone()[0]
                            if max_bonus_id is None:
                                bonus_id = "BONUS001"
                            else:
                                num = int(max_bonus_id.replace("BONUS", "")) + 1
                                bonus_id = f"BONUS{num:03d}"
                            cur_dis.execute("INSERT INTO BONUSES (BONUS_ID, BASE_QTY, BONUS_QTY) VALUES (?, ?, ?)",
                                            (bonus_id, base_qty, bonus_qty))
                            bonus_mapping[bonus_key] = bonus_id
                            logging.info(f"تم إنشاء بونص جديد: {bonus_id} بـ BASE_QTY={base_qty}, BONUS_QTY={bonus_qty}")

                        if prod_id not in prod_bonus_mapping:
                            prod_bonus_mapping[prod_id] = set()  # استخدام set لتجنب التكرار
                        prod_bonus_mapping[prod_id].add(bonus_id)
                    except Exception as e:
                        logging.warning(f"تخطي صف {index} بسبب خطأ غير متوقع لـ PROD_ID {row.get(prod_id_mapping, 'غير محدد')}: {str(e)}")
                        skipped_bonuses_count += 1
                        continue

                for prod_id, bonus_ids in prod_bonus_mapping.items():
                    cur_dis.execute("DELETE FROM PRODUCT_BONUS_LINK WHERE PROD_ID = ?", (prod_id,))
                    for bonus_id in bonus_ids:
                        cur_dis.execute("INSERT INTO PRODUCT_BONUS_LINK (PROD_ID, BONUS_ID) VALUES (?, ?)", (prod_id, bonus_id))
                        updated_bonuses_count += 1
                logging.info(f"تم تحديث {updated_bonuses_count} رابط بونص في جدول PRODUCT_BONUS_LINK")

            connection_dis.commit()
            success_msg = "تم التحديث بنجاح!\n"
            if updated_tax_count > 0:
                success_msg += f"تم تحديث أو إضافة {updated_tax_count} ضريبة في جدول TAXES\n"
            if selected_columns:
                success_msg += f"تم تحديث {updated_additions_count} سجل في جدول ADDITIONS\n"
                if skipped_additions_count > 0:
                    success_msg += f"تم تخطي {skipped_additions_count} سجل بسبب بيانات غير صالحة\n"
            if update_bonuses:
                success_msg += f"تم تحديث {updated_bonuses_count} رابط بونص\n"
                if skipped_bonuses_count > 0:
                    success_msg += f"تم تخطي {skipped_bonuses_count} رابط بسبب بيانات غير صالحة"
            messagebox.showinfo("نجاح", success_msg)
            logging.info(success_msg)

            # تحديث تبويب الأصناف مع البونصات
            self.items_with_bonuses_tab.load_items_with_bonuses()

        except fdb.Error as e:
            connection_dis.rollback()
            error_msg = f"فشل تحديث البيانات: {str(e)}"
            messagebox.showerror("خطأ", error_msg)
            logging.error(error_msg)

        except Exception as e:
            connection_dis.rollback()
            error_msg = f"خطأ غير متوقع: {str(e)}"
            messagebox.showerror("خطأ", error_msg)
            logging.error(error_msg)

        finally:
            if cur_dis is not None:
                cur_dis.close()

class AgreementDiscountTab:
    def __init__(self, notebook, get_connections):
        self.frame = ttk.Frame(notebook)
        notebook.add(self.frame, text="خصم اتفاق")
        self.get_connections = get_connections
        self.current_data = []
        self.sort_column = None
        self.sort_reverse = False

        main_frame = ttk.Frame(self.frame)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)

        title_label = ttk.Label(main_frame, text="إدارة خصومات الاتفاق", font=('Segoe UI', 13, 'bold'))
        title_label.pack(pady=(0, 10))

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill='x', pady=5)

        add_button = ttk.Button(button_frame, text="إضافة خصم اتفاق", command=self.open_add_agreement_discount_window, style="Add.TButton")
        add_button.pack(side='left', padx=5)

        edit_button = ttk.Button(button_frame, text="تعديل", command=self.open_edit_agreement_discount_window, style="Edit.TButton")
        edit_button.pack(side='left', padx=5)

        delete_button = ttk.Button(button_frame, text="حذف", command=self.delete_agreement_discount, style="Delete.TButton")
        delete_button.pack(side='left', padx=5)

        reload_button = ttk.Button(button_frame, text="إعادة تحميل", command=self.load_agreement_discount_data, style="Reload.TButton")
        reload_button.pack(side='left', padx=5)

        export_button = ttk.Button(button_frame, text="حفظ كـ Excel", command=self.export_to_excel, style="Export.TButton")
        export_button.pack(side='left', padx=5)

        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill='both', expand=True)

        columns = ("agreement_discount_id", "discount_rate")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings')
        self.tree.heading("agreement_discount_id", text="كود خصم الاتفاق", anchor='center', command=lambda: self.sort_by_column("agreement_discount_id"))
        self.tree.heading("discount_rate", text="نسبة الخصم", anchor='center', command=lambda: self.sort_by_column("discount_rate"))

        self.tree.column("agreement_discount_id", width=150, anchor='center')
        self.tree.column("discount_rate", width=100, anchor='center')

        scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side='right', fill='y')
        self.tree.pack(fill='both', expand=True)

        self.tree.bind('<Double-1>', self.open_edit_agreement_discount_window)

    def sort_by_column(self, col):
        self.sort_reverse = not self.sort_reverse if self.sort_column == col else False
        self.sort_column = col
        col_index_map = {"agreement_discount_id": 0, "discount_rate": 1}
        col_index = col_index_map[col]

        def sort_key(row):
            value = row[col_index]
            if value is None:
                return (0, 0)
            try:
                numeric_value = float(str(value).replace(',', ''))
                return (1, numeric_value)
            except (ValueError, AttributeError):
                return (2, str(value).lower())

        self.current_data.sort(key=sort_key, reverse=self.sort_reverse)
        self.update_treeview()
        logging.info(f"Sorted by column {col}, reverse={self.sort_reverse}")

    def update_treeview(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.current_data:
            self.tree.insert("", "end", values=row)

    def load_agreement_discount_data(self):
        self.current_data = []
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            self.frame.after(0, lambda: messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!"))
            return
        cur = None
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT AGREEMENT_DISCOUNT_ID, DISCOUNT_RATE FROM AGREEMENT_DISCOUNTS")
            rows = cur.fetchall()
            self.current_data = list(rows)
            logging.info(f"Loaded {len(self.current_data)} agreement discount records")
            self.update_treeview()
        except fdb.Error as e:
            error_msg = f"فشل تحميل بيانات خصومات الاتفاق: {str(e)}"
            logging.error(error_msg)
            self.frame.after(0, lambda msg=error_msg: messagebox.showerror("خطأ", msg))
        finally:
            if cur is not None:
                cur.close()

    def export_to_excel(self):
        if not self.current_data:
            messagebox.showwarning("تحذير", "لا توجد بيانات لحفظها!")
            return

        df = pd.DataFrame(self.current_data, columns=["كود خصم الاتفاق", "نسبة الخصم"])
        filename = f"AgreementDiscounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = os.path.join(os.getcwd(), filename)
        df.to_excel(filepath, index=False, engine='openpyxl')
        logging.info(f"Saved Agreement Discounts to {filepath}")
        messagebox.showinfo("نجاح", f"تم حفظ الملف باسم {filename}")
        os.startfile(os.path.dirname(filepath))

    def open_add_agreement_discount_window(self):
        add_window = tk.Toplevel(self.frame)
        add_window.title("إضافة خصم اتفاق جديد")
        add_window.geometry("400x200")
        add_window.configure(bg="#f3f3f3")
        ttk.Label(add_window, text="إضافة خصم اتفاق جديد", font=('Segoe UI', 13, 'bold')).pack(pady=10)

        fields = [
            ("كود خصم الاتفاق:", ""),
            ("نسبة الخصم:", "")
        ]
        entries = {}
        for label_text, value in fields:
            frame = ttk.Frame(add_window)
            frame.pack(fill='x', padx=10, pady=5)
            ttk.Label(frame, text=label_text, width=15, anchor='e').pack(side='right', padx=5)
            entry = ttk.Entry(frame)
            entry.pack(side='left', fill='x', expand=True, padx=5)
            entry.insert(0, value)
            entries[label_text] = entry

        def add_agreement_discount():
            try:
                agreement_discount_id = entries["كود خصم الاتفاق:"].get()
                discount_rate = float(entries["نسبة الخصم:"].get())
                if not agreement_discount_id:
                    messagebox.showerror("خطأ", "يرجى ملء كود خصم الاتفاق!")
                    return

                _, _, connection_dis = self.get_connections()
                if connection_dis:
                    cur = connection_dis.cursor()
                    cur.execute("INSERT INTO AGREEMENT_DISCOUNTS (AGREEMENT_DISCOUNT_ID, DISCOUNT_RATE) VALUES (?, ?)",
                               (agreement_discount_id, discount_rate))
                    connection_dis.commit()
                    cur.close()
                    self.load_agreement_discount_data()
                    messagebox.showinfo("نجاح", "تم إضافة خصم الاتفاق بنجاح!")
                    add_window.destroy()
            except (fdb.Error, ValueError) as e:
                error_msg = f"فشل إضافة خصم الاتفاق: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)

        ttk.Button(add_window, text="إضافة", command=add_agreement_discount, style="Add.TButton").pack(pady=10)

    def open_edit_agreement_discount_window(self, event=None):
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("تحذير", "يرجى اختيار خصم اتفاق لتعديله!")
            return

        item = self.tree.item(selected_item[0])
        agreement_discount_id, discount_rate = item['values']

        edit_window = tk.Toplevel(self.frame)
        edit_window.title("تعديل خصم الاتفاق")
        edit_window.geometry("400x200")
        edit_window.configure(bg="#f3f3f3")

        ttk.Label(edit_window, text="تعديل خصم اتفاق", font=('Segoe UI', 13, 'bold')).pack(pady=10)

        fields = [
            ("كود خصم الاتفاق:", agreement_discount_id, False),
            ("نسبة الخصم:", discount_rate, True)
        ]
        entries = {}
        for label_text, value, editable in fields:
            frame = ttk.Frame(edit_window)
            frame.pack(fill='x', padx=10, pady=5)
            ttk.Label(frame, text=label_text, width=15, anchor='e').pack(side='right', padx=5)
            entry = ttk.Entry(frame)
            entry.pack(side='left', fill='x', expand=True, padx=5)
            entry.insert(0, str(value))
            if not editable:
                entry.config(state='disabled')
            entries[label_text] = entry

        def save_changes():
            try:
                new_rate = float(entries["نسبة الخصم:"].get())
                _, _, connection_dis = self.get_connections()
                if not connection_dis:
                    messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
                    return
                cur = connection_dis.cursor()
                cur.execute("UPDATE AGREEMENT_DISCOUNTS SET DISCOUNT_RATE = ? WHERE AGREEMENT_DISCOUNT_ID = ?",
                           (new_rate, agreement_discount_id))
                connection_dis.commit()
                cur.close()
                self.load_agreement_discount_data()
                messagebox.showinfo("نجاح", "تم تعديل خصم الاتفاق بنجاح!")
                edit_window.destroy()
            except (fdb.Error, ValueError) as e:
                error_msg = f"فشل تعديل خصم الاتفاق: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)

        ttk.Button(edit_window, text="حفظ", command=save_changes, style="Save.TButton").pack(pady=10)

    def delete_agreement_discount(self):
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("تحذير", "يرجى اختيار خصم اتفاق للحذف!")
            return

        item = self.tree.item(selected_item[0])
        agreement_discount_id = item['values'][0]

        if messagebox.askyesno("تأكيد الحذف", f"هل أنت متأكد من حذف خصم الاتفاق {agreement_discount_id}؟"):
            _, _, connection_dis = self.get_connections()
            if not connection_dis:
                messagebox.showerror("خطأ", "لا يوجد اتصال بقاعدة البيانات!")
                return
            cur = None
            try:
                cur = connection_dis.cursor()
                cur.execute("DELETE FROM AGREEMENT_DISCOUNTS WHERE AGREEMENT_DISCOUNT_ID = ?", (agreement_discount_id,))
                connection_dis.commit()
                logging.info(f"Deleted agreement discount with ID: {agreement_discount_id}")
                self.load_agreement_discount_data()
                messagebox.showinfo("نجاح", "تم حذف خصم الاتفاق بنجاح!")
            except fdb.Error as e:
                error_msg = f"فشل حذف خصم الاتفاق: {str(e)}"
                logging.error(error_msg)
                messagebox.showerror("خطأ", error_msg)
            finally:
                if cur is not None:
                    cur.close()

    def insert_agreement_discount_data(self):
        _, _, connection_dis = self.get_connections()
        if not connection_dis:
            logging.error("No connection to dis_db for inserting agreement discount data")
            return

        cur = None
        try:
            cur = connection_dis.cursor()
            cur.execute("SELECT COUNT(*) FROM AGREEMENT_DISCOUNTS")
            count = cur.fetchone()[0]
            if count == 0:
                connection_dis.begin()  # بدء المعاملة صراحةً
                default_data = [
                    ("AGREE001", 5.0),
                    ("AGREE002", 7.5)
                ]
                cur.executemany("INSERT INTO AGREEMENT_DISCOUNTS (AGREEMENT_DISCOUNT_ID, DISCOUNT_RATE) VALUES (?, ?)", default_data)
                connection_dis.commit()
                logging.info(f"Inserted {len(default_data)} default agreement discounts")
            else:
                logging.info(f"Found {count} agreement discounts, skipping insertion")
        except fdb.Error as e:
            connection_dis.rollback()  # التراجع في حالة الخطأ
            error_msg = f"فشل إدخال بيانات خصومات الاتفاق: {str(e)}"
            logging.error(error_msg)
            self.frame.after(0, lambda msg=error_msg: messagebox.showerror("خطأ", msg))
        finally:
            if cur is not None:
                cur.close()




class ComparisonTab:
    def __init__(self, notebook, get_connections):
        self.tab = ttk.Frame(notebook)
        notebook.add(self.tab, text="مقارنة الأصناف")
        self.get_connections = get_connections
        self.current_data = []
        self.sort_column = None
        self.sort_reverse = False

        # إعداد الواجهة الرسومية
        main_frame = ttk.Frame(self.tab)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)

        title_label = ttk.Label(main_frame, text="مقارنة الأصناف", font=('Segoe UI', 13, 'bold'))
        title_label.pack(pady=(0, 10))

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill='x', pady=5)

        reload_button = ttk.Button(button_frame, text="إعادة تحميل", command=self.load_comparison_data, style="Reload.TButton")
        reload_button.pack(side='left', padx=5)

        export_button = ttk.Button(button_frame, text="حفظ كـ Excel", command=self.export_to_excel, style="Export.TButton")
        export_button.pack(side='left', padx=5)

        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill='both', expand=True)

        # إضافة عمود "خصم الموزع" إلى الأعمدة
        columns = (
            "prod_id", "prod_name", "price", "tax_qty", "sale_plus_main_margin", "sale_discount",
            "cash_discount", "agreement_discount", "discount_added", "main_margin", "base_qty",
            "bonus_qty", "discount_rate", "weighted", "max_store_discount", "comparison",
            "distributor_discount"  # العمود الجديد
        )
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings')

        # تعيين عناوين الأعمدة مع دعم الفرز
        self.tree.heading("prod_id", text="كود الصنف", command=lambda: self.sort_by_column("prod_id"))
        self.tree.heading("prod_name", text="اسم الصنف", command=lambda: self.sort_by_column("prod_name"))
        self.tree.heading("price", text="السعر", command=lambda: self.sort_by_column("price"))
        self.tree.heading("tax_qty", text="قيمة مضافة", command=lambda: self.sort_by_column("tax_qty"))
        self.tree.heading("sale_plus_main_margin", text="هامش ربح ص+م", command=lambda: self.sort_by_column("sale_plus_main_margin"))
        self.tree.heading("sale_discount", text="خصم صيدلي", command=lambda: self.sort_by_column("sale_discount"))
        self.tree.heading("cash_discount", text="خصم نقدي", command=lambda: self.sort_by_column("cash_discount"))
        self.tree.heading("agreement_discount", text="خصم اتفاق", command=lambda: self.sort_by_column("agreement_discount"))
        self.tree.heading("discount_added", text="خصم إضافي", command=lambda: self.sort_by_column("discount_added"))
        self.tree.heading("main_margin", text="هامش ربح م", command=lambda: self.sort_by_column("main_margin"))
        self.tree.heading("base_qty", text="كمية", command=lambda: self.sort_by_column("base_qty"))
        self.tree.heading("bonus_qty", text="بونص", command=lambda: self.sort_by_column("bonus_qty"))
        self.tree.heading("discount_rate", text="نسبة الخصم", command=lambda: self.sort_by_column("discount_rate"))
        self.tree.heading("weighted", text="مرجح", command=lambda: self.sort_by_column("weighted"))
        self.tree.heading("max_store_discount", text="أعلى خ مخزن", command=lambda: self.sort_by_column("max_store_discount"))
        self.tree.heading("comparison", text="مقارنة مخزن/موزع", command=lambda: self.sort_by_column("comparison"))
        self.tree.heading("distributor_discount", text="خصم الموزع", command=lambda: self.sort_by_column("distributor_discount"))

        # تعيين عرض الأعمدة
        for col in columns:
            self.tree.column(col, width=100, anchor='center')

        scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side='right', fill='y')
        self.tree.pack(fill='both', expand=True)

        self.load_comparison_data()

    def sort_by_column(self, col):
        self.sort_reverse = not self.sort_reverse if self.sort_column == col else False
        self.sort_column = col

        col_index_map = {
            "prod_id": 0, "prod_name": 1, "price": 2, "tax_qty": 3, "sale_plus_main_margin": 4,
            "sale_discount": 5, "cash_discount": 6, "agreement_discount": 7, "discount_added": 8,
            "main_margin": 9, "base_qty": 10, "bonus_qty": 11, "discount_rate": 12, "weighted": 13,
            "max_store_discount": 14, "comparison": 15, "distributor_discount": 16  # تحديث الفهرس
        }
        col_index = col_index_map[col]

        def sort_key(row):
            value = row[col_index]
            if value is None:
                return (0, 0)
            try:
                numeric_value = float(str(value).replace(',', ''))
                return (1, numeric_value)
            except (ValueError, AttributeError):
                return (2, str(value).lower())

        self.current_data.sort(key=sort_key, reverse=self.sort_reverse)
        self.update_treeview()
        logging.info(f"تم الفرز حسب العمود {col}, عكسي={self.sort_reverse}")

    def update_treeview(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.current_data:
            self.tree.insert("", "end", values=row)

    def load_comparison_data(self):
        logging.info("Entering load_comparison_data")
        self.current_data = []
        connection_stores, connection_main, connection_dis = self.get_connections()
        if not all([connection_stores, connection_main, connection_dis]):
            self.tab.after(0, lambda: messagebox.showerror("خطأ", "لا يوجد اتصال بجميع قواعد البيانات!"))
            return

        cur_dis = None
        cur_stores = None
        try:
            cur_dis = connection_dis.cursor()
            cur_stores = connection_stores.cursor()

            query_dis = """
                SELECT a.PROD_ID, a.PROD_NAME, a.PRICE_1, a.TAX_QTY, a.SALE_MARGIN, a.MAIN_MARGIN, 
                    a.SALE_DISCOUNT, a.DISCOUNT_COST, b.BASE_QTY, b.BONUS_QTY, b.DISCOUNT_RATE,
                    s.CASH_DISCOUNT_ID, ad.DISCOUNT_RATE AS AGREEMENT_DISCOUNT
                FROM ADDITIONS a
                LEFT JOIN PRODUCT_BONUS_LINK pbl ON a.PROD_ID = pbl.PROD_ID
                LEFT JOIN BONUSES b ON pbl.BONUS_ID = b.BONUS_ID
                LEFT JOIN SUPPLIERS s ON a.COP_NAME = s.SUPPLIER_NAME
                LEFT JOIN AGREEMENT_DISCOUNTS ad ON s.AGREEMENT_DISCOUNT_ID = ad.AGREEMENT_DISCOUNT_ID
            """
            cur_dis.execute(query_dis)
            dis_data = cur_dis.fetchall()

            store_query = "SELECT PROD_ID, MAX(DISCOUNT_IN) FROM PRODUCTS GROUP BY PROD_ID"
            store_discounts = {}
            cur_stores.execute(store_query)
            for row in cur_stores.fetchall():
                store_discounts[str(row[0])] = float(row[1] or 0.0)

            for row in dis_data:
                prod_id, prod_name, price, tax_qty, sale_margin, main_margin, sale_discount, discount_cost, base_qty, bonus_qty, bonus_discount_rate, cash_discount_id, agreement_discount = row
                price = float(price or 0.0)
                tax_qty = float(tax_qty or 0.0)
                sale_margin = float(sale_margin or 0.0)
                main_margin = float(main_margin or 0.0)
                sale_discount = float(sale_discount or 0.0) / 100  # تحويل إلى نسبة
                discount_cost = float(discount_cost or 0.0)
                base_qty = int(base_qty or 0)
                bonus_qty = int(bonus_qty or 0)
                bonus_discount_rate = float(bonus_discount_rate or 0.0)
                cash_discount_id = cash_discount_id or "غير محدد"
                agreement_discount = float(agreement_discount or 0.0) / 100  # تحويل إلى نسبة
                discount_added = 0.0  # افتراضيًا، يمكن تعديله إذا كان مخزنًا في قاعدة البيانات
                max_store_discount = store_discounts.get(prod_id, 0.0)

                sale_plus_main_margin = sale_margin + main_margin

                # حساب نسبة الخصم الحالية (discount_rate)
                if base_qty + bonus_qty != 0 and price != 0:
                    discount_rate = ((((((((price - sale_discount) * (1 - agreement_discount) - tax_qty) * 
                                        (1 - discount_added) * (1 - bonus_discount_rate)) + tax_qty) * 
                                        base_qty + (bonus_qty * tax_qty) + (discount_cost * base_qty)) / 
                                        (base_qty + bonus_qty)) / price) - 1) * (-100)
                else:
                    discount_rate = 0.0

                # حساب خصم الموزع بناءً على المعادلة المطلوبة
                if base_qty + bonus_qty != 0 and price != 0:
                    cash_discount = 0.0 if cash_discount_id == "غير محدد" else float(bonus_discount_rate)  # افتراض مؤقت
                    distributor_discount = (((((((((price - sale_plus_main_margin) - (sale_discount)) - tax_qty) - 
                                             ((cash_discount + agreement_discount)) - (discount_added)) + tax_qty) * 
                                             base_qty) + (bonus_qty * tax_qty) + (main_margin * base_qty)) / 
                                             (base_qty + bonus_qty)) / price) * 100
                else:
                    distributor_discount = 0.0

                comparison = discount_rate - max_store_discount if max_store_discount else "غير متوفر"
                self.current_data.append((
                    prod_id, prod_name, round(price, 2), round(tax_qty, 2), round(sale_plus_main_margin, 2),
                    round(sale_discount * 100, 2), cash_discount_id, round(agreement_discount * 100, 2),
                    round(discount_added, 2), round(main_margin, 2), base_qty, bonus_qty,
                    round(discount_rate, 2), round(discount_cost, 2), round(max_store_discount, 2),
                    round(comparison, 2) if isinstance(comparison, (int, float)) else comparison,
                    round(distributor_discount, 2)  # إضافة خصم الموزع
                ))

            logging.info(f"تم تحميل {len(self.current_data)} سجل مقارنة")
            self.update_treeview()

        except Exception as e:
            error_msg = f"خطأ غير متوقع: {str(e)}"
            logging.error(error_msg)
            self.tab.after(0, lambda msg=error_msg: messagebox.showerror("خطأ", msg))
        finally:
            if cur_dis:
                cur_dis.close()
            if cur_stores:
                cur_stores.close()

    def export_to_excel(self):
        if not self.current_data:
            messagebox.showwarning("تحذير", "لا توجد بيانات لحفظها!")
            return

        df = pd.DataFrame(self.current_data, columns=[
            "كود الصنف", "اسم الصنف", "السعر", "قيمة مضافة", "هامش ربح ص+م", "خصم صيدلي",
            "خصم نقدي", "خصم اتفاق", "خصم إضافي", "هامش ربح م", "كمية", "بونص",
            "نسبة الخصم", "مرجح", "أعلى خ مخزن", "مقارنة مخزن/موزع", "خصم الموزع"  # إضافة العمود
        ])
        filename = f"Comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = os.path.join(os.getcwd(), filename)
        df.to_excel(filepath, index=False, engine='openpyxl')
        logging.info(f"تم حفظ المقارنة في {filepath}")
        messagebox.showinfo("نجاح", f"تم حفظ الملف باسم {filename}")
        os.startfile(os.path.dirname(filepath))

def main():
    start_time = time.time()

    print("بدء تشغيل البرنامج...")
    root = tk.Tk()
    print(f"تم إنشاء نافذة Tkinter في {time.time() - start_time:.2f} ثانية")
    root.title("نظام إدارة المخازن والتوزيع")
    root.geometry("1200x800")

    notebook = ttk.Notebook(root)
    notebook.pack(fill='both', expand=True)
    print(f"تم إنشاء التبويبات الأساسية في {time.time() - start_time:.2f} ثانية")

    # إنشاء التبويبات الأساسية
    items_list_tab = ItemsListTab(notebook, None)
    add_edit_tab = AddEditTab(notebook, None)

    # إنشاء DatabaseTab مع تمرير التبويبات المطلوبة
    db_tab = DatabaseTab(
        notebook,
        items_list_tab,
        add_edit_tab.bonus_tab,
        
        add_edit_tab.cash_discount_tab,
        add_edit_tab.product_type_tab,
        add_edit_tab.tax_tab,
        add_edit_tab.suppliers_tab,
        add_edit_tab.agreement_discount_tab
    )

    # تعريف دالة get_connections بعد إنشاء db_tab
    def get_connections():
        return db_tab.connection_stores, db_tab.connection_main, db_tab.connection_dis

    # إنشاء تبويبات إضافية مع تمرير get_connections
    import_data_tab = ImportDataTab(notebook, get_connections, items_list_tab)  # تم استبدال items_with_bonuses_tab بـ items_list_tab
    comparison_tab = ComparisonTab(notebook, get_connections)


    # تعيين get_connections لجميع التبويبات
    items_list_tab.get_connections = get_connections
    import_data_tab.get_connections = get_connections
    add_edit_tab.get_connections = get_connections
    add_edit_tab.bonus_tab.get_connections = get_connections
    add_edit_tab.cash_discount_tab.get_connections = get_connections
    add_edit_tab.product_type_tab.get_connections = get_connections
    add_edit_tab.tax_tab.get_connections = get_connections
    add_edit_tab.suppliers_tab.get_connections = get_connections
    add_edit_tab.agreement_discount_tab.get_connections = get_connections
    comparison_tab.get_connections = get_connections  # هذا اختياري لأنه تم تمريره بالفعل

    print("جاهز لبدء الحلقة الرئيسية.")
    root.mainloop()

if __name__ == "__main__":
    main()