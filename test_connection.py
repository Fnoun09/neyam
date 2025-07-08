
import os
import psycopg2
import pandas as pd
from urllib.parse import urlparse

# بيانات وهمية
data = {
    'product_id': [1, 2, 3],
    'product_name': ['وسادة نيام', 'مفرش نيام', 'معطر نيام'],
    'category': ['وسائد', 'مفرش', 'معطر'],
    'stock_quantity': [150, 80, 200],
    'sales_count': [1200, 600, 850],
    'avg_customer_rating': [4.5, 4.2, 4.8]
}

df = pd.DataFrame(data)

# قراءة متغير الاتصال
url = os.getenv('DATABASE_URL') or os.getenv('Postgres.DATABASE_URL')

if not url:
    raise Exception("DATABASE_URL not found.")

result = urlparse(url)

try:
    conn = psycopg2.connect(
        dbname=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port
    )

    cur = conn.cursor()

    create_table_query = '''
    CREATE TABLE IF NOT EXISTS products_niyam (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT NOT NULL,
        category TEXT NOT NULL,
        stock_quantity INTEGER,
        sales_count INTEGER,
        avg_customer_rating REAL
    );
    '''
    cur.execute(create_table_query)
    conn.commit()

    for index, row in df.iterrows():
        insert_query = '''
        INSERT INTO products_niyam (product_id, product_name, category, stock_quantity, sales_count, avg_customer_rating)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            stock_quantity = EXCLUDED.stock_quantity,
            sales_count = EXCLUDED.sales_count,
            avg_customer_rating = EXCLUDED.avg_customer_rating;
        '''
        cur.execute(insert_query, (
            row['product_id'],
            row['product_name'],
            row['category'],
            row['stock_quantity'],
            row['sales_count'],
            row['avg_customer_rating']
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ تم رفع البيانات بنجاح إلى قاعدة PostgreSQL!")

except Exception as e:
    print("❌ فشل الاتصال أو رفع البيانات:")
    print(e)
