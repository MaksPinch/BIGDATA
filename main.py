import psycopg2

file_path_customer = 'Customer.csv'

with psycopg2.connect(database="BIGdata", user="postgres", password="130006") as conn:
    with conn.cursor() as cur:
        # удаление таблиц
        cur.execute("""
        DROP TABLE customer;
        
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS customer (
                customerid INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                phone VARCHAR(20),
                email VARCHAR(100)
            );
        """)
        conn.commit()

        with open(file_path_customer, 'r', encoding='utf-8') as f:
            next(f)  # пропускаем заголовок CSV
            cur.copy_from(f, 'customer', sep=',', columns=('customerid', 'name', 'phone', 'email'))

        conn.commit()



        cur.execute("""
            SELECT * FROM customer;
        """)
        print(cur.fetchall())

