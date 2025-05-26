import psycopg2

file_path_customer = 'Customer.csv'

with psycopg2.connect(database="BIGdata", user="postgres", password="130006") as conn:
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE customer, staff, menuitem;
        
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
            next(f)
            cur.copy_from(f, 'customer', sep=',', columns=('customerid', 'name', 'phone', 'email'))

        conn.commit()



        cur.execute("""
            SELECT * FROM customer;
        """)
        print(cur.fetchall())


        cur.execute("""
            CREATE TABLE IF NOT EXISTS staff (
                staffid INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                role VARCHAR(100) NOT NULL,
                phone VARCHAR(100) NOT NULL
            );
        """)

        conn.commit()

        with open('Staff.csv', 'r', encoding='utf-8') as f:
            next(f)
            cur.copy_from(f, 'staff', sep=',', columns=('staffid', 'name', 'role', 'phone'))

        conn.commit()



        cur.execute("""
            SELECT * FROM staff;
        """)

        print(cur.fetchall())


        cur.execute("""
            CREATE TABLE IF NOT EXISTS menuitem (
                menuitemid INT PRIMARY KEY,
                itemname VARCHAR(100) NOT NULL,
                price NUMERIC(10, 2) NOT NULL,
                category VARCHAR(100) NOT NULL
            );
        """)

        conn.commit()

        with open ('MenuItem.csv', 'r', encoding='utf-8') as f:
            next(f)
            cur.copy_from(f, 'menuitem', sep=',', columns=('menuitemid', 'itemname', 'price', 'category'))


        conn.commit()

        cur.execute("""
            SELECT * FROM menuitem;
        """)

        print(cur.fetchall())

