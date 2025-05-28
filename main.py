import psycopg2

file_path_customer = 'Updated_Customer_csv.csv'
file_path_staff = 'Updated_Staff_csv.csv'
file_path_menuitem = 'Updated_MenuItem_csv.csv'
file_path_orders = 'Updated_Order_csv.csv'
file_path_orderitme = 'Updated_OrderItem_csv.csv'
file_path_payment = 'Updated_Payment_csv.csv'
file_path_reservation = 'Updated_Reservation_csv.csv'


with psycopg2.connect(database="BIGdata", user="postgres", password='130006') as conn:
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE customer, staff, menu_item, orders, payment, order_item;

        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS customer (
                customer_id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                phone VARCHAR(20),
                email VARCHAR(100)
            );
        """)
        conn.commit()

        with open(file_path_customer, 'r', encoding='utf-8') as f:
            next(f)
            cur.copy_from(f, 'customer', sep=',', columns=('customer_id', 'name', 'phone', 'email'))

        conn.commit()



        cur.execute("""
            SELECT * FROM customer;
        """)
        print(cur.fetchall())


        cur.execute("""
            CREATE TABLE IF NOT EXISTS staff (
                staff_id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                role VARCHAR(100) NOT NULL,
                phone VARCHAR(100) NOT NULL
            );
        """)

        conn.commit()

        with open(file_path_staff, 'r', encoding='utf-8') as f:
            next(f)
            cur.copy_from(f, 'staff', sep=',', columns=('staff_id', 'name', 'role', 'phone'))

        conn.commit()



        cur.execute("""
            SELECT * FROM staff;
        """)

        print(cur.fetchall())


        cur.execute("""
            CREATE TABLE IF NOT EXISTS menu_item (
                menu_item_id INT PRIMARY KEY,
                item_name VARCHAR(100) NOT NULL,
                price NUMERIC(10, 2) NOT NULL,
                category VARCHAR(100) NOT NULL
            );
        """)

        conn.commit()

        with open (file_path_menuitem, 'r', encoding='utf-8') as f:
            next(f)
            cur.copy_from(f, 'menu_item', sep=',', columns=('menu_item_id', 'item_name', 'price', 'category'))


        conn.commit()

        cur.execute("""
            SELECT * FROM menu_item;
        """)

        print(cur.fetchall())

        cur.execute("""
                   CREATE TABLE IF NOT EXISTS orders (
                       order_id INT PRIMARY KEY,
                       order_date VARCHAR(100) NOT NULL,
                       order_type VARCHAR(100) NOT NULL,
                       customer_id INT,
                       staff_id INT,
                       FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
                       FOREIGN KEY (staff_id) REFERENCES staff(staff_id)

                   );
               """)

        conn.commit()

        with open(file_path_orders, 'r', encoding='utf-8') as f:
            next(f)
            cur.copy_from(f, 'orders', sep=',', columns=('order_id', 'order_date', 'order_type', 'customer_id', 'staff_id'))

        conn.commit()

        cur.execute("""
                   SELECT * FROM orders;
               """)

        print(cur.fetchall())

        cur.execute("""
                    CREATE TABLE IF NOT EXISTS order_item (
                        order_item_id INT PRIMARY KEY,
                        order_id INT,
                        menu_item_id INT,
                        quantity INT CHECK (quantity > 0),
                        FOREIGN KEY (order_id) REFERENCES orders(order_id),
                        FOREIGN KEY (menu_item_id) REFERENCES menu_item(menu_item_id)
                    );
                """)

        conn.commit()

        with open(file_path_orderitme, 'r', encoding='utf-8') as f:
            next(f)
            cur.copy_from(f, 'order_item', sep=',', columns=('order_item_id', 'order_id', 'menu_item_id', 'quantity'))

        conn.commit()

        cur.execute("""
                    SELECT * FROM order_item;
                """)

        print(cur.fetchall())

        cur.execute("""
                    CREATE TABLE IF NOT EXISTS payment (
                        payment_id INT PRIMARY KEY,
                        order_id INT,
                        payment_date DATE,
                        amount INT NOT NULL,
                        payment_method VARCHAR(20) NOT NULL,
                        FOREIGN KEY (order_id) REFERENCES orders(order_id)
                    );
                """)

        conn.commit()

        with open(file_path_payment, 'r', encoding='utf-8') as f:
            next(f)
            cur.copy_from(f, 'payment', sep=',', columns=('payment_id', 'order_id', 'payment_date', 'amount', 'payment_method'))

        conn.commit()

        cur.execute("""
                    SELECT * FROM payment;
                """)

        print(cur.fetchall())

        cur.execute("""
            CREATE TABLE IF NOT EXISTS reservation (
                reservation_id INT PRIMARY KEY,
                customer_id INT,
                table_number INT NOT NULL, 
                reservation_date DATE,
                reservation_time TIME
            );
        """)

        conn.commit()

        with open(file_path_reservation, 'r', encoding='utf-8') as f:
            next(f)

            cur.copy_from(f, 'reservation', sep=',', columns=('reservation_id', 'customer_id', 'table_number', 'reservation_date', 'reservation_time'))

        conn.commit()


        cur.execute("""
            SELECT * FROM reservation;
        """)

        print(cur.fetchall())

