import psycopg2


def create_db(conn):
    """Функция, создающая структуру БД (таблицы)."""
    cur = conn.cursor()
    cur.execute("""
            CREATE TABLE IF NOT EXISTS Client(
                id SERIAL PRIMARY KEY NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL
            );
            """)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS PhoneData(
                id SERIAL PRIMARY KEY NOT NULL,
                phone TEXT UNIQUE,
                client_id INTEGER,
                CONSTRAINT pd FOREIGN KEY (client_id) REFERENCES Client(id) ON DELETE CASCADE
            );
            """)
    conn.commit()

    pass


def add_client(conn, first_name, last_name, email, phones=None):
    """Функция, позволяющая добавить нового клиента."""
    cur = conn.cursor()
    cur.execute("""
            INSERT INTO Client(first_name, last_name, email) VALUES(%s, %s, %s);
            """, (first_name, last_name, email))
    conn.commit()
    if phones is not None:
        for phone in phones:
            cur.execute("""
                SELECT id FROM Client WHERE email=%s;
                """, (email,))
            client_id = cur.fetchone()
            add_phone(conn, phone, *client_id)
            pass
    else:
        pass
    conn.commit()
    pass


def add_phone(conn, phone, client_id):
    """Функция, позволяющая добавить телефон для существующего клиента."""
    cur = conn.cursor()
    cur.execute("""
            INSERT INTO PhoneData(phone, client_id) VALUES(%s, %s);
            """, (phone, client_id))
    conn.commit()
    pass


def change_client(conn, client_id, **dataset):
    """Функция, позволяющая изменить данные о клиенте."""
    cur = conn.cursor()
    for key, value in dataset.items():
        if key == 'first_name':
            cur.execute("""
                UPDATE Client SET first_name=%s WHERE id=%s;
                """, (value, client_id))
        elif key == 'last_name':
            cur.execute("""
                UPDATE Client SET last_name=%s WHERE id=%s;
                """, (value, client_id))
        elif key == 'email':
            cur.execute("""
                UPDATE Client SET email=%s WHERE id=%s;
                """, (value, client_id))
        elif key == 'phones':
            cur.execute("""
                UPDATE PhoneData SET phone=%s WHERE client_id=%s;
                """, (value, client_id))
    conn.commit()
    pass


def delete_phone(conn, phone, client_id):
    """Функция, позволяющая удалить телефон для существующего клиента."""
    cur = conn.cursor()
    cur.execute("""
            DELETE FROM PhoneData WHERE phone='%s' AND client_id=%s;
            """, (phone, client_id))
    conn.commit()
    pass


def delete_client(conn, client_id):
    """Функция, позволяющая удалить существующего клиента."""
    cur = conn.cursor()
    cur.execute("""
            DELETE FROM Client WHERE id=%s RETURNING *;
            """, (client_id,))
    conn.commit()
    pass


def find_client(conn, **dataset):
    """Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону."""
    cur = conn.cursor()
    for key, value in dataset.items():
        if key == 'first_name':
            cur.execute("""
                SELECT * FROM client WHERE first_name=%s;
                """, (value,))
        elif key == 'last_name':
            cur.execute("""
                SELECT * FROM client WHERE last_name=%s;
                """, (value,))
        elif key == 'email':
            cur.execute("""
                SELECT * FROM client WHERE email=%s;
                """, (value,))
        elif key == 'phones':
            cur.execute("""
                SELECT * FROM client c
                JOIN phoneData p ON c.id = p.client_id 
                WHERE phone=%s;
                """, (value,))
        print(cur.fetchall())
    pass


with psycopg2.connect(dbname='clients_db', user='postgres',
                      password='', host='localhost') as conn:
    cur = conn.cursor()
    cur.execute("""
    DROP TABLE IF EXISTS PhoneData;
    DROP TABLE IF EXISTS Client;
    """)

    create_db(conn)
    add_client(conn, 'Anton', 'Molot', '1@mail.ru', phones=[89111111111, 89222222222, 89333333333])
    add_client(conn, 'Роман', 'Листов', '2@mail.ru', phones=[89444444444])
    add_client(conn, 'Сергей', 'Зайцев', '3@mail.ru')
    add_phone(conn, 89555555555, 3)
    delete_phone(conn, 89222222222, 1)
    delete_client(conn, 1)
    change_client(conn, 2, first_name='Tom', last_name='Tami', email='4@mail.ru', phones='89666666666')
    find_client(conn, first_name='Tom')
    find_client(conn, phones='89555555555')
    pass
conn.close()