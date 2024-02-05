import psycopg2
from psycopg2 import IntegrityError

def read_credentials(file: str):
    credentials = {}
    with open(file, 'r') as file:
        for line in file:
            key = line.strip().split(": ")[0]
            value = line.strip().split(": ")[1]
            credentials[key] = value
    return credentials

def connect_to_database(credentials: dict):
    # Establish a connection to the PostgreSQL database
    try:
        conn = psycopg2.connect(
            host = credentials["host"],
            port = credentials["port"],
            database = credentials["database"],
            user = credentials["user"],
            password = credentials["password"]
        )
        print("Database connected successfully")

    except:
        print("Database not connected successfully")

    return conn

def add_user(conn, user: dict):
    query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'users';"

    cursor = conn.cursor() 
    cursor.execute(query)

    columns = cursor.fetchall()
    column_names = [column[0] for column in columns]
    for key in user.keys():
        if key not in column_names:
            raise (ValueError(f"Json key '{key}' not matching any table column name"))
        user[key] = f"'{user[key]}'"

    query = f"SELECT EXISTS (SELECT 1 FROM users WHERE username = {user['username']}) AS value_exists;"
    cursor.execute(query)
    already_exists = cursor.fetchall()[0][0]
    if already_exists:
        raise (IntegrityError(f"Username aready exists"))
    
    query = f"SELECT EXISTS (SELECT 1 FROM users WHERE credentials = {user['credentials']}) AS value_exists;"
    cursor.execute(query)
    already_exists = cursor.fetchall()[0][0]
    if already_exists:
        raise (IntegrityError(f"Credentials aready exists"))
        
    insert_query = f"INSERT INTO users ({', '.join(user.keys())}) VALUES ({', '.join(user.values())});"
    values = list(user.values())
    cursor.execute(insert_query, values)
    conn.commit()

    cursor.close()

credentials = read_credentials("credentials.txt")
conn = connect_to_database(credentials)
user_dict = {"username": "yuk", "permission": None, "credentials": "yuk32"}
add_user(conn, user_dict)
conn.close()