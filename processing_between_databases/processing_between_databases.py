import psycopg2

def connect_to_database(dbname):
    # Establish a connection to the PostgreSQL database
    try:
        conn = psycopg2.connect(
            host = "test-data-1-instance-1.cteav6vpg16n.eu-central-1.rds.amazonaws.com",
            port = "5432",
            database = dbname,
            user = "postgres",
            password = "ecotenistheway"
        )
        print("Database connected successfully")
    except:
        print("Database not connected successfully")

    return conn

def map_algebra(sql_script_path, conn):
    with open(sql_script_path, 'r') as file:
        sql_script = file.read()
    cursor = conn.cursor()
    cursor.execute(sql_script)
    result_data = cursor.fetchall()
    cursor.close()
    conn.close()
    return result_data

def upload_result(target_conn, result_data):
    target_cursor = target_conn.cursor()
    target_cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.test (
            rast raster
        );""")
    target_conn.commit()
    for row in result_data:
        target_cursor.execute("""INSERT INTO public.test (rast) VALUES (%s)""", (row[0],))
    target_conn.commit()
    target_cursor.close()
    target_conn.close()


sql_script = "map_algebra_test.sql"
conn = connect_to_database("dbsample")
result_data = map_algebra(sql_script, conn)
target_conn = connect_to_database("third_module")
upload_result(target_conn, result_data)