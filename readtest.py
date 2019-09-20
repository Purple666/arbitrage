import psycopg2
from read_config import read_config

def read_people():
    import psycopg2
    connect_str =  "host=" + HOST + " port=" + str(PORT) + " dbname=" + DB + " user=" + USER + " password=" + PASSWORD 
    
                  
                  
    conn = psycopg2.connect(connect_str)
    cursor = conn.cursor()
    # cursor.execute("CREATE TABLE testtable (name text, age int);")
    # cursor.execute("INSERT INTO testtable values ('vincent', 54);")
    # cursor.execute("COMMIT")
    # cursor.execute("""SELECT * from testtable;""")
    cursor.execute("DROP TABLE testtable;")
    # rows = cursor.fetchall()
    # print("-----------------")
    # print(rows)
    # print("-----------------")
    cursor.close()
    conn.close()
    return None



HOST, PORT, DB, USER, PASSWORD = read_config('psql.config')
read_people()
