import psycopg2
from read_config import read_config

def read_people():
    import psycopg2
    connect_str =  "host=" + HOST + " port=" + str(PORT) + " dbname=" + DB + " user=" + USER + " password=" + PASSWORD 
    
                  
                  
    conn = psycopg2.connect(connect_str)
    cursor = conn.cursor()
    cursor.execute("""SELECT * from people;""")
    rows = cursor.fetchall()
    print("-----------------")
    print(rows)
    print("-----------------")
    cursor.close()
    conn.close()
    return None



HOST, PORT, DB, USER, PASSWORD = read_config('db.config')
read_people()
