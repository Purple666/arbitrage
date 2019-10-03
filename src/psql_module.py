import psycopg2

def read_config(filename):
    """
    reads and extracts various data from a given config file.
    """
    with open(filename, 'r') as file:
        HOST = file.readline().split(':')[1].strip()
        PORT = int(file.readline().split(':')[1].strip())
        DB   = file.readline().split(':')[1].strip()
        USER = file.readline().split(':')[1].strip()
        PASSWORD = file.readline().split(':')[1].strip()
    return (HOST, PORT, DB, USER, PASSWORD)



def connect_to_psql(filename):
    """
    Takes in kev-value file containing HOST, PORT, DB, USER, and PASSWORD needed to connect to your postgresql database.
    Returns connection.
    """
    HOST, PORT, DB, USER, PASSWORD = read_config(filename)
    connect_str =  "host=" + HOST + " port=" + str(PORT) + " dbname=" + DB + " user=" + USER + " password=" + PASSWORD 
    conn = psycopg2.connect(connect_str)
    return conn


def create_table(conn, table_name, schema_string):
    """
    Creates a table w/ table_name and schema schema_string in database.
    """
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE " + table_name + schema_string + ";")
    cursor.close()


def write_row(cursor, table_name, row):
    """
    Takes the result (row: list of floats) and writes it to table table_name.
    """
    values = str(tuple(row.collect()))
    string = "INSERT INTO " + table_name + " values " + values + ";"
    cursor.execute(string)


