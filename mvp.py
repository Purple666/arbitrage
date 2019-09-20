import psycopg2
from read_config import read_config
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

## Creating spark context and session
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.master("local[*]").getOrCreate()
####***********************************************


def connect_to_psql(filename):
    """
    Takes in kev-value file containing HOST, PORT, DB, USER, and PASSWORD needed to connect to your postgresql database.
    Returns connection.
    """
    HOST, PORT, DB, USER, PASSWORD = read_config('psql.config')
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
    return None



def cycle_ratio(cycle, edges):
    res = 1.0
    for x, y in cycle:
        res = res * edges[x][y]
    return res


def read_price_and_update(filename, edges):
    """
    Takes in filename (of form AB.csv), where 1st letter refers to base currency and 2nd letter refers to the quoted currency.

    For example, if the price listed in AB.csv is 1.3, then that means B is worth 1.3 A.

    Returns a triplet: (base currency, quoted currency, price)
    """
    tmp = filename[:-3]
    base, quoted = tmp[0].strip(), tmp[1].strip()

    with open(filename, 'r') as file:
        file.readline()
        price = float(file.readline().split(',')[1].strip())
        edges[base][quoted] = price
        if abs(price) > 1.0e-8:
            edges[quoted][base] = 1.0 / price
        elif price > 0:
            edges[quoted][base] = 1.0e8
        else:
            edges[quoted][base] = -1.0e8
        return (base, quoted, price)



def write_row(cursor, table_name, row):
    """
    Takes the result (row: list of floats) and writes it to table table_name.
    """
    values = str(tuple(row.collect()))
    string = "INSERT INTO " + table_name + " values " + values + ";"
    cursor.execute(string)
    return None

def main():
    TABLE = 'cycles'


    ## Connecting to psql
    conn = connect_to_psql('psql.config')
    create_table(conn, TABLE, '(cycle1 real, cycle2 real, cycle3 real)')
    cursor = conn.cursor()

    
    ## Initializing nodes and edges
    edges = {}
    for node in nodes: edges[node] = {}

    
    ## Reading price from files
    print(read_price_and_update('AB.csv', edges))
    print(read_price_and_update('BC.csv', edges))
    print(read_price_and_update('AD.csv', edges))
    print(read_price_and_update('AC.csv', edges))
    print(read_price_and_update('CD.csv', edges))
    

    ## Defining cycles
    cycle1 = [('A','B'), ('B','C'), ('C','A')]
    cycle2 = [('A','B'), ('B','C'), ('C','D'), ('D','A')]
    cycle3 = [('A','C'), ('C','D'), ('D','A')]
    cycles = [cycle1, cycle2, cycle3]


    ## Calculate value of the cycles
    calculate = lambda x: cycle_ratio(x, edges)
    A = sc.parallelize(cycles)
    B = A.map(calculate)


    ## Write result to database
    write_row(cursor, TABLE, B)


    ## Commit writes to database
    cursor.execute("COMMIT")


    ## Check that data is written to database
    cursor.execute("SELECT * from cycles;")
    rows = cursor.fetchall()
    print("-----------------")
    print(rows)
    print("-----------------")


    ## Close connection
    cursor.close()
    conn.close()


main()
    
