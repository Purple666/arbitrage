import psycopg2
import csv
from read_config import read_config
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

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
    """"
    returns the product of the weights of a given cycle's edges
    """
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
    tmp = filename[-6:-4]
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

def combineDataFrames(dfs):
    """
    Given a list of spark dataframes, combine them one on top another and return the resulting dataframe.
    """
    res = dfs[0]
    for k in range(1, len(dfs)):
        res = res.union(dfs[k])
    return res

def saveDataframeAsOneFile(df, directory):
      """
      Takes a spark dataframe and outputs it as a single-file, headerless csv file 'part-00000' inside the designated directory.
      """
      df.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile(directory)
      return None

def main():
    TABLE = 'arbitrages'

    ## create currencies dictionary
    currency_file = csv.reader(open('currencies_250k.csv', 'r'),
                            delimiter = ',')
    # read header so it's not added to currencies dict
    next(currency_file)

    currencies = {}
    for pair in currency_file:
        id, currency = pair
        # currencies[id] = currency
        currencies[currency] = id

    ## read pairs and create the graph's edges dictionary
    pair_file = csv.reader(open('pairs_250k.csv', 'r'), delimiter = ',')
    next(pair_file) # skip over header row

    pairs = []
    edges = {node: {} for node in currencies.keys()}
    for quartet in pair_file:
        num_base, num_quote, base, quote = quartet
        edges[base][quote] = 0.0
        edges[quote][base] = 0.0
        pairs.append((base,quote))
    # print(edges)

    ## read cycles
    # cycle1 = [('A','B'), ('B','C'), ('C','A')]
    cycle_file = csv.reader(open('cycles_250k.txt', 'r'), delimiter = ',')
    cycles = []
    for cycle in cycle_file:
        tmp = []
        for k, v in enumerate(cycle):
            if k + 1 < len(cycle):
                tmp.append((v, cycle[k + 1]))
            else:
                tmp.append((v, cycle[0]))
        cycles.append(tmp)
    # print(cycles)


    ## read ticker files and combine and sort them
    exchange = 'bfnx/'
    
    # get file names
    name_files = csv.reader(open('filenames_250k.csv', 'r'),
                            delimiter = ',')
    file_names = []
    next(name_files)
    for row in name_files:
        file_names.append(row)
    print(file_names)

    # define a list of spark dataframes
    dfs = []
    for triplet in file_names:
          file_name, base, quote = tuple(triplet)
          tmp_df = (spark.read.format('csv')
                    .option('header', 'true')
                    .option("ignoreLeadingWhiteSpace", True)
                    .option("ignoreTrailingWhiteSpace", True)
                    .option("inferSchema", True)
                    .load(exchange + file_name))\
                    .withColumn('dataframe', lit(base + '-' + quote))
          dfs.append(tmp_df)


    res = combineDataFrames(dfs)
    res = res.sort(res.timestamp)

    # write res to disk
    tmp_file_path = 'tmp'
    saveDataframeAsOneFile(res, tmp_file_path)
    # combined_file = csv.reader(open(tmp_file_path + '/part-00000', 'r'),
    #                            delimiter = ',')


    ## Connecting to psql
    conn = connect_to_psql('psql.config')
    create_table(conn, TABLE, '(cycle1 real, cycle2 real, cycle3 real)')
    cursor = conn.cursor()

    

    
        
    ## Reading price from files
    # print(read_price_and_update(data_path + 'AB.csv', edges))
    # print(read_price_and_update(data_path + 'BC.csv', edges))
    # print(read_price_and_update(data_path + 'AD.csv', edges))
    # print(read_price_and_update(data_path + 'AC.csv', edges))
    # print(read_price_and_update(data_path + 'CD.csv', edges))
    

    # ## Defining cycles
    # cycle1 = [('A','B'), ('B','C'), ('C','A')]
    # cycle2 = [('A','B'), ('B','C'), ('C','D'), ('D','A')]
    # cycle3 = [('A','C'), ('C','D'), ('D','A')]
    # cycles = [cycle1, cycle2, cycle3]


    # ## Calculate value of the cycles
    # calculate = lambda x: cycle_ratio(x, edges)
    # A = sc.parallelize(cycles)
    # B = A.map(calculate)


    # ## Write result to database
    # write_row(cursor, TABLE, B)


    # ## Commit writes to database
    # cursor.execute("COMMIT")


    # ## Check that data is written to database
    # cursor.execute("SELECT * from cycles;")
    # rows = cursor.fetchall()
    # print("-----------------")
    # print(rows)
    # print("-----------------")


    ## Close connection
    cursor.close()
    conn.close()


main()
    
