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

    ## create currencies dictionary
    currency_file = csv.reader(open('currencies_250k.csv', 'r'),
                            delimiter = ',')
    # read header so it's not added to currencies dict
    next(currency_file)

    currencies = {}
    for pair in currency_file:
        id, currency = pair
        currencies[id] = currency
        # currencies[currency] = id

    ## read pairs and create the graph's edges dictionary
    pair_file = csv.reader(open('pairs_250k.csv', 'r'), delimiter = ',')
    next(pair_file) # skip over header row

    pairs = {}
    edges = {node: {} for node in currencies.values()}
    for k, quartet in enumerate(pair_file):
        num_base, num_quote, base, quote = quartet
        edges[base][quote] = 0.0
        edges[quote][base] = 0.0
        pairs[base + '-' + quote] = (k, base,quote)
    # print(edges)
    

    ## read cycles
    cycle_file = csv.reader(open('cycles_250k.txt', 'r'), delimiter = ',')
    cycles = []
    for cycle in cycle_file:
        tmp = []
        for k, v in enumerate(cycle):
            
            if k + 1 < len(cycle):
                tmp.append((currencies[v], currencies[cycle[k + 1]]))
            else:
                tmp.append((currencies[v], currencies[cycle[0]]))
        cycles.append(tmp)
    # print(cycles)



    ## read ticker files and combine and sort them
    read = True
    tmp_file_path = 'tmp'
    if not read:
        exchange = 'bfnx/'
    
        # get file names
        name_files = csv.reader(open('filenames_250k.csv', 'r'),
                                delimiter = ',')
        file_names = []
        next(name_files)
        for row in name_files:
            file_names.append(row)
        # print(file_names)

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
        saveDataframeAsOneFile(res, tmp_file_path)
    else:
        combined_file = csv.reader(open(tmp_file_path + '/part-00000', 'r'),
                                   delimiter = ',')
            


    ## Connecting to psql
    conn = connect_to_psql('psql.config')

    ## create table if it doesn't exist
    table_name = 'arbitrages'
    schema = '(time bigint, cycle integer, price real'
    for k in range(len(pairs)):
        schema += ", pair" + str(k + 1) + " real"
    schema += ")"
    
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS " + table_name + " CASCADE;")
    create_table(conn, table_name, schema)
    


    ## Reading and updating data
    
    current_state = {}
    for pair in pairs.keys():
        current_state[pair] = False

    
        
    ## Reading price from files
    def is_ready(state):
        """
        Returns True if all states are True, else False
        """
        for k, v in state.items():
            if not v:
                return False
        return True



    def update(edges, state, row):
        """
        Take data from row and update edges
        """
        time, transact_id, price, amount, seller, transact_pair = tuple(row)
        base, quote = tuple(transact_pair.split('-'))
        state[transact_pair] = float(price)
        edges[base][quote] = float(price)
        edges[quote][base] = 1.0 / float(price)
        return None

    ## Keep updating current_state until it's ready

    idx = 0
    A = sc.parallelize(cycles)
    calculate = lambda x: cycle_ratio(x, edges)
    for row in combined_file:
        idx += 1
        print("\n\n n =", idx, '\n')
        if idx == 1500: break
        update(edges, current_state, row)
        if is_ready(current_state):
            time = row[0]
            transaction_pair = row[5]
            # ## Calculate value of the cycles
            B = A.map(calculate)
            C = B.collect()

            # ## Write result to database
            # write_row(cursor, TABLE, B)
            for n, val in enumerate(C):
                values = str(time) + ', ' + str(n + 1) + ', ' + str(val)
                for k, v in current_state.items():
                    values += ', ' + str(v)
                string = "INSERT INTO " + table_name + " VALUES (" + values[:-2] + ");"
                # print(string)
                cursor.execute(string)


    # ## Commit writes to database
    cursor.execute("COMMIT")


    # ## Check that data is written to database
    cursor.execute("SELECT * from " + table_name + ";")
    rows = cursor.fetchall()
    # print("-----------------")
    # print("table: ", table_name)
    # print(rows)
    # print("-----------------")


    ## Close connection
    cursor.close()
    conn.close()


main()
    
