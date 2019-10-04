from pgcopy import CopyManager
import csv
from psql_module import connect_to_psql, create_table, write_row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

## Creating spark context and session
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()
####***********************************************


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
      Takes a spark dataframe and outputs it as a single file, headerless csv file 'part-00000' inside the designated directory.
      """
      df.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile(directory)




def main():
        
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

    ## create currencies dictionary
    currency_file = csv.reader(open('../graph_data_files/currencies_250k.csv', 'r'),
                            delimiter = ',')
    # read header so it's not added to currencies dict
    next(currency_file)

    currencies = {}
    for pair in currency_file:
        id, currency = pair
        currencies[id] = currency

    ## read pairs and create the graph's edges dictionary
    pair_file = csv.reader(open('../graph_data_files/pairs_250k.csv', 'r'), delimiter = ',')
    next(pair_file) # skip over header row

    pairs = {}
    edges = {node: {} for node in currencies.values()}
    for k, quartet in enumerate(pair_file):
        num_base, num_quote, base, quote = quartet
        edges[base][quote] = 0.0
        edges[quote][base] = 0.0
        pairs[base + '-' + quote] = (k, base,quote)
    

    ## read cycles
    cycle_file = csv.reader(open('../graph_data_files/cycles_250k.txt', 'r'), delimiter = ',')
    cycles = []
    for n, cycle in enumerate(cycle_file):
        tmp = []
        for k, v in enumerate(cycle):
            
            if k + 1 < len(cycle):
                tmp.append((currencies[v], currencies[cycle[k + 1]]))
            else:
                tmp.append((currencies[v], currencies[cycle[0]]))
        cycles.append(tmp)


    ## read ticker files and combine and sort them
    read = True
    tmp_file_path = 'tmp'
    print("-------------------------")
    print("    starting to read bfnx files")
    print("-------------------------")

    if not read:
        exchange = '../bfnx/'
    
        # get file names
        name_files = csv.reader(open('../graph_data_files/filenames_250k.csv', 'r'),
                                delimiter = ',')
        file_names = []
        next(name_files)
        for row in name_files:
            file_names.append(row)

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
    conn = connect_to_psql('../psql.config')

    ## create table if it doesn't exist
    arbitrages_table = 'arbitrages'
    arbitrages_schema = '(id bigint, time bigint, cycle integer, price real)'
    arbitrages_col = tuple(['id', 'time', 'cycle', 'price'])

    pairs_table = 'pairs'
    pairs_schema = '(id bigint, time bigint'
    for k in range(len(pairs)):
        pairs_schema += ", pair" + str(k + 1) + " real"
    pairs_schema += ")"
    pairs_col = tuple(['id', 'time'] + ['pair' + str(k + 1) for k in range(len(pairs))])

    print("col_names: ", arbitrages_col)
    print("col_names: ", pairs_col)

    
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS " + arbitrages_table + " CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS " + pairs_table + " CASCADE;")
    create_table(conn, arbitrages_table, arbitrages_schema)
    create_table(conn, pairs_table, pairs_schema)


    ## Reading and updating data
    
    current_state = {}
    for pair in pairs.keys():
        current_state[pair] = False



    ## Keep updating current_state until it's ready
    from time import time
    log = open('arbitrage.log', 'w')
    log.close()
    t_f = time()

    idx = 0
    calculate = lambda x: cycle_ratio(x, edges)

    arbitrages_values = []
    pairs_values = []
    for row in combined_file:
        idx += 1
        update(edges, current_state, row)
        if is_ready(current_state):
            timestamp = row[0]
            transaction_pair = row[5]
            C = map(calculate, cycles)

            ## Write result to database
            for n, val in enumerate(C):
                tmp = [idx, int(timestamp), n + 1, val]
                arbitrages_values.append(tuple(tmp))

            tmp = [idx, int(timestamp)] + list(current_state.values())
            pairs_values.append(tuple(tmp))
                
            if idx % 100000 == 0: 
                # write to arbitrages table
                mgr = CopyManager(conn, arbitrages_table, arbitrages_col)
                mgr.copy(arbitrages_values) 
                conn.commit() ## Commit writes to database
                # write to pairs table
                mgr = CopyManager(conn, pairs_table, pairs_col)
                mgr.copy(pairs_values) 
                conn.commit() ## Commit writes to database
                # reset arbitrages_values & pairs_values
                arbitrages_values = []
                pairs_values = []
                with open('arbitrage.log', 'a') as log:
                    t_i = t_f
                    t_f = time()
                    log.write('n = ' + str(idx) + ', ' + str(t_f - t_i) + ', ' + str((t_f - t_i) / 10000.0) + ', ' + '\n')
            
        
    ## Copy whatever's left
    # write to arbitrages table
    mgr = CopyManager(conn, arbitrages_table, arbitrages_col)
    mgr.copy(arbitrages_values) 
    conn.commit() ## Commit writes to database
    # write to pairs table
    mgr = CopyManager(conn, pairs_table, pairs_col)
    mgr.copy(pairs_values) 
    conn.commit() ## Commit writes to database

    with open('arbitrage.log', 'a') as log:
        t_i = t_f
        t_f = time()
        log.write('n = ' + str(idx) + ', ' + str(t_f - t_i) + ', ' + str((t_f - t_i) / 10000.0) + ', ' + '\n')

    ## Close connection
    cursor.close()
    conn.close()


main()
    
