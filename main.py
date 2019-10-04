import psycopg2
from pgcopy import CopyManager
import csv
import boto3
import smart_open
from pyspark import SparkConf, SparkContext

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

def read_config_and_create_s3_session(filename):
    """
    reads and extracts various data from a given config file.
    """
    with open(filename, 'r') as file:
        HOST = file.readline().split(':')[1].strip()
        PORT = int(file.readline().split(':')[1].strip())
        DB   = file.readline().split(':')[1].strip()
        USER = file.readline().split(':')[1].strip()
        PASSWORD = file.readline().split(':')[1].strip()
        AWS_ACCESS_ID = file.readline().split(':')[1].strip()
        AWS_SECRET_KEY = file.readline().split(':')[1].strip()
        session = boto3.Session(aws_access_key_id = AWS_ACCESS_ID,
                                aws_secret_access_key = AWS_SECRET_KEY)
    return (HOST, PORT, DB, USER, PASSWORD, session)


def s3_open(filepath, session):
      return smart_open.open(filepath, transport_params = dict(session = session))

def create_header_dict(headers):
    """
    Given a list of col names, returns a dictionary.
    
    The dictionary is to allow user to switch between col name and col index.
    """
    header = {}
    for k, col in enumerate(headers):
        header[col] = k
        header[k] = col
    return header

def get_time_bid_ask(line):
    """
    Takes a list line = [timestamp, bid, ask, volume] and
    returns [int(timestamp), float(bid), float(ask)]
    """
    timestamp, bid, ask, volume = tuple(line)
    return [int(timestamp.replace(' ', '')), float(bid), float(ask)]

def initialize_queue_header_edges(files, pairs, currencies):
    """
    Given a list of tuples (pair_name, pair_file), where pair_name is a string
    and pair_file is a _csv.reader object, returns current_queue, header dict, and edges.
    Assumes header is present in all files.
    """
    # initialize current_queue
    current_queue = {pair: [] for pair in pairs}
    
    # initialize headers dict
    headers = ['timestamp', 'bid', 'ask', 'amount']
    for pair_name, pair_file in files.items():
        row = next(pair_file)
        current_queue[pair_name] = get_time_bid_ask(row)
        
    header_dict = create_header_dict(headers)
        
    # initialize edges
    edges = {x: {} for x in currencies}
    for pair in pairs:
        x, y = pair.split('-')
        edges[x][y] = 0.0
        edges[y][x] = 0.0
        
    return current_queue, header_dict, edges

def get_min_timestamps(current_queue, header):
    """
    Given the current_queue and the header,
    returns an ordered pair (a tuple) containing the min timestamp and a list of
    pairs with that timestamp.
    """
    tmp = [(k, int(v[header['timestamp']])) for k, v in current_queue.items()]
    tmp = sorted(tmp, key = lambda x: x[1])
    new_timestamp = tmp[0][1]
    min_keys = list(map(lambda x: x[0], filter(lambda x: x[1] == new_timestamp, tmp)))
    return new_timestamp, min_keys


def update_edges(edges, files, current_queue, pairs, header):
    """
    updates edges and current_queue and returns new_timestamp
    """
    new_timestamp, min_keys = get_min_timestamps(current_queue, header)
    
    for key in min_keys:
        idx = pairs.index(key)
        x, y = pairs[idx].split('-')
        timestamp, bid, ask = tuple(current_queue[key])
        edges[x][y] = bid
        edges[y][x] = ask
        
        try:
            row = next(files[key])
            current_queue[key] = get_time_bid_ask(row)
        except StopIteration:
            row = current_queue[key]
            row[0] = int('9' * 17)
            current_queue[key] = row

    if new_timestamp == int('9' * 17): 
        new_timestamp = -1

    return new_timestamp

# initialize files dict, pairs list, and currencies set

def initialize_files(directory, YEAR, MONTH, session, verbose = False):
    """
    Reads the file 'files.txt' from path = directory + YEAR + '/'.
    'files.txt' is assumed to contain a list of exchange files with each
    transaction separated by a newline character.
    
    Returns files = (pair name, _csv.reader), pairs list, and currencies set
    """
    assert isinstance(YEAR, str) , "the parameter YEAR needs to be a 4 digit string"
    assert isinstance(MONTH, str), "the parameter MONTH needs to be a 2 digit string"
    assert len(YEAR) == 4,  "the parameter YEAR needs to be a 4 digit string"
    assert len(MONTH) == 2, "the parameter MONTH needs to be a 2 digit string"
    path = directory + YEAR + '/'
    YEAR_MONTH = YEAR + MONTH
    
    files = {}
    pairs = []
    currencies = set()
    
    for f in s3_open(path + 'files.txt', session):
        if YEAR_MONTH in f:
            tmp = f.split('_')[2].lower()
            x, y = tmp[:3], tmp[3:]
            pair = '-'.join([x, y])
            currencies.update({x, y})
            
            filename = f.strip()
            pairs.append(pair)
            if verbose: print(filename)
            files[pair] = csv.reader(s3_open(path + filename, session),
                                     skipinitialspace = True)
    return files, pairs, currencies
    

def cycle_ratio(cycle, edges, currencies):
    """"
    returns the product of the weights of a given cycle's edges
    """
    res = 1.0
    for x, y in cycle:
        if x in currencies and y in currencies:
            if y in edges[x].keys():
                res = res * edges[x][y]
        else:
            return 0
    return res


def read_cycles():

    # create dictionary of currencies
    forex_keys = {}
    file = csv.reader(open('find_cycles/forex.keys', 'r'), delimiter = ',', skipinitialspace = True)
    next(file)
    for line in file:
        x, y = tuple(line)
        y = y.lower()
        forex_keys[x] = y
        forex_keys[y] = x

    # create list of cycles
    cycles = []
    file = csv.reader(open('find_cycles/cycles.txt', 'r'), delimiter = ',', skipinitialspace = True)
    for line in file:
        cycle = []
        cycle_r = []
        for k, v in enumerate(line):
            if k + 1 < len(line):
                cycle.append((forex_keys[v], forex_keys[line[k + 1]]))
                cycle_r.append((forex_keys[line[k + 1]], forex_keys[v]))
            else:
                cycle.append((forex_keys[v], forex_keys[line[0]]))
                cycle_r.append((forex_keys[line[0]], forex_keys[v]))
        cycles.append(cycle)
        cycles.append(cycle_r)
    return forex_keys, cycles
        



def pipeline(YEAR, MONTH):
    HOST, PORT, DB, USER, PASSWORD, session = read_config_and_create_s3_session('config.txt')

    files, pairs, currencies = initialize_files('s3://consumingdata/testing/', 
                                                YEAR, MONTH, session, True)    
    current_queue, header, edges = initialize_queue_header_edges(files, pairs, currencies)
    curr_dict, cycles = read_cycles()




    ## Connecting to psql
    conn = connect_to_psql('config.txt')

    ## create table if it doesn't exist
    forex_table = 'forex_' + YEAR + '_' + MONTH
    forex_schema = '(id bigint, time bigint, cycle integer, ratio real)'
    forex_col = tuple(['id', 'time', 'cycle', 'ratio'])

    print(pairs)

    pairs_table = 'forex_pairs_' + YEAR + '_' + MONTH
    pairs_schema = '(id bigint, time bigint'
    for k in range(len(pairs)):
        pairs_schema += ", pair" + str(k + 1) + " real"
    pairs_schema += ")"
    pairs_col = tuple(['id', 'time'] + ['pair' + str(k + 1) for k in range(len(pairs))])

    print("col_names: ", forex_col)
    print("col_names: ", pairs_col)

    
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS " + forex_table + " CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS " + pairs_table + " CASCADE;")
    create_table(conn, forex_table, forex_schema)
    create_table(conn, pairs_table, pairs_schema)




    ######################
    idx = 0
    forex_values = []
    pairs_values = []
    timestamp = update_edges(edges, files, current_queue, pairs, header)

    while timestamp > 0:
        C = map(lambda x: cycle_ratio(x, edges, currencies), cycles)

        ## Write result to database
        for n, val in enumerate(C):
            tmp = [idx, int(timestamp), n + 1, val]
            forex_values.append(tuple(tmp))
           
        idx += 1
        if idx % 10 == 0: 

            # write to forex table
            mgr = CopyManager(conn, forex_table, forex_col)
            mgr.copy(forex_values) 
            conn.commit() ## Commit writes to database

            # write to pairs table

            forex_values = []
            pairs_values = []
            
        timestamp = update_edges(edges, files, current_queue, pairs, header)

    mgr = CopyManager(conn, forex_table, forex_col)
    mgr.copy(forex_values) 
    conn.commit() ## Commit writes to database
    for k in range(3):
        print(cycles[k])

    cursor.close()
    conn.close()


if __name__ == "__main__":

    sc = SparkContext.getOrCreate()

    workqueue = []
    for year in range(2001, 2002):
        for month in range(101, 103):
            YEAR = str(year)
            MONTH = str(month)[1:]
            workqueue.append((YEAR, MONTH))
    workqueue = sc.parallelize(workqueue)

    workqueue.foreach(lambda x: pipeline(x[0], x[1]))
    # pipeline(YEAR, MONTH)
    
    
