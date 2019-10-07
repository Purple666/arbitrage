import psycopg2
from pgcopy import CopyManager
import csv
import boto3
import smart_open

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
        row = next(pair_file[1])
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

    if new_timestamp == int('9' * 17): 
        new_timestamp = -1
    
    for key in min_keys:
        idx = pairs.index(key)
        x, y = pairs[idx].split('-')
        timestamp, bid, ask = tuple(current_queue[key])
        edges[x][y] = bid
        try:
            edges[y][x] = 1.0 / ask
        except ZeroDivisionError:
            edges[y][x] = 1e8;

        row = False
        try:
            row = next(files[key][1], False)
        except Exception as e:
            print("weird stop iteration or protocol error: ", e)
            print('csv parser: ', files[key][1])
            try:
                row = next(files[key][1], False)
            except Exception as e:
                pass
            
        if row: # Not EOF
            current_queue[key] = get_time_bid_ask(row)
        else:
            row = current_queue[key]
            row[0] = int('9' * 17)
            current_queue[key] = row
            

    return new_timestamp

# initialize files dict, pairs list, and currencies set

def initialize_files(directory, currencies, 
                     YEAR, MONTH, session, verbose = False):
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
    used_currencies = set()
    
    idx = 0
    for f in s3_open(path + 'files.txt', session):
        idx += 1
        print('filename: ', f, YEAR_MONTH in f)
        if YEAR_MONTH in f:
            tmp = f.split('_')[2].lower()
            x, y = tmp[:3], tmp[3:]
            if True: 
                print(x, y)
                print(f.strip(), x, x in currencies, y, y in currencies)
            if x in currencies and y in currencies:
                pair = '-'.join([x, y])
                used_currencies.update({x, y})
            
                filename = f.strip()
                pairs.append(pair)
                csv_file = s3_open(path + filename, session)
                parser = csv.reader(csv_file, skipinitialspace = True)
                files[pair] = (csv_file, parser)
    if verbose: 
        print("number of files: ", idx)
        print("number of files read: ", len(files))
        print("number of pairs: ", len(pairs))
    return files, pairs, used_currencies
    

def cycle_ratio(cycle, edges, currencies):
    """"
    returns the product of the weights of a given cycle's edges
    """
    r, s = 1.0, 1.0

    for x, y in cycle:
        if x in currencies and y in currencies:
            if y in edges[x].keys():
                r *= edges[x][y]
                s *= edges[y][x]
            else:
                return (0.0, 0.0)
        else:
            return (0.0, 0.0)
    return (r, s)


def read_cycles(max_cycle_length, require_USD, verbose = True):

    # create dictionary of currencies
    currency_dict = {}
    path = 'find_cycles/'
    with open(path + 'forex.keys', 'r') as f:
        csv_parser = csv.reader(f, delimiter = ',', skipinitialspace = True)
        next(csv_parser)
        for line in csv_parser:
            x, y = tuple(line)
            y = y.lower()
            currency_dict[x] = y
            currency_dict[y] = x

    # create list of cycles
    pairs = set()
    cycles = []
    currencies = set()
    with open(path + 'cycles.txt', 'r') as f:
        csv_parser = csv.reader(f, delimiter = ',', skipinitialspace = True)
        idx = 0
        for line in csv_parser:
            if require_USD:
                if currency_dict['usd'] not in line: continue  # only want cycles involving US dollars
            if len(line) > max_cycle_length: continue   # only want cycles that aren't too long
            idx += 1
            cycle = []
            for k, v in enumerate(line):
                x = currency_dict[v]
                y = currency_dict[line[k + 1]] if k + 1 < len(line) else currency_dict[line[0]]
                cycle.append((x, y))
                pairs.add(','.join([x, y]))
                currencies.update({x, y})
            cycles.append(cycle)
            if len(cycle) > max_cycle_length:
                print("cycle violation: ", cycle, len(cycle), max_cycle_length)

    if verbose: 
        print('number of cycles w/ usd and length = 3: ', idx)
    return currencies, currency_dict, cycles
        
def update_cycles_files_currencies(files, pairs, currencies, cycles, currency_dict):
    """
    Since the read_cycles method allows the user to ignore cycles that are too long or does not
    contain US dollar, some of the files in the dictionary files are no longer needed. We get
    rid of those here. We also get rid of the cycles that we do not have the data for.

    Returns: pairs
    """
    used_currencies = set()
    used_pairs = set()
    for cycle in cycles:
        for pair in cycle:
            x, y = pair
            used_currencies.update({x, y})
            used_pairs.add('-'.join([x, y]))

    # update currency_dict
    # for currency in currency_dict.keys():
    #     if currency not in used_currencies: currency_dict.pop(currency)

    # update pairs
    for pair in pairs:
        if pair not in used_pairs: pairs.remove(pair)

    # update files
    for name, pair in files.items():
        csv_file, csv_parser = pair
        if name not in used_pairs:
            csv_file.close()
            files.pop(name)

    return pairs

def close_files(files):
    """
    Closes file connections.
    """
    for k, v in files.items():
        v[0].close()
        print('closed connection to', k)


def pipeline(YEAR, MONTH):
    HOST, PORT, DB, USER, PASSWORD, session = read_config_and_create_s3_session('config.txt')

    currencies, currency_dict, cycles = read_cycles(max_cycle_length = 3, require_USD = True)
    files, pairs, currencies = initialize_files('s3://consumingdata/ascii/', 
                                                currencies,
                                                YEAR, MONTH, session, True)    
    current_queue, header, edges = initialize_queue_header_edges(files, pairs, currencies)



    ## Connecting to psql
    conn = connect_to_psql('config.txt')

    ## 
    arbitrages_table = 'arbitrages_' + YEAR + '_' + MONTH
    arbitrages_schema = '(id bigint, time bigint, cycle integer, ratio_f real, ratio_r real)'
    arbitrages_col = tuple(['id', 'time', 'cycle', 'ratio_f', 'ratio_r'])

    rates_table = 'rates_' + YEAR + '_' + MONTH
    rates_schema = '(id bigint, time bigint'
    rates_col = ['id', 'time']
    for k, pair in enumerate(pairs):
        x, y = tuple(pair.split('-'))
        f_pair = '_'.join([x, y])
        r_pair = '_'.join([y, x])
        rates_schema += ", " + f_pair + " real, " + r_pair + " real"
        rates_col += [f_pair, r_pair]
    rates_schema += ")"
    rates_col = tuple(rates_col)
    

    cycles_table = 'cycles_' + YEAR + '_' + MONTH
    cycles_schema = '(cycle int, pair1 text, pair2 text, pair3 text)'
    cycles_col = ('cycle', 'pair1', 'pair2', 'pair3')
    print("number of cycles: ", len(cycles))
    cycles_values = []
    for k, cycle in enumerate(cycles):
        row = [k + 1]
        for x, y in cycle:
            f_pair = '_'.join([x, y])
            row.append(f_pair)
        cycles_values.append(tuple(row))
        print(tuple(row))



    
    ## create tables
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS " + arbitrages_table + " CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS " + rates_table + " CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS " + cycles_table + " CASCADE;")
    create_table(conn, arbitrages_table, arbitrages_schema)
    create_table(conn, rates_table, rates_schema)
    create_table(conn, cycles_table, cycles_schema)


    ######################
    idx = 1
    arbitrages_values = []
    rates_values = []
    timestamp = update_edges(edges, files, current_queue, pairs, header)

    while timestamp > 0:
        C = map(lambda x: cycle_ratio(x, edges, currencies), cycles)

        ## Write result to database
        pre = [idx, int(timestamp)]

        # the ratio_f & ratio_r for each cycle
        for n, val in enumerate(C):
            r, s = val
            tmp = pre + [n + 1, r, s]
            arbitrages_values.append(tuple(tmp))

        # the edge values
        tmp = pre
        for pair in pairs:
            x, y = pair.split('-')
            tmp = tmp + [edges[x][y], edges[y][x]]
        rates_values.append(tuple(tmp))

        idx += 1
        if idx % 10000 == 0: print('idx = ', idx)
        if idx % 10000 == 0: 
            # write to forex table
            mgr = CopyManager(conn, arbitrages_table, arbitrages_col)
            mgr.copy(arbitrages_values) 
            conn.commit()         # Commit writes to database
            # write to pairs table
            mgr = CopyManager(conn, rates_table, rates_col)
            mgr.copy(rates_values) 
            conn.commit()         # Commit writes to database
            # reset values
            arbitrages_values = []
            rates_values = []

        timestamp = update_edges(edges, files, current_queue, pairs, header)


    # write values to tables
    mgr = CopyManager(conn, arbitrages_table, arbitrages_col)
    mgr.copy(arbitrages_values) 
    conn.commit()         # Commit writes to database
    mgr = CopyManager(conn, rates_table, rates_col)
    mgr.copy(rates_values) 
    conn.commit()         # Commit writes to database
    mgr = CopyManager(conn, cycles_table, cycles_col)
    mgr.copy(cycles_values) 
    conn.commit()         # Commit writes to database


    # close connections
    cursor.close()
    conn.close()
    close_files(files)
