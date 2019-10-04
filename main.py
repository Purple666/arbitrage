import csv
import boto3
from smart_open import open


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


def s3_open(filepath):
      return open(filepath, transport_params = dict(session = session))

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

def initialize_queue_header_edges(files):
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


def update_edges(edges, files, current_queue, header):
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
        
        row = next(files[key])
        current_queue[key] = get_time_bid_ask(row)

    return new_timestamp

# initialize files dict, pairs list, and currencies set

def initialize_files(directory, YEAR, MONTH, verbose = False):
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
    
    for f in s3_open(path + 'files.txt'):
        if YEAR_MONTH in f:
            tmp = f.split('_')[2].lower()
            x, y = tmp[:3], tmp[3:]
            pair = '-'.join([x, y])
            currencies.update({x, y})
            
            filename = f.strip()
            pairs.append(pair)
            if verbose: print(filename)
            files[pair] = csv.reader(s3_open(path + filename), skipinitialspace = True)
    return files, pairs, currencies
    

if __name__ == "__main__":


      HOST, PORT, DB, USER, PASSWORD, session = read_config_and_create_s3_session('config.txt')

      files, pairs, currencies = initialize_files('s3://consumingdata/testing/', '2001', '01', True)    
      current_queue, header, edges = initialize_queue_header_edges(files)

      
      for k in range(3):
            timestamp = update_edges(edges, files, current_queue, header)
            print(edges)
