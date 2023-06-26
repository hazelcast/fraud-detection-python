import sys
import hazelcast
import csv
import multiprocessing
import json
import time
import os


def process_csv_chunk(filename, start, end, print_after_k):
    print(f'processing chunk {start} to {end}',flush=True)

    #Hazelcast client
    cluster_members = os.environ.get('HZ_ENDPOINT', '127.0.0.1')
    client = hazelcast.HazelcastClient(cluster_members=[cluster_members],use_public_ip=True)

    #load transactions in parallel (4 processes)
    transaction_map = client.get_map("transactions").blocking()
    #transaction_map = client.get_map("transactions")

    # Open the CSV file
    with open(filename, mode='r', encoding='utf-8') as f:
        # Create a CSV reader object
        reader = csv.reader(f,quotechar='"', delimiter=',',quoting=csv.QUOTE_ALL, skipinitialspace=True,doublequote=True)
        
        # Skip the header row
        next(reader)
        count = 0
        
        # Read the rows in the specified range
        for i, row in enumerate(reader):
            if i >= start and i < end:
                #cc_num(0),trans_num(1),amt(2),merchant(3),transaction_date(4),merch_lat(5),merch_long(6),is_fraud(7)
                #3517182278248964,2de6ae16cd114c4d4a7f31f4716b9a07,327.63,fraud_Erdman-Schaden,2022-09-16 18:35:45,34.670239,-84.634454,0
                transaction = {"cc_num": row[0],
                            "trans_num": row[1],
                            "merchant":row[3],
                            "amount":float(row[2]),
                            "transaction_date":row[4],
                            "lat":float(row[5]),
                            "long":float(row[6])}
                #put transaction into 'transactions' map - note the @ for keyAwarePartitioningStrategy
                transaction_map.put(transaction['trans_num']+ '@' + transaction['cc_num'],json.dumps(transaction))
                count = count + 1
                if ((count+1) % print_after_k == 0):
                    print (f'{print_after_k} transactions inserted from chunk starting on row {start} to row {end}')
    
    print (f'all transactions in chunk {start} to {end} loaded ')
    client.shutdown()
    

def process_csv_parallel(filename, num_processes,print_after_k):
    # Get the total number of rows in the CSV file
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        num_rows = sum(1 for row in reader)

    # Calculate the chunk size and number of chunks
    chunk_size = int(num_rows / num_processes)
    chunks = [(i * chunk_size, (i + 1) * chunk_size) for i in range(num_processes)]
    chunks[-1] = (chunks[-1][0], num_rows)

    # Create a pool of worker processes
    pool = multiprocessing.Pool(processes=num_processes)

    # Read the CSV file in parallel    
    for start, end in chunks:
        pool.apply_async(process_csv_chunk, args=(filename, start, end, print_after_k,))

    # Wait for all the worker processes to finish
    pool.close()
    pool.join()

if __name__ == "__main__":

    transaction_data_file = sys.argv[1] if len(sys.argv) >= 2 else './data/test.csv'
    number_parallel_processes = int(sys.argv[2]) if len(sys.argv) >= 3 else 4
    print_after_k = int(sys.argv[3]) if len(sys.argv) >= 4 else 100


    #load transactions in parallel (with number_parallel_processes)
    st = time.time()
    process_csv_parallel(transaction_data_file, number_parallel_processes, print_after_k)
    et = time.time()

    #measure elapsed time
    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')

     #Check how many transactions were loaded
    cluster_members = os.environ.get('HZ_ENDPOINT', '127.0.0.1')
    client = hazelcast.HazelcastClient(cluster_members=[cluster_members],use_public_ip=True)

    
    transaction_map = client.get_map("transactions").blocking()
    print(str(transaction_map.size()) + ' transactions loaded')
    client.shutdown()
    
