import sys
import pandas as pd
def process_chunk(chunk, output_file, chunk_no):
    target_output_file = output_file + '_' + "{}".format(chunk_no)
    # Example: Append each processed chunk to another CSV file
    chunk.to_csv(target_output_file, mode='a', header=False, index=False)

# Example usage

output_file = 'processed_large_file.csv'
chunksize = 10**6
chunk_no = 1
for chunk in pd.read_csv('D:\CDAC\Final Project\obis.csv', chunksize=chunksize):
    print("processing chunk no = ", chunk_no,flush=True)
    sys.stdout.flush()
    process_chunk(chunk, output_file, chunk_no)
    chunk_no+=1