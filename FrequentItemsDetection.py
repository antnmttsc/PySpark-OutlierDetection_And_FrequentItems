# Import libraries
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import random
import math
import collections

################################################################################
############################## RESERVOIR SAMPLING ##############################
################################################################################

def update_reservoir_sample(stream, reservoir_sample, m, t):

    for i, item in enumerate(stream): #* for each x_t in \Sigma do

        if len(reservoir_sample) < m: #* if t<=m then
            reservoir_sample.append(item) #* add x_t to S1
        else: #* else

          if random.random() <= m/(t+i+1): #* with probability m/t do the following{

            j = random.randint(0, m - 1) #* evict a random element x from S1
            reservoir_sample[j] = item #* add x_t to S1}

    return reservoir_sample

################################################################################
############################### STICKY SAMPLING ################################
################################################################################

def update_sticky_sample(stream, sticky_sample, sampling_rate):

  for x_t in stream: #**  for each x_t in \Sigma do

    if x_t in sticky_sample: #** If x_t is in S2, increment f_e(x_t) by 1
      sticky_sample[x_t] += 1

    else:
      if random.random() <= sampling_rate: #** Add x_t to S2 with probability r/n
        sticky_sample[x_t] = 1

  return sticky_sample

################################################################################
############################# HOW TO PROCESS BATCH #############################
################################################################################

def process_batch(time, batch):
    global streamLength, reservoir_sample, sticky_sample, true_frequent_items

    # Check if we have reached or exceeded the required number of items
    if streamLength[0] >= n:
      return

    batch_data = batch.collect()
    batch_size = len(batch_data) # batch.count()

    # Update stream length
    if streamLength[0] + batch_size >= n:
      max_val = n - streamLength[0]
      batch_data = batch_data[:max_val]  # batch to reach n items
      batch_size = len(batch_data)
      streamLength[0] = n
    else:
      streamLength[0] += batch_size

    # True frequent items
    true_frequent_items.update(batch_data)

    # Reservoir sampling
    update_reservoir_sample(batch_data, reservoir_sample, m, streamLength[0]-batch_size)

    # Sticky sampling
    update_sticky_sample(batch_data, sticky_sample, sampling_rate)

    if streamLength[0] >= n:
      stopping_condition.set()

if __name__ == "__main__":

  # 1) Spark Context Setup
  conf = SparkConf().setMaster("local[*]").setAppName("G061HW3")
  sc = SparkContext(conf=conf)
  ssc = StreamingContext(sc, 0.01) # batch interval of 0.01 seconds.
  ssc.sparkContext.setLogLevel("ERROR")

  # 2) Reading Input Parameters
  assert len(sys.argv) == 7, "USAGE: <n> <phi> <epsilon> <delta> <hostname> <portExp>"

  n = int(sys.argv[1])
  phi = float(sys.argv[2])
  epsilon = float(sys.argv[3])
  delta = float(sys.argv[4])
  hostname = sys.argv[5]
  portExp = int(sys.argv[6])
  stopping_condition = threading.Event()

  # Initialize data structures
  reservoir_sample = [] #* S1<-Empty
  sticky_sample = {} #** S2<-empty Hash Table
  true_frequent_items = collections.Counter()

  # Define useful variables
  m = math.ceil(1 / phi) # s.t. e.g. 14.2-->15
  r = math.log(1 / (delta * phi)) / epsilon # ln(1/(delta*phi))/eps
  sampling_rate = r/n #** sampling rate = r/n
  threshold = (phi - epsilon) * n
  streamLength = [0]

  # Create DStream
  DStream = ssc.socketTextStream(hostname, portExp, StorageLevel.MEMORY_AND_DISK)
  DStream.foreachRDD(lambda time, batch: process_batch(time, batch))

  # Start the streaming context
  ssc.start()
  stopping_condition.wait()
  ssc.stop(stopSparkContext=False, stopGraceFully=True)

  # COMPUTE AND PRINT FINAL STATISTICS
  print("INPUT PROPERTIES")
  print(f"n = {n} phi = {phi} epsilon = {epsilon} delta = {delta} port = {portExp}")

  ## EXACT
  print(f"EXACT ALGORITHM")
  print(f"Number of items in the data structure = {len(true_frequent_items)}")
  true_frequent_items = [item for item, count in true_frequent_items.items() if count >= phi * n]
  true_frequent_items = sorted(true_frequent_items, key=lambda x: int(x))
  print(f"Number of true frequent items = {len(true_frequent_items)}")
  print("True frequent items:")
  for item in true_frequent_items:
    print(f"{item}")

  ## RESERVOIR
  print(f"RESERVOIR SAMPLING")
  print(f"Size m of the sample = {m}")
  reservoir_sample = sorted(set(reservoir_sample), key=lambda x: int(x))
  print(f"Number of estimated frequent items = {len(reservoir_sample)}")
  print("Estimated frequent items:")
  for item in reservoir_sample:
    if item in true_frequent_items:
      print(f"{item} +")
    else:
      print(f"{item} -")

  ## STICKY
  print(f"STICKY SAMPLING")
  print(f"Number of items in the Hash Table = {len(sticky_sample)}")
  sticky_sample = [item for item, count in sticky_sample.items() if count >= threshold]
  sticky_sample = sorted(set(sticky_sample), key=lambda x: int(x))
  print(f"Number of estimated frequent items = {len(sticky_sample)}")
  print("Estimated frequent items:")
  for item in sticky_sample:
    if item in true_frequent_items:
      print(f"{item} +")
    else:
      print(f"{item} -")
