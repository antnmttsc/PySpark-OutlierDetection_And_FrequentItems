# import libraries
from pyspark import SparkContext, SparkConf, StorageLevel
import sys
import os
import time
import math
import numpy as np
import random

################################################################################
############################ FROM PREVIOUS CODE ################################
################################################################################
# COMPUTE THE CELL COORDINATES FOR A POINT
def map_to_cell(point):

  i, j = math.floor(point[0] / Lambda), math.floor(point[1] / Lambda)

  return (i, j), 1

# MAP THE POINTS TO THEIR RESPECTIVE CELL AND COUNT THE NUMBER OF POINT IN EACH CELL
def points_to_cells_rdd(points_rdd):

  cells_rdd = points_rdd.map(lambda x: map_to_cell(x)).reduceByKey(lambda x, y: x + y)

  return cells_rdd

# COMPUTE THE N3 AND N7 VALUES FOR A CELL
def compute_N3_N7(cell, dict_cells):

  i, j = cell
  grid = np.zeros((7, 7))

  for dx in range(-3, 4):
    x = i + dx
    for dy in range(-3, 4):
      y = j + dy
      grid[dx + 3, dy + 3] += dict_cells.get((x, y), 0)

  N3 = np.sum(grid[2:5, 2:5])
  N7 = np.sum(grid)

  return N3, N7, dict_cells[cell]

# FILTER THE DICTIONARY OF CELLS SELECTING ONLY THE CELLS IDENTIFYED AS OUTLIERS OR UNCERTAIN OUTLIERS
def get_sure_uncertain_outliers(cells_dict_N3_N7, M):

  numb_surely_outliers = 0
  numb_uncertain_outliers = 0

  for cell in cells_dict_N3_N7.keys():

    N3 = cells_dict_N3_N7[cell][0]
    N7 = cells_dict_N3_N7[cell][1]
    if N3 > M:
      continue
    # NOTE: can't be N3>M and N7<M bc N3 is included in N7
    elif N7 <= M:
      numb_surely_outliers += cells_dict_N3_N7[cell][2]

    elif N3 <= M and N7 > M:
      numb_uncertain_outliers += cells_dict_N3_N7[cell][2]

  return numb_surely_outliers, numb_uncertain_outliers

def MRApproxOutliers(points_rdd, D, M):

  global Lambda
  Lambda = D / (2*math.sqrt(2))

  # convert the rdd of points into an rdd of cells where each (key, value) pair is like (coordinates, number of element in the cell)
  cells_rdd = points_to_cells_rdd(points_rdd).cache()

  # convert cells_rdd into a dictionary where keys are cells coordinates and values represent the number of points in each cell
  cells_dict = cells_rdd.collectAsMap()

  # create a dictionary with keys representing cell_i coordinates and values as tuples (N3_i, N7_i)
  cells_dict_N3_N7 = {cell: compute_N3_N7(cell, cells_dict) for cell in cells_dict}

  numb_surely_outliers, numb_uncertain_outliers = get_sure_uncertain_outliers(cells_dict_N3_N7, M)

  # printing
  print("Number of sure outliers =", numb_surely_outliers)
  print("Number of uncertain points = ", numb_uncertain_outliers)

################################################################################
################################ COMPUTE FFT ###################################
################################################################################

# COMPUTE FFT
def SequentialFFT(P, K):

  N = len(P)
  # select at random a point in P
  S = [P[random.randint(0, N-1)]]  # add the first center to the set of centers
  dists_centers = [float('inf')] * N # initialize all distances equal to inf

  for idx in range(K-1):

    max_dist = 0
    next_center_idx = None
    point_c_new = S[-1]

    for i in range(N):

      new_dist = math.dist(point_c_new, P[i]) # when i = c_new then new_dist = 0

      if new_dist < dists_centers[i]:
        dists_centers[i] = new_dist

      if dists_centers[i] > max_dist:
        max_dist = dists_centers[i]
        next_center_idx = i # take the index of the farthest point from the centers

    S.append(P[next_center_idx]) # add the index to the list of centers indexes


  return S # return the list of centers (this is my T_i)

################################################################################
############################### COMPUTE MRFFT ##################################
################################################################################
def MRFFT(P, K): # P is a RDD, K is an integer

  #------------- ROUND 1 --------------------------

  t_start1 = time.perf_counter()
  coresetPoints_rdd = P.mapPartitions(lambda x: SequentialFFT(list(x), K)).persist(StorageLevel.MEMORY_AND_DISK)
  coresetPoints_count = coresetPoints_rdd.count()  # triggering the persist
  t_end1 = time.perf_counter()

  t_r1 = round((t_end1 - t_start1) * 1000)

  print("Running time of MRFFT Round 1 = {} ms".format(t_r1))

  #------------- ROUND 2 --------------------------

  t_start2 = time.perf_counter()

  coresetPoints = coresetPoints_rdd.collect()
  C = SequentialFFT(coresetPoints, K)

  t_end2 = time.perf_counter()

  t_r2 = round((t_end2 - t_start2)*1000)
  print("Running time of MRFFT Round 2 = {} ms".format(t_r2))

  #------------- ROUND 3 --------------------------

  center_broadcast = sc.broadcast(C).value # save centers in a local data structure

  t_start3 = time.perf_counter()
  rad = float(P.map(lambda x: min(math.dist(x, center) for center in center_broadcast)).reduce(max))
  t_end3 = time.perf_counter()

  t_r3 = round((t_end3 - t_start3) * 1000)

  print("Running time of MRFFT Round 3 = {} ms".format(t_r3))

  print("Radius = {:.8f}".format(rad))

  return rad

################################################################################
##################################### MAIN #####################################
################################################################################

def main():
  # CHECKING NUMBER OF CMD LINE PARAMETERS
  assert len(sys.argv) == 5, "Usage: python G061HW2.py <file_name> <M> <K> <L>"

  # SPARK SETUP
  conf = SparkConf().setAppName('G061HW2')
  conf.set("spark.locality.wait",  "0s")

  global sc
  sc = SparkContext(conf=conf)

  # INPUT READING

  # 1. Read input file and subdivide it into L partitions
  data_path = sys.argv[1]
  # assert os.path.isfile(data_path), "File or folder not found" # remove bc doesn't work in CloudVeneto

  # 2. Setting global variables
  M = sys.argv[2]
  assert M.isdigit(), "M must be an integer"
  M = int(M)
  assert M > 0, "M must be greater than zero"

  K = sys.argv[3]
  assert K.isdigit(), "K must be an integer"
  K = int(K)
  assert K > 0, "K must be greater than zero"

  # 3. Read number of partitions
  global L
  L = sys.argv[4]
  assert L.isdigit(), "L must be an integer"
  L = int(L)
  assert L > 0, "L must be greater than zero"

  # convert each line to a tuple of floats separated by commas
  rawData = sc.textFile(data_path).repartition(numPartitions = L)
  inputPoints = rawData.map(lambda line: tuple(map(float, line.split(",")))).cache()

################################################################################

  tot_num_point = inputPoints.count()

  print("{} M={} K={} L={}".format(data_path, M, K, L))
  print("Number of points = {}".format(tot_num_point))

################################################################################

  D = MRFFT(inputPoints, K) # compute the radius and store in variable D

################################################################################

  t_start_MRApprox = time.perf_counter()
  MRApproxOutliers(inputPoints, D, M)
  t_end_MRApprox = time.perf_counter()

  t_MRApprox = round((t_end_MRApprox - t_start_MRApprox)*1000)

  print("Running time of MRApproxOutliers = {} ms".format(t_MRApprox))

################################################################################

  #Stop SparkContext
  sc.stop()

if __name__ == "__main__":
  main()
