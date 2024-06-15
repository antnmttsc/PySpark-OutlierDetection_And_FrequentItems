from pyspark import SparkContext, SparkConf
import sys
import os
import time
import math
import numpy as np

################################################################################
############################### EXACT OUTLIERS #################################
################################################################################

def ExactOutliers(ListOfPoints, D, M, K):

  point_dist_count_dict = [1] * tot_num_point  # from the delivery: |B_{S}(p, D)| is always at least 1
  outlier_list = []

  for i in range(tot_num_point):
    point_i = ListOfPoints[i]

    for j in range(i+1, tot_num_point):

      if math.dist(point_i, ListOfPoints[j]) <= D:# euclidean distance
          point_dist_count_dict[i] += 1           # increment this value when point i has distance from j less than D
          point_dist_count_dict[j] += 1           # do the same for j (distance from i is less than D) --> "symmetry of distance"

    # if the number of exemples whose distance from i is less or equal than M then i is an outlier --> add i to the outlier's list
    if point_dist_count_dict[i] <= M:
        outlier_list.append((point_i, point_dist_count_dict[i]))

  outlier_list.sort(key = lambda x: x[1], reverse = False)

  print("Number of Outliers = {}".format(len(outlier_list)))
  for point in outlier_list[:K]:
    print("Point: ({},{})".format(point[0][0], point[0][1]))

################################################################################
############################## APPROX OUTLIERS #################################
################################################################################

# COMPUTE THE CELL CORDINATES FOR A POINT
def map_to_cell(point):
  i, j = math.floor(point[0] / Lambda), math.floor(point[1] / Lambda)
  return (i, j), 1 # Alt1: [((i,j), 1)]

# MAP THE POINTS TO THEIR RESPECTIVE CELL AND COUNT THE NUMBER OF POINT IN EACH CELL
def points_to_cells_rdd(points_rdd):

  # Alt1: cells_rdd = points_rdd.flatMap(map_to_cell).reduceByKey(lambda x, y: x + y)
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
def get_sure_uncertain_outliers(cells_dict_N3_N7, M, K):
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

def new_get_first_K_cells(cells_rdd, K):
  first_K_cells = cells_rdd.map(lambda x: (x[1], x[0])).sortByKey().take(K)
  return first_K_cells

def MRApproxOutliers(points_rdd, D, M, K):

  global Lambda
  Lambda = D / (2*math.sqrt(2))

  # convert the rdd of points into an rdd of cells where each (key, value) pair is like (cordinates, number of element in the cell)
  cells_rdd = points_to_cells_rdd(points_rdd).cache()

  # convert cells_rdd into a dictionary where keys are cell coordinates and values represent the number of points in each cell
  cells_dict = cells_rdd.collectAsMap()

  # create a dictionary with keys representing cell coordinates and values as tuples (N3_i, N7_i)
  cells_dict_N3_N7 = {cell: compute_N3_N7(cell, cells_dict) for cell in cells_dict}

  numb_surely_outliers, numb_uncertain_outliers = get_sure_uncertain_outliers(cells_dict_N3_N7, M, K)

  first_K_cells = new_get_first_K_cells(cells_rdd, K)

  # printing
  print("Number of sure outliers =", numb_surely_outliers)
  print("Number of uncertain points = ", numb_uncertain_outliers)

  for cell in first_K_cells:
    print("Cell: ({},{})  Size = {}".format(cell[1][0],cell[1][1], cell[0]))

################################################################################
##################################### MAIN #####################################
################################################################################

def main():
  # CHECKING NUMBER OF CMD LINE PARAMETERS
  assert len(sys.argv) == 6, "Usage: python G061HW1.py <D> <M> <K> <L> <file_name>"

  # SPARK SETUP
  conf = SparkConf().setAppName('G061HW1')
  sc = SparkContext(conf=conf)

  # INPUT READING

  # 1. Read number of partitions
  L = sys.argv[4]
  assert L.isdigit(), "L must be an integer"
  L = int(L)
  assert L > 0, "K must be grater than zero"

  # 2. SETTING GLOBAL VARIABLES
  D = sys.argv[1]
  assert D.replace('.', '', 1).isdigit(), "D must be a float"
  D = float(D)
  assert D > 0, "D must be grater than zero"

  M = sys.argv[2]
  assert M.isdigit(), "M must be an integer"
  M = int(M)
  assert M > 0, "M must be grater than zero"

  K = sys.argv[3]
  assert K.isdigit(), "K must be an integer"
  K = int(K)
  assert K > 0, "K must be grater than zero"

  # 3. Read input file and subdivide it into L partitions
  data_path = sys.argv[5]
  assert os.path.isfile(data_path), "File or folder not found"

  # Read input file and convert each line to a tuple of floats separated by commas
  rawData = sc.textFile(data_path).repartition(numPartitions = L)
  inputPoints = rawData.map(lambda line: tuple(map(float, line.split(","))))

  global tot_num_point
  tot_num_point = inputPoints.count()

  print("{} D={} M={} K={} L={}".format(data_path, D, M, K, L))
  print("Number of points = {}".format(tot_num_point))

################################################################################

  if tot_num_point <= 200000:
    listOfPoints = inputPoints.collect() # Downloads the points into a list called listOfPoints

    t_start = time.perf_counter()
    ExactOutliers(listOfPoints, D, M, K)
    t_end = time.perf_counter()

    time_ExactOutliers = round((t_end - t_start)*1000)
    print("Running time of ExactOutliers = {} ms".format(time_ExactOutliers))

################################################################################

  t_start = time.perf_counter()
  MRApproxOutliers(inputPoints, D, M, K)
  t_end = time.perf_counter()

  time_MRApproxOutliers = round((t_end - t_start)*1000)
  print("Running time of MRApproxOutliers = {} ms".format(time_MRApproxOutliers))


  #Stop SparkContext
  sc.stop()

if __name__ == "__main__":
  main()
