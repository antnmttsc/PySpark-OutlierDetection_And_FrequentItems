
## OutlierDetectionSpark

This code implements two algorithms for detecting outliers in a set of points using Apache Spark. The two methods are:

1. **ExactOutliers**: An exact method that computes the number of points within a certain distance for each point to determine outliers.
2. **MRApproxOutliers**: A MapReduce-based approximate method that partitions the space into cells and computes the number of points in each cell and neighboring cells to identify potential outliers.

### Features
- Uses Euclidean distance for outlier detection.
- Handles large datasets efficiently with Spark's distributed computing capabilities.
- Includes both exact and approximate algorithms for flexibility based on dataset size.

### Usage
Run the script with the following command:
```bash
python OutlierDetectionSpark.py <D> <M> <K> <L> <file_name>
```
Where:

- D: Distance threshold for defining neighbors.
- M: Maximum number of neighbors a point can have to be considered an outlier.
- K: Number of top outliers to display.
- L: Number of partitions for the input data.
- file_name: Path to the input file containing the points.
