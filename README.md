
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

## OutlierDetectionWithMRFFT

This code implements an outlier detection algorithm using Apache Spark. It leverages a MapReduce-based Farthest First Traversal (MRFFT) approach to identify outliers in a set of points. The method involves the following steps:

1. **MRFFT (MapReduce-based Farthest First Traversal)**: A multi-round Farthest First Traversal-based method to approximate the centers of clusters.
2. **MRApproxOutliers**: Uses the results from MRFFT to approximate outliers in the dataset.

### Features
- Uses MRFFT for efficient center approximation in large datasets.
- Handles large datasets efficiently with Spark's distributed computing capabilities.

### Usage
Run the script with the following command:
```bash
python OutlierDetectionWithMRFFT.py <file_name> <M> <K> <L>
```
Where:

- file_name: Path to the input file containing the points.
- M: Maximum number of neighbors a point can have to be considered an outlier.
- K: Number of centers to be computed by MRFFT.
- L: Number of partitions for the input data.

## FrequentItemsDetection

This code implements two sampling algorithms using Apache Spark Streaming to detect frequent items in a data stream. The implemented methods are:

1. **Reservoir Sampling**: Maintains a representative sample of the data stream.
2. **Sticky Sampling**: Estimates the frequency of items in the stream using a probabilistic approach.

### Features
- Utilizes Apache Spark Streaming for real-time data processing.
- Implements Reservoir Sampling and Sticky Sampling algorithms.
- Configurable input parameters for flexibility in different streaming environments.

### Usage
To execute the script, use the following command:
```bash
python FrequentItemsDetection.py <n> <phi> <epsilon> <delta> <hostname> <port>
```
Where:

- n: Total number of items expected in the data stream.
- phi: Threshold fraction for identifying frequent items.
- epsilon: Precision parameter for Sticky Sampling.
- delta: Probability parameter for Sticky Sampling.
- hostname: Hostname or IP address where the streaming source is available.
- port: Port number where the data stream is accessible.


