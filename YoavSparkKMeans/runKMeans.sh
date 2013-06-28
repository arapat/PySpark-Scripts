export PYTHONPATH="/oasis/scratch/csd181/hadoop/spark-0.7.2/python":$PYTHONPATH

# Generating data
# Usage: python entropy_kmeans_data_generator.py <dimension> <data_size> <random_seed>
python entropy_kmeans_data_generator.py 3 50 10 > data.txt

# Run script
# Usage: python YoavSparkKMeans.py <master> <file> <k> <convergeDist>
# Master should be "mesos://ion-21-14.ibnet0:5050" in order to compute in parallel, "local" to compute locally
python YoavSparkKMeans.py local data.txt 4 0.01 
# Use the following commandline to filter out logs 
# python YoavSparkKMeans.py local data.txt 4 0.01 2> /dev/null
