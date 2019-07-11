# To run this on your local machine, you need to first run a Netcat server
#     `$ nc -lk 9999`
#  and then run the example
#     `$ bin/spark-submit examples/src/main/python/streaming/network_wordcount.py localhost 9999`
#
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    ssc = StreamingContext(sc, 1)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (len(word), word))\
                  .reduceByKey(lambda a, b: a + ',' +b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()