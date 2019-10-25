from hdfs import InsecureClient
import os
client = InsecureClient("http://localhost:9870", user='gyue')
client.delete("streamInput/area", True)
client.makedirs("streamInput/area")
# os.removedirs('file')