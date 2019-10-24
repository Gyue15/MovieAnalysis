from hdfs import InsecureClient
import os
client = InsecureClient("http://localhost:9870", user='shea')
client.delete("streamInput",True)
client.makedirs("streamInput")
# os.removedirs('file')