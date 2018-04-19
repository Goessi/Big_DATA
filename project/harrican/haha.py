import io
from urllib import request
import csv

response = request.urlopen('https://dq-content.s3.amazonaws.com/251/storm_data.csv')
reader = csv.reader(io.textIOWrapper(response))

for line in reader:
    print(line)
