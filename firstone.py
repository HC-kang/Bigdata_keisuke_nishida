import re
import pandas as pd
import os
os.chdir('/Users/heechankang/projects/pythonworkspace/bigdata')



pattern = re.compile('^\S+ \S+ \S+  \[(.*)\] "(.*)" (\S+) (\S+)$')

def parse_access_log(path):
    for line in open(path, encoding='utf-8'):
        for m in pattern.finditer(line):
            yield m.groups()


columns = ['time', 'request', 'status', 'bytes']
pd.DataFrame(parse_access_log('NASA_access_log_Aug95'), columns=columns)



df = pd.DataFrame(parse_access_log('access.log'), columns = columns)

df.time = pd.to_datetime(df.time, format = '%d%b%Y:%X', exact = False)

df.head(2)



df.to_csv('access_log.csv', index = False)
# !head -3 access_log.csv