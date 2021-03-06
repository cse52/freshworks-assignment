import sys
from crontab import CronTab
from app import KVDB
from datetime import datetime

def writeLog(action, key):
	logf = open('./logs.txt', 'a+')
	logf.write('{} {} on {}\n'.format(action, key, str(datetime.now())))
	logf.close()

# extract atrgs
file_path = sys.argv[1]
key = sys.argv[2]

# create key-value store object
db = KVDB(file_path)
db.delete(key)