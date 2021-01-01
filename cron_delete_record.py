import sys
from crontab import CronTab

def cron_delete_record(file_path, key):
	f = open(file_path, 'a+')
	retry = True
	while retry:
		inUse = getInUse()

		if inUse == '0':
			setInUse(f, 1)
			if not keyExists(f, key):
				print('DeleteError: Key does not exists!')
				return 'DeleteError: Key does not exists!'
			else:
				# delete line from file
				f.seek(0)
				lines = f.readlines()
				f.seek(0)
				f.truncate()

				for line in lines:
					tokens = line.split('::')
					if tokens[0] == key:
						continue
					f.write(line)
				saveFile(f)
				writeLog(f, 'deleted', key)
			retry = False
			setInUse(f, 0)


def keyExists(f, key):
		f.seek(0)
		line = f.readline()
		while line:
			tokens = line.split('::')
			if tokens[0] == key:
				return True
			line = f.readline()
		return False

def setInUse(f, use):
	f.seek(0)
	lines = f.readlines()
	if len(lines)==0:
		f.seek(0)
		f.write('inUse={}'.format(use))
		_saveFile()
	else:
		f.seek(0)
		f.write('inUse={}'.format(use))
		f.writelines(lines[1:])
		_saveFile()

def getInUse(f):
	f.seek(0)
	lines = f.readlines()
	if len(lines)==0:
		f.seek(0)
		f.write('inUse=0')
		_saveFile()
		return '0'
	else:
		f.seek(0)
		return lines[0].split('=')[1]

def saveFile(f):
	f.flush()
	os.fsync(f.fileno())


def writeLog(action, key):
	logf = open('./log.txt', 'a+')
	logf.write('action {} on {}\n'.format(key, str(datetime.now())))
	logf.close()



file_path = sys.argv[1]
key = sys.argv[2]
cron_delete_record(file_path, key)