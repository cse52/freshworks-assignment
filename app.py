import json
import os
from crontab import CronTab
from datetime import datetime

def is_json(data):
	try:
		json_object = json.loads(data)
	except ValueError as e:
		return False
	return True

def remove_whitespace(data):
	return data.replace('\n', '').replace('\t','').replace(' ','')

class KVDB:
	def __init__(self, file_path='./data.txt'):
		self.file_path = file_path
		self.f = open(file_path, 'a+')
		self._addMeta()
		self.cron = CronTab()

	# key is 32 bits, val is valid json_data
	def create(self, key, val, ttl=None):
		if len(key) > 32:
			return "Invalid Key"
		if is_json(val):
			val = remove_whitespace(val)
		else:
			return "Invalid Json data"

		retry = True
		while retry:
			inUse = self._getInUse()
			if inUse == '0':
				self._setInUse(1)
				if self._keyExists(key):
					print('CreateError: Key already exists!')
					return 'CreateError: Key already exists!'
				else:
					# write to file
					self.f.seek(2)
					self.f.write('\n{}::{}'.format(key, val))
					self._saveFile()
					self._writeLog('created', key)
					
					# set a cron to delete file entry
					if ttl:
						now = datetime.now()
						hrs = int(ttl/60)
						mins = ttl%60
						job = cron.new(command='{} {} {} {} * python3 cron_delete_record.py {} {}'.format(mins, hrs, now.day, now.month, self.file_path, key), comment='delete {}'.format(key))

				retry = False
				self._setInUse(0)


	def read(self, key):
		retry = True
		while retry:
			inUse = self._getInUse()
			if inUse == '0':
				self._setInUse(1)
				if not self._keyExists(key):
					print('ReadError: Key does not exists!')
					return 'ReadError: Key does not exists!'
				else:
					self._writeLog('read', key)
					return self._getValue(key)
				retry = False
				self._setInUse(0)


	def delete(self, key):
		retry = True
		while retry:
			inUse = self._getInUse()

			if inUse == '0':
				self._setInUse(1)
				if not self._keyExists(key):
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
					self._saveFile()
					self._writeLog('deleted', key)
				retry = False
				self._setInUse(0)


	def _keyExists(self, key):
		self.f.seek(0)
		line = self.f.readline()
		while line:
			tokens = line.split('::')
			if tokens[0] == key:
				return True
			line = self.f.readline()
		return False

	def _getValue(self, key):
		self.f.seek(0)
		line = self.f.readline()
		while line:
			tokens = line.split('::')
			if tokens[0] == key:
				return tokens[1]
			line = self.f.readline()
		return None

	def _addMeta(self):
		self.f.seek(0)
		lines = self.f.readlines()
		if len(lines)==0:
			self.f.seek(0)
			self.f.write('inUse=0')
			self._saveFile()
		elif 'inUse=0' in lines[0] or 'inUse=1' in lines[0]:
			return
		else:
			self.f.seek(0)
			self.f.write('inUse=0')
			self.f.writelines(lines)
			self._saveFile()

	def _setInUse(self, use):
		self.f.seek(0)
		lines = self.f.readlines()
		if len(lines)==0:
			self.f.seek(0)
			self.f.write('inUse={}'.format(use))
			self._saveFile()
		else:
			self.f.seek(0)
			self.f.write('inUse={}'.format(use))
			self.f.writelines(lines[1:])
			self._saveFile()

	def _getInUse(self):
		self.f.seek(0)
		lines = self.f.readlines()
		if len(lines)==0:
			self.f.seek(0)
			self.f.write('inUse=0')
			self._saveFile()
			return '0'
		else:
			self.f.seek(0)
			return lines[0].split('=')[1]

	def _saveFile(self):
		self.f.flush()
		os.fsync(self.f.fileno())


	def _writeLog(self, action, key):
		logf = open('./log.txt', 'a+')
		logf.write('action {} on {}\n'.format(key, str(datetime.now())))
		logf.close()
