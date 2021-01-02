import json
import os
from crontab import CronTab
from datetime import datetime
import time

def is_json(data):
	try:
		json_object = json.loads(data)
	except ValueError as e:
		return False
	return True

def remove_whitespace(data):
	return data.replace('\n', '').replace('\t','').replace(' ','')

def writeLog(action, key):
	logf = open('./logs.txt', 'a+')
	logf.write('{} {} on {}\n'.format(action, key, str(datetime.now())))
	logf.close()


class KVDB:
	cron = CronTab(user='whoisvikas')

	def __init__(self, file_path='./data.txt'):
		self.file_path = file_path
		self.f = open(file_path, 'a+')
		self._addMeta()
		self._setInUse(0)

	@staticmethod
	def showCron():
		for job in KVDB.cron:
			print(job)

	# key is 32 bits, val is valid json_data
	def create(self, key, val, ttl=-1):
		if len(key) > 32:
			print("Invalid Key")
			return "Invalid Key"
		if is_json(val):
			val = remove_whitespace(val)
		else:
			print("Invalid Json data")
			return "Invalid Json data"

		retry = True
		while retry:
			inUse = self._getInUse()
			# print(inUse)

			if inUse == 0:
				self._setInUse(1)
				
				if self._keyExists(key):
					print('CreateError: Key already exists!')
					self._setInUse(0)
					return 'CreateError: Key already exists!'
				else:
					# write to file
					self.f.seek(2)
					self.f.write('\n{}::{}'.format(key, val))
					self._saveFile()
					writeLog('created', key)
					
					# set a cron to delete file entry
					if ttl != -1:
						now = datetime.now()
						mins = ttl%60
						hrs = int(ttl/60)
						days = int(hrs/24)
						hrs = hrs%24
						job = KVDB.cron.new(command='python3 cron_delete_record.py {} {}'.format(self.file_path, key), comment=key)
						job.minute.on(now.minute+mins)
						job.hour.on(now.hour+hrs)
						job.day.on(now.day+days)
						job.month.on(now.month)
						KVDB.cron.write()

				retry = False
				self._setInUse(0)

			print('waiting...')
			time.sleep(0.1)


	def read(self, key):
		retry = True
		while retry:
			inUse = self._getInUse()

			if inUse == 0:
				self._setInUse(1)
				if not self._keyExists(key):
					print('ReadError: Key does not exists!')
					self._setInUse(0)
					return -1
				else:
					writeLog('read', key)
					self._setInUse(0)
					return self._getValue(key)
				retry = False

			print('waiting...')
			time.sleep(0.1)


	def delete(self, key):
		retry = True
		while retry:
			inUse = self._getInUse()

			if inUse == 0:
				self._setInUse(1)
				if not self._keyExists(key):
					print('DeleteError: Key does not exists!')
					self._setInUse(0)
					return 'DeleteError: Key does not exists!'
				else:
					# delete line from file
					self.f.seek(0)
					lines = self.f.readlines()
					self.f.seek(0)
					self.f.truncate()

					deleted = False
					for line in lines:
						tokens = line.split('::')
						if tokens[0] == key:
							deleted = True
							continue
						self.f.write(line)

					self._saveFile()
					self._setInUse(0)
					if deleted:
						writeLog('deleted', key)
						return 1
					else:
						return 0

				retry = False
				self._setInUse(0)

			print('waiting...')
			time.sleep(0.1)


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
			self.f.truncate()
			if len(lines) > 1:
				self.f.write('inUse={}\n'.format(use))
			else:
				self.f.write('inUse={}'.format(use))
			self.f.writelines(lines[1:])
			self._saveFile()

	def _getInUse(self):
		self.f.seek(0)
		lines = self.f.readlines()
		if len(lines)==0:
			self.f.write('inUse=0')
			self._saveFile()
			return 0
		else:
			self.f.seek(0)
			return int(lines[0].split('=')[1])

	def _saveFile(self):
		self.f.flush()
		os.fsync(self.f.fileno())



if __name__ == '__main__':
	print("========================================================")
	print("             KEY-VALUE STORE")
	print("========================================================")

	file_path = input('Enter File Path: ')
	db = KVDB(file_path)
	
	print('\ncommands: \n# create <key> <json_object> <ttl in minutes (optional)>')
	print('# read <key>')
	print('# delete <key>')
	print('# show_cron')
	print('# exit')

	while(True):
		command = input('\ncommand > ')
		
		if command == 'create':
			key = input('key (<= 32chars) : ')
			if len(key) > 32:
				print('KeyError: Key is invalid')
				continue

			val = input('value (json_object) : ')
			if not is_json(val):
				print('ValueError: json_data is invalid')
				continue
			
			ttl = int(input('TTL (in minutes) | type -1 to skip : '))
			db.create(key, remove_whitespace(val), ttl)

		elif command == 'read':
			key = input('key (<= 32chars) : ')
			if len(key) > 32:
				print('KeyError: Key is invalid')
				continue
			res = db.read(key)
			if (res == -1):
				print('record with key ({}) doesnot exist'.format(key))
			else:
				print('Value of {} = {}'.format(key, res))

		elif command == 'delete':
			key = input('key (<= 32chars) : ')
			if len(key) > 32:
				print('KeyError: Key is invalid')
				continue
			if db.delete(key) == 1:
				print('record with key ({}) deleted'.format(key))
			else:
				print('Error in Deletion')

		elif command == 'show_cron':
			KVDB.showCron()

		elif command == 'exit':
			break

		else:
			print(' ! Command/Command-Format Invalid.')


	print("=========================== EXIT =============================")