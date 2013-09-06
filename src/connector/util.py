import traceback, os, sys, logging
from datetime import datetime
from datetime import timedelta

###################################################################################################
###################################################################################################
#
# class:	DBUtil
# purpose:	Helper class that contains database helper methods
#
# author:	Jason Sardano
# date:		Aug 10, 2013
#
###################################################################################################
class DBUtil(object):
	def __init__(self):
		return
		
	def getColumns(self, cursor):
		cols = cursor.description;
		columns = dict()
		for i in xrange(len(cols)):
			field_name = cols[i][0];
			columns[field_name] = i
		return columns
		
	def close(self, obj):
		if obj is not None:
			try:
				obj.close()
			except:
				None
				"""tb = sys.exc_info()[2]
				tbinfo = traceback.format_tb(tb)[0]
				msg = "Error in close:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
				logging.error(msg);"""

###################################################################################################
###################################################################################################
#
# class:	DateUtil
# purpose:	Helper class that contains Date utility methods
#
# author:	Jason Sardano
# date:		Aug 10, 2013
#
###################################################################################################
class DateUtil(object):
	def __init__(self):
		return
		
	def ts(self, prefix = ""):
		return prefix + str(int(time.mktime(datetime.now().timetuple())))
		
	def now(self):
		return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	
	def tomorrow(self):
		t = datetime.now() + timedelta(days=1)
		return t.strftime('%Y-%m-%d %H:%M:%S')

###################################################################################################
###################################################################################################
#
# class:	Config
# purpose:	Helper class to load config settings from a file
#
# author:	Jason Sardano
# date:		Aug 10, 2013
#
###################################################################################################

class Config(object):
	def __init__(self, path):
		self._data = {}
		try:
			f = open(path)
			lines = f.readlines()
			self._readProps(lines)
			close(f)
		except:
			None
		return
		
	def __str__(self):
		s='{'
		for key,value in self._data.items():
			s = ''.join((s,key,'=',value,', '))
		s=''.join((s[:-2],'}'))
		return s

	def _readProps(self, lines):
		for line in lines:
			line = line.strip()
			if not line or line.find('#') == 0:
				continue
			n = line.find('=')
			if(n == -1):
				self._data[line] = ''
				continue
				
			key = line[0:(n)]
			val = line[n + 1:]
			self._data[key] = val
		return
		
	def __getitem__(self, name):
		return self._data.get(name,'')

	def __setitem__(self, name, value):
		self._data[name] = value
		return
			
	def hasValues(self, keys):
		n = 0
		for key in keys:
			if self.hasKey(key) == False:
				logging.debug('Missing ' + key)
				n = n + 1
			elif not self._data[key]:
				logging.debug(key + ' missing value')
				n = n + 1
		return n==0
		
	def hasKey(self, key):
		return key in self._data

###################################################################################################
###################################################################################################
#
# class:	LockFile
# purpose:	Helper class to determine if import process is running
#
# author:	Jason Sardano
# date:		Aug 20, 2013
#
###################################################################################################

class LockFile(object):
	def __init__(self, path):
		self._path = path
		
	def locked(self):
		return os.path.exists(self._path)
		
	def lock(self):
		try:
			with open(self._path, 'w') as f:
				logging.info("Writing lock file.")
				dateutil = DateUtil()
				f.write(dateutil.now())
		except Exception as e:
			logging.error('Error writing lock file')
			logging.exception(e)
		return
		
	def unlock(self):
		try:
			if os.path.exists(self._path):
				logging.debug('Removing lock file')
				os.remove(self._path)
		except Exception as e:
			logging.error('Error removing lock file')
			logging.exception(e)
		return
