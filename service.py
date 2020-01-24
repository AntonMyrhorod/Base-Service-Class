"""
Module for creating services, transferring requests and responses between them using the base class Service.
"""

from threading import Thread
from queue import Queue, Empty, Full
from collections.abc import Iterable

from log import Log


class Package(object):
	"""
	Container for transfering data between services.
	"""

	def __init__(self,
				 requestServiceName,
				 requestMethod,
				 requestArguments = None,
				 responseServiceName = None,
				 responseMethod = None):
		"""
		Package constructor.
		[in] requestServiceName - Request service name.
		[in] requestMethod - Request method in service.
		[in] requestArguments  - Request arguments for method, can be ignored.
		[in] responseServiceName - Response service name, can be ignored.
		[in] responseMethod - Response method in service, can be ignored.
		"""

		self.requestServiceName = requestServiceName
		self.requestMethod = requestMethod

		if requestArguments == None:
			self.requestArguments = None

		elif isinstance(requestArguments, Iterable) == True: 
			self.requestArguments = requestArguments

		else:
			self.requestArguments = [requestArguments]

		self.responseServiceName = responseServiceName
		self.responseMethod = responseMethod
		self.responseArguments = None


class Service(object):
	"""
	Base service class.
	"""

	# Service.__jobsQueues - Map of services names and jobs queues.
	__jobsQueues = {}


	def __init__(self, name, workerTimeout = 0, numWorkerThreads = 16, jobQueueMaxSize = 100):
		"""
		Service constructor.
		[in] name - Service name.
		[in] workerTimeout - Service worker timeout..
		[in] numWorkerThreads - Count of service worker threads.
		[in] jobQueueMaxSize - Service job queue max size.
		"""

		Log.debug(name)

		self.name = name
		self.__run = False
		self.__workerThreads = []
		self.__workerTimeout = workerTimeout
		self.__jobs = Queue(maxsize = jobQueueMaxSize)

		try:
			for _ in range(numWorkerThreads):
				workerThread = Thread(target = self.__worker)
				self.__workerThreads.append(workerThread)
		except Exception as e:
			Log.error(str(e))


	def __del__(self):
		"""
		Service destructor. Joins worker threads.
		"""

		Log.debug(self.name)

		if self.__run == True:
			self.stop()


	def start(self):
		"""
		Starts service. Runs worker threads.
		"""

		try:
			if self.__run == False:
				Log.debug(self.name)

				self.__run = True
				Service.__jobsQueues[self.name] = self.__jobs

				for workerThread in self.__workerThreads:
					workerThread.start()
		except Exception as e:
			Log.error(str(e))


	def stop(self):
		"""
		Stops Service. Joins worker threads.
		"""
		
		try:
			if self.__run == True:
				Log.debug(self.name)

				self.__run = False
				del Service.__jobsQueues[self.name]

				for workerThread in self.__workerThreads:
					workerThread.join()
		except Exception as e:
			Log.error(str(e))


	def state(self):
		"""
		Gets service state.
		[out] true - Service is running.
		[out] false - Service is stopped.
		"""

		return self.__run


	def sendRequest(self, package):
		"""
		Sends request to service.
		[in] package - Contained method and arguments to call in request service.
		"""

		Log.debug(self.name + " to " + package.requestServiceName + ", " + package.requestMethod)

		try:
			if package.requestServiceName in Service.__jobsQueues:
				Service.__jobsQueues[package.requestServiceName].put(package, block = False)
			else:
				Log.warning(package.requestServiceName + " is nox exist")
		except Full:
			Log.warning(package.requestServiceName + " job queue is full")
		except Exception as e:
			Log.error(str(e))


	def sendResponse(self, package):
		"""
		Sends responce to service.
		[in] package - Contained method and arguments to call in response service.
		"""

		if package.responseServiceName == None:
			return

		Log.debug(self.name + " to " + package.responseServiceName + ", " + package.responseMethod)

		try:
			if package.responseServiceName in Service.__jobsQueues:
				Service.__jobsQueues[package.responseServiceName].put(package, block = False)
			else:
				Log.warning(package.responseServiceName + " is nox exist")
		except Full:
			Log.warning(package.responseServiceName + " job queue is full")
		except Exception as e:
			Log.error(str(e))


	def __request(self, package):
		"""
		Executes request method.
		[in] package - Contained method and arguments to call in request service.
		"""

		method = getattr(self, package.requestMethod, None)
		
		if method == None:
			Log.warning("Method " + package.requestMethod + " is not found in " + self.name)
			return

		Log.debug(self.name + " call " + package.requestMethod)

		if package.requestArguments == None:
			requestResult = method()
		else:
			requestResult = method(*package.requestArguments)
		
		if requestResult == None:
			package.responseArguments = None
		elif isinstance(requestResult, Iterable) == True: 
			package.responseArguments = requestResult
		else:
			package.responseArguments = [requestResult]

		self.sendResponse(package)
			

	def __respond(self, package):
		"""
		Executes respond method.
		[in] package - Contained method and arguments to call in response service.
		"""

		method = getattr(self, package.responseMethod, None)
		
		if method == None:
			Log.warning("Method " + package.responseMethod + " is not found in " + self.name)
			return 
		
		Log.debug(self.name + " call " + package.responseMethod)
		
		if package.responseArguments == None:
			method()
		else:
			method(*package.responseArguments)


	def __worker(self):
		"""
		Worker cycle gets packages from queue, calls request methods and response methods.
		"""

		while self.__run:
			try:
				package = self.__jobs.get(timeout = self.__workerTimeout)

				if package.requestServiceName == self.name:
					self.__request(package)

				elif package.responseServiceName == self.name:
					self.__respond(package)

				self.__jobs.task_done()

			except Empty:
				continue
			except Exception as e:
				Log.error(str(e))
