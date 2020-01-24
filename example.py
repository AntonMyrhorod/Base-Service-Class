"""
Example of using base Service class.
To run: python3 example.py 
"""

from time import sleep

import log
from log import Log
from service import Service, Package


class User(Service):
	"""
	User service.
	"""

	def __init__(self):
		super().__init__(self.__class__.__name__, workerTimeout = 1)


	def start(self):
		super().start()


	def askPlusOperation(self, a, b):
		Log.debug(str(a) + ", " + str(b))

		self.sendRequest(Package("Calculator", "plusOperation", [a, b], "User", "onPlusOperationDone"))


	def onPlusOperationDone(self, a, b, result):
		Log.debug(str(a) + " + " + str(b) + " = " + str(result))


class Calculator(Service):
	"""
	Calculator service.
	"""

	def __init__(self):
		super().__init__(self.__class__.__name__, workerTimeout = 1)


	def start(self):
		super().start()


	def plusOperation(self, a, b):
		Log.debug(str(a) + ", " + str(b))

		return a, b, a + b


def main():
	log.init("debug", "main.log")

	calculator_service = Calculator()
	user_service = User()

	calculator_service.start()
	user_service.start()

	user_service.askPlusOperation(34, 21)

	calculator_service.stop()
	user_service.stop()


if __name__ == '__main__':
	main()
