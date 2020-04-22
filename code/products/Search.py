import json
import logging
import pathlib

class Search():
	def __init__(self, *args, **kwargs):
		self.data = kwargs
		logging.debug('Generated Search object')