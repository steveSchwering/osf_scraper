# Author: Steve Schwering
# Reference: https://www.youtube.com/watch?v=PTO78zxkEtM
# Reference: https://osf.io/rs986/
# Reference: https://developer.osf.io/#tag/Nodes
# Reference: https://api.osf.io/v2/nodes/

import time
import json
import pathlib
import requests
import logging
import pandas as pd
import helper_functions as hf
from furl import furl
from Node import Node

class OSF_Scraper():
	def __init__(self, 
				 OSF_API_URL = 'https://api.osf.io/v2/'):
		# Logger
		logging.basicConfig(format = '%(asctime)s - %(levelname)s - %(message)s', level = logging.DEBUG)
		logging.debug('Created OSF scraper object')

		# Base URL
		self.OSF_API_URL = OSF_API_URL

		# Track nodes
		self.nodes = []

		# Start url queue
		self.request_queue = []
		self.initialize_queue(self.get_search_parameters())

	def get_search_parameters(self, 
							  parameter_dir = '../search_parameters', 
							  parent_path = None):
		"""
		Returns a dictionary of search parameters found in directory
		+ keys: names of parameters
		+ values: potential values of parameters
		"""

		output_parameters = []

		if not parent_path:
			parameter_paths = pathlib.Path().cwd().joinpath(parameter_dir).glob('*.tsv')
		else:
			parameter_paths = pathlib.Path().joinpath(parent_path, parameter_dir).glob('*.tsv')

		for parameter_path in parameter_paths:

			df = pd.read_csv(parameter_path, sep = '\t')

			output_parameters += df.T.to_dict().values()

		logging.debug(f'Found {len(output_parameters)} set(s) of parameters to search')
		return output_parameters

	def initialize_queue(self, search_parameters_l: list,
						 subdir = 'nodes/'):
		"""
		Initializes the request queue from search parameters
		"""
		for search_params in search_parameters_l:

			url = self.assemble_url(subdir = subdir, parameters = search_params)

			self.queue_url(url = url, subdir = subdir)

	def assemble_url(self, 
					 subdir = 'nodes/',
					 parameters = {},
					 case_sensitive = False):
		"""
		All urls are constructed with static OSF_API_URL
		+ subdir targets a specific section of the OSF API
		+ parameters filter the request
		"""
		url = furl(self.OSF_API_URL)

		url.path /= subdir

		# Parameters need be formatted with various values passed to filter
		for parameter_name in parameters:
			if parameters[parameter_name] != 'None':

				# See: https://developer.osf.io/#tag/Filtering
				if case_sensitive:
					parameter_f = f"filter[{parameter_name}][contains]"
				else:
					parameter_f = f"filter[{parameter_name}][icontains]"
				
				# Add parameter to furl
				url.args[parameter_f] = parameters[parameter_name]

		return url

	def queue_url(self, url, subdir):
		"""
		Adds a new url to the queue and queue info
		"""
		logging.debug(f'Adding {url.url} to position {len(self.request_queue) + 1} of queue')
		self.request_queue.append({'url' : url,
								   'subdir' : subdir})

	def feed_requests(self):
		"""
		Janky generator
		"""
		try:
			_ = self.request_queue.pop(0)
		except IndexError:
			logging.debug(f'No elements in request queue. You shouldn\'t be here.')
			raise
		return _

	def scrape(self):
		"""
		Takes urls from request queue and makes requests
		If more urls are detected in returned links, urls are added to request queue
		"""
		logging.debug(f'Starting request feed of length {len(self.request_queue)}')

		while self.request_queue:
			request_info = self.feed_requests()

			response, response_time = self.request_url(url = request_info['url'])

			links = self.parse_response(response = response, 
										subdir = request_info['subdir'], 
										meta_data = {'url' : request_info['url'],
													 'response_time' : response_time})

			for link in links:
				self.queue_url(url = link['url'], subdir = link['subdir'])

	def request_url(self, url):
		"""
		Request a set of data from the OSF API
		"""
		response_data = requests.get(url).json()
		response_time = time.strftime("%a-%Y-%m-%d %H:%M:%S", time.localtime())

		return response_data, response_time

	def parse_response(self, response, subdir, meta_data = {}):
		"""
		Parses response and returns next urls
		"""
		links = []

		# Nodes contain information about the indvidual studies
		if subdir == 'nodes/':

			for node_data in response['data']:
				self.nodes.append(Node(node_data = node_data,
									   meta_data = meta_data))

			# Next link is always another node
			if response['links']['next']:
				links.append({'url': furl(response['links']['next']),
							  'subdir' : 'nodes/'})

		return links

if __name__ == '__main__':
	scraper = OSF_Scraper()
	scraper.scrape()