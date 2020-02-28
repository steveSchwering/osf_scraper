# Author: Steve Schwering
# Reference: https://www.youtube.com/watch?v=PTO78zxkEtM
# Reference: https://osf.io/rs986/

import csv
import time
import json
import pathlib
import requests
import logging
import pandas as pd
from furl import furl

class OSF_Scraper():
	def __init__(self, OSF_API_URL = 'https://api.osf.io/v2/'):
		# Logger
		logging.basicConfig(format = '%(asctime)s - %(levelname)s - %(message)s', level = logging.DEBUG)
		logging.info('Created OSF scraper object')

		# Base URL
		self.OSF_API_URL = OSF_API_URL

		# Get search parameters
		search_parameters_l = self.get_search_parameters()

		# Start url queue
		self.initialize_queue(search_parameters_l)

	def get_search_parameters(self, 
							  parameter_dir = '../search_parameters', 
							  parent_path = None,
							  glob_search = '*.tsv'):
		"""
		Returns a dictionary of search parameters found in directory
		+ keys: names of parameters
		+ values: potential values of parameters
		"""

		output_parameters = []

		if not parent_path:
			parameter_paths = pathlib.Path().cwd().joinpath(parameter_dir).glob(glob_search)
		else:
			parameter_paths = pathlib.Path().joinpath(parent_path, parameter_dir).glob(glob_search)

		for parameter_path in parameter_paths:

			if 'tsv' in parameter_path.suffix:
				df = pd.read_csv(parameter_path, sep = '\t')
			elif 'csv' in parameter_path.suffix:
				df = pd.read_csv(parameter_path, sep = ',')

			output_parameters += df.T.to_dict().values()

		logging.info(f'Found {len(output_parameters)} set of parameter(s) to search')
		return output_parameters

	def initialize_queue(self, search_parameters_l: list):
		"""
		Initializes the request queue by generating urls from the search parameters
		"""
		self.request_queue = [url for url in self.generator_param_urls(search_parameters_l)]

	def generator_param_urls(self, search_parameters_l: list,
							 subdir_node = 'nodes/'):
		"""
		Generator of urls
		"""
		for parameters in search_parameters_l:
			yield self.assemble_url(parameters = parameters,
									subdir_node = subdir_node)

	def assemble_url(self, parameters: dict, 
					 subdir_node = 'nodes/',
					 case_sensitive = False):
		"""
		Construct URLs by which OSF JSON data can be accessed. 
		Parameters are supplied to filters restrict how nodes are accessed.
		+ furl implements a url object: https://github.com/gruns/furl
		"""
		url = furl(self.OSF_API_URL)

		url.path /= subdir_node

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

	def feed_requests(self):
		"""
		Takes urls from request queue and makes requests
		If more urls are detected in returned links, urls are added to request queue
		"""
		logging.info(f'Starting requests with feed of length {len(self.request_queue)}')

		for url in self.request_queue:
			links = self.process_url(url)

			# TODO: Project how much longer scraping will take given 'last' parameter in links

			if links['next']:
				next_page = furl(links['next'])
				logging.debug(f'Adding next page of {next_page.url} to queue')
				self.request_queue.append(next_page)

	def process_url(self, url,
					save_extension = '.tsv'):
		"""
		Requests url, processes returned nodes, and saves node info
		Returns links to the next requests
		"""
		# Request data from OSF
		data, links = self.request_nodes(url)

		# Save data storage
		parsed_info = {'url': url.url,
					   'time_access': time.strftime("%a-%Y-%m-%d %H:%M:%S", time.localtime())}

		# Parse node and save
		for node in data:
			parsed_info = self.parse_node(node, parsed_info = parsed_info)
			self.save_node_info(parsed_info = parsed_info, 
								filename = parsed_info['id'],
								save_extension = save_extension)

		# Return links
		return links

	def request_nodes(self, url: str):
		"""
		Request a set of data from the OSF API
		"""
		response = requests.get(url).json() 

		return response['data'], response['links']

	def parse_node(self, node: dict, parsed_info = {}):
		"""
		Takes responses from OSF API and converts responses into useful format
		Reference: https://developer.osf.io/#tag/Nodes
		Reference: https://api.osf.io/v2/nodes/
		+ Values extracted:
		+-- name of study
		+-- osf ID of study
		+-- matched search parameter
		+-- matched parameter value
		+-- study description
		"""
		title = node['attributes']['title']
		logging.info(f'Parsing "{title}"')

		parsed_info['id'] = node['id']
		parsed_info['title'] = title
		parsed_info['tags'] = node['attributes']['tags']
		parsed_info['category'] = node['attributes']['category']
		parsed_info['description'] = node['attributes']['description'].strip().lower()
		parsed_info['date_created'] = node['attributes']['date_created']

		return parsed_info

	def save_node_info(self, parsed_info, filename,
					   save_extension = '.tsv',
					   node_dir = 'nodes'):
		"""
		Creates a folder for the accessed node and saves csv of data
		"""

		# Create directory
		try:
			folder_path = pathlib.Path().cwd().parent.joinpath(node_dir, filename)
			pathlib.Path.mkdir(folder_path)
		except FileExistsError:
			title = parsed_info['title']
			logging.debug(f'Folder for "{title}" already exists -- skipping')
			return

		# Save file
		file_path = folder_path.joinpath(filename).with_suffix(save_extension)
		with open (file_path, 'w') as f:
			writer = csv.DictWriter(f, fieldnames = parsed_info.keys(), delimiter = '\t')
			writer.writeheader()
			writer.writerow(parsed_info)

if __name__ == '__main__':
	scraper = OSF_Scraper()
	scraper.feed_requests()