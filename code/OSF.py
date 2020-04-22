# Author: Steve Schwering
# Reference: https://www.youtube.com/watch?v=PTO78zxkEtM
# Reference: https://osf.io/rs986/
# Reference: https://developer.osf.io/#tag/Nodes
# Reference: https://api.osf.io/v2/nodes/

import datetime
import logging
import pandas as pd
import pathlib
import requests

from yarl import URL

# Parsers
from parsers.parse_search import parse_search
from parsers.parse_node import parse_node
from parsers.parse_storage import parse_storage
from parsers.parse_children import parse_children

# Products
from products.Node import Node

class OSF():
	def __init__(self, **kwargs):
		# Get info
		self.client_info = kwargs

		# Logger
		self.initialize_logger()

	def initialize_logger(self,
						  logging_dir = './logs',
						  filename_root:str = 'osfl'):
		"""
		Creates logger object for the client
		+ filename uses osfl for Open Science Foundation Log
		"""
		# Make the log folder if it doesn't exist, continue if it does
		pathlib.Path(logging_dir).mkdir(parents = True, exist_ok = True)

		# Define filename of log
		now = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
		filename = f'{filename_root}_{now}.txt'
		log_path = pathlib.Path(logging_dir).joinpath(filename)

		# Create handler
		handler = logging.FileHandler(filename = log_path, mode = 'a')

		# Formatting log
		fh = logging.Formatter(fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		handler.setFormatter(fh)

		# Creating logger object and setting other parameters
		self.logger = logging.getLogger(name = now)
		self.logger.setLevel(logging.DEBUG)
		self.logger.addHandler(handler)

		self.logger.debug(f'Initialized logger')

	def get_search_parameters(self, 
							  parameter_dir = './search'):
		"""
		Returns a dictionary of search parameters found in directory
		+ keys:   names of parameters
		+ values: potential values of parameters
		"""
		parameter_paths = pathlib.Path(parameter_dir).glob('*.tsv')

		search_parameters = []
		for parameter_path in parameter_paths:
			df = pd.read_csv(parameter_path, sep = '\t')
			search_parameters += df.T.to_dict().values()

		self.logger.debug(f'Found {len(search_parameters)} set(s) parameters to search.')

		return search_parameters

	def format_search_parameters(self, parameters,
								 case_sensitive = False):
		"""
		Formats search parameters for later use
		"""
		final_params = {}

		for parameter in parameters:
			# Check if parameter has value
			if parameters[parameter] != 'None':
				# Check for case sensitivity
				if case_sensitive:
					parameter_f = f'filter[{parameter}][contains]'
				else:
					parameter_f = f'filter[{parameter}][icontains]'
				# Add formatted parameter to the listof url arguments
				final_params.update({parameter_f : parameters[parameter]})

		return final_params

	def build_url(self, url_scheme:str, url_host:str, url_path:str, url_query:dict = None):
		"""
		Generic url maker
		"""
		return URL.build(
			scheme = url_scheme,
			host = url_host,
			path = url_path,
			query = url_query
		)

	def request(self, url, 
				method = 'GET',
				callback = None,
				*args,
				**kwargs):
		"""
		Basic request function with a parse callback
		"""
		if method == 'GET':
			response = requests.get(url)
		elif method == 'POST':
			response = requests.post(url)

		self.logger.debug(
			f'{url.host} \"{method} {url.raw_path}?{url.query_string}\" {response.status_code}'
		)

		try:
			assert response.status_code == 200
		except AssertionError:
			self.logger.debug(f'Request returned non-200 status code. Cannot parse search results.')
			return None, response.status_code

		# Get callback
		if callback:
			parsed = callback(response = response, logger = self.logger, *args, **kwargs)
			return parsed, response.status_code
		else:
			return response, response.status_code

	def node(self, id,
			 url_scheme = 'https',
			 url_host = 'api.osf.io',
			 url_path = '/v2/nodes'):
		"""
		Searches for specific node with specific ID
		Functionality can be recreated with search -- this function is here for ease of use
		"""
		url = self.build_url(
			url_scheme = url_scheme,
			url_host = url_host,
			url_path = f'{url_path}/{id}'
		)

		return self.request(url = url, callback = parse_node)

	def storage(self, id,
				url_scheme = 'https',
				url_host = 'api.osf.io',
				url_path = '/v2/nodes'):
		"""
		Gets links to files of a given node, specifically from OSF storage
		"""
		url = self.build_url(
			url_scheme = url_scheme,
			url_host = url_host,
			url_path = f'{url_path}/{id}/files/osfstorage'
		)

		return self.request(url = url, callback = parse_storage)

	def append_storage(self, node_info):
		"""
		Adds storage information to a node's information
		Returns the node; if failed, returns unmodified node
		"""
		files, status_code = self.storage(id = node_info['attributes']['id'])
		
		try: # Save file info
			assert status_code == 200
			node_info['searched']['files'] = True
			node_info['files'] += files['files']
		except AssertionError:
			self.logger.debug(f'Cannot get file information for node {node_info["attributes"]["id"]}.')
			return node_info, status_code

		return node_info, status_code

	def children(self, parent,
				 ancestors = [],
				 url_scheme = 'https',
				 url_host = 'api.osf.io',
				 url_path = '/v2/nodes'):
		"""
		Gets information of child nodes of a given node
		"""
		url = self.build_url(
			url_scheme = url_scheme,
			url_host = url_host,
			url_path = f'{url_path}/{parent}/children'
		)

		ancestors = ancestors.append(parent)
		return self.request(url = url, callback = parse_children, parent = parent, ancestors = ancestors)

	def recursive_children(self, node_info):
		"""
		Recursively searches for child nodes
		Does some parsing on node_info
		Returns the passed node and its children
		"""
		node_id = node_info['attributes']['id']
		ancestors = node_info['ancestors']

		# Identify children of current node
		children, status_code = self.children(parent = node_id, ancestors = ancestors)
		children = children['parsed_nodes']
		node_info['searched']['children'] = True # Parsing

		# Add children ids to current node
		for child in children:
			node_info['children'].append(child['attributes']['id']) # Parsing

		# Recursively create new nodes for descendents
		descendents = []
		status_codes = [status_code]
		for child in children:
			family_branch, status_codes = self.recursive_children(node_info = child) # RECURSIVE CALL
			descendents += family_branch
			status_codes += status_codes

		return [node_info] + descendents, status_codes

	def expand_node(self, root_node):
		"""
		Recursively searches for files of node and child nodes. May create many new child nodes
		Combines a parsing function and a searching function
		"""
		print(f'Root node {root_node["attributes"]["id"]}: {root_node["ancestors"]}')

		# Create children recursively
		family, family_status_codes = self.recursive_children(node_info = root_node)

		# Iterate through the children and find their files
		file_status_codes = []
		for index, node in enumerate(family):
			family[index], file_status_code = self.append_storage(node_info = node)
			file_status_codes += [file_status_code]

		return family, {'family_status_codes': family_status_codes, 'file_status_codes' : file_status_codes}

	def search(self, 
			   parameters = None,
			   url = None,
			   page_size = 100,
			   url_scheme = 'https',
			   url_host = 'api.osf.io',
			   url_path = '/v2/nodes',
			   case_sensitive = False):
		"""
		Searches nodes by parameters
		Expects EITHER parameters OR url to be provided

		+ Maximum results returned by page_size is 100
		"""
		if url:
			try:
				assert parameters == None
			except:
				self.logger.debug('Called search with url but parameters also provided. Cannot handle')
				return None, None

		if not url:
			try: 
				assert parameters
			except AssertionError: 
				self.logger.debug('Called search with no url but no parameters provided')
				return None, None

			parameters_f = self.format_search_parameters(parameters = parameters,
														 case_sensitive = case_sensitive)
			parameters_f['page[size]'] = page_size # Add the page size parameter

			url = self.build_url(
				url_scheme = url_scheme,
				url_host = url_host,
				url_path = url_path,
				url_query = parameters_f
			)

		return self.request(url = url, callback = parse_search, parameters = parameters)

	def search_iterative(self, parameters):
		"""
		Calls a search and iterates through pages of search
		"""
		# Get initial search
		pages, status_codes = self.search(parameters = parameters)
		status_codes = [status_codes]
		next_page = pages[-1]['links']['next']

		# While we have more pages to search...
		while next_page:
			next_page = URL(next_page)
			page_n, page_n_status_code = self.search(url = next_page)

			# Add page to logger
			pages.append(page_n)
			status_codes.append(page_n_status_code)

			# Get next page
			next_page = page_n[-1]['links']['next']

		return pages, status_codes

if __name__ == '__main__':
	osf = OSF()
	search_parameters = osf.get_search_parameters()
	for query in search_parameters:
		results, _ = osf.search_iterative(query)
		for page in results:
			for node in page['parsed_nodes']:
				print(f'Node {node["attributes"]["id"]}: {node["ancestors"]}')
				expanded_nodes, _ = osf.expand_node(root_node = node)
				for child in expanded_nodes:
					n = Node(**child)
					#n.describe()
					n.save()