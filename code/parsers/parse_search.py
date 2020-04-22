import datetime
import logging

from parsers.parse_node import parse_node

def parse_search(response, logger, *args, **kwargs):
	"""
	Takes responses from OSF API and converts responses into useful format
	Essentially, we are converting OSF's json into the json we want and then wrapping the parse_node function
	"""
	logger.debug(f'Parsing search <{response.url}>')

	response_json = response.json()

	parsed_nodes = [] # All nodes stored here
	for node in response_json['data']:

		# Pull the information we want
		parsed_info = parse_node(response = node, logger = logger)

		# Add the node to node tracker
		parsed_nodes.append(parsed_info)

	search_info = [{
		'accessed_datetime' : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
		'search_parameters' : kwargs['parameters'],
		'parsed_nodes'      : parsed_nodes,
		'links'             : response_json['links']
	}]

	return search_info