import datetime
import logging
import requests

from parsers.parse_node import parse_node

def parse_children(response, logger, parent = None, ancestors = [], *args, **kwargs):
	logger.debug(f'Parsing children from <{response.url}>')

	response_json = response.json()

	# Parse file information
	parsed_nodes = []
	for node in response_json['data']:

		# Pull node information
		parsed_info = parse_node(response = node, logger = logger, parent = parent, ancestors = ancestors)

		# Add node to tracker
		parsed_nodes.append(parsed_info)

	parsed_info = {
		'self_link'         : response.url,
		'accessed_datetime' : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
		'parsed_nodes'      : parsed_nodes
	}

	return parsed_info