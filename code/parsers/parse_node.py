import datetime
import logging
import requests

def parse_node(response, logger, parent = None, ancestors = [], *args, **kwargs):
	# Converts response to dictionary json if a Response object
	if isinstance(response, requests.models.Response):
		logger.debug(f'Parsing node from <{response.url}> from Response')
		response = response.json()['data']

	if isinstance(response, tuple):
		logger.debug(f'Parsing top response from tuple')
		response = response[0]['data']

	# Parse the node now that it is a dictionary
	parsed_info = {
		'accessed_datetime' : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
		'attributes'        : {
			'id'            : _deep_get(response, 'id'),
			'title'         : _deep_get(response, 'attributes', 'title'),
			'description'   : _deep_get(response, 'attributes', 'description'),
			'tags'          : _deep_get(response, 'attributes', 'tags'),
			'category'      : _deep_get(response, 'attributes', 'category'),
			'date_created'  : _deep_get(response, 'attributes', 'date_created'),
			'date_modified' : _deep_get(response, 'attributes', 'date_modified'),
			'registration'  : _deep_get(response, 'attributes', 'registration'),
			'preprint'      : _deep_get(response, 'attributes', 'preprint'),
			'wiki'          : _deep_get(response, 'attributes', 'wiki_enabled'),
			'public'        : _deep_get(response, 'attributes', 'public')
		},
		'links'             : {
			'self_link'     : _deep_get(response, 'links', 'self'),
			'files_link'    : _deep_get(response, 'relationships', 'files', 'links', 'related', 'href'),
			'children_link' : _deep_get(response, 'relationships', 'children', 'links', 'related', 'href')
		},
		'searched'          : {
			'files'         : False,
			'children'      : False
		},
		'parent'            : parent,
		'ancestors'         : ancestors,
		'children'          : [],
		'files'             : []
	}

	return parsed_info

def _deep_get(dct, *keys):
	for key in keys:
		try:
			dct = dct[key]
		except KeyError:
			return None
	return dct