import datetime
import logging
import requests

def parse_storage(response, logger, *args, **kwargs):
	logger.debug(f'Parsing storage from <{response.url}>')

	response_json = response.json()

	# Parse file information
	files = []
	for file in response_json['data']:
		file_info = {
			'id'         : _deep_get(file, 'id'),
			'attributes' : {
				'guid'          : _deep_get(file, 'attributes', 'guid'),
				'name'          : _deep_get(file, 'attributes', 'name'),
				'kind'          : _deep_get(file, 'attributes', 'kind'),
				'size'          : _deep_get(file, 'attributes', 'size'),
				'date_created'  : _deep_get(file, 'attributes', 'date_created'),
				'date_modified' : _deep_get(file, 'attributes', 'date_modified')
			},
			'links'      : {
				'self_link'     : _deep_get(file, 'links', 'info'),
				'download_link' : _deep_get(file, 'links', 'download')
			}
		}
		files.append(file_info)

	# Append file information to general information for parse
	parsed_info = {
		'self_link'         : response.url,
		'accessed_datetime' : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
		'files'             : files
	}

	return parsed_info

def _deep_get(dct, *keys):
	for key in keys:
		try:
			dct = dct[key]
		except KeyError:
			return None
	return dct