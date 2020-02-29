import logging
import pathlib
import csv
import helper_functions as hf

class Node():
	def __init__(self, node_data, meta_data = {}):
		self.parsed_info = self.parse_node(node_data)

		title = self.parsed_info['title']
		logging.debug(f'Generated new node for {hf.log_str_format(title)}')

		self.save_node_info(parsed_info = self.parsed_info,
							filename = self.parsed_info['id'])

	def parse_node(self, node_data: dict):
		"""
		Takes responses from OSF API and converts responses into useful format
		+ Values extracted:
		+-- name of study
		+-- osf ID of study
		+-- matched search parameter
		+-- matched parameter value
		+-- study description
		"""
		parsed_info = {}

		parsed_info['id'] = node_data['id']
		parsed_info['title'] = node_data['attributes']['title']
		parsed_info['tags'] = node_data['attributes']['tags']
		parsed_info['category'] = node_data['attributes']['category']
		parsed_info['description'] = node_data['attributes']['description'].strip().lower()
		parsed_info['date_created'] = node_data['attributes']['date_created']

		return parsed_info

	def save_node_info(self, parsed_info, filename,
					   node_dir = 'osf_nodes'):
		"""
		Creates a folder for the accessed node and saves tsv of data
		"""
		# Create directory
		try:
			folder_path = pathlib.Path().cwd().parent.joinpath(node_dir, filename)
			pathlib.Path.mkdir(folder_path, parents = True) # Parents makes missing parent directories, too
		except FileExistsError:
			title = parsed_info['title']
			logging.debug(f'Folder for "{hf.log_str_format(title)}" already exists -- skipping')
			return

		# Save file
		file_path = folder_path.joinpath(filename).with_suffix('.tsv')
		with open (file_path, 'w') as f:
			writer = csv.DictWriter(f, fieldnames = parsed_info.keys(), delimiter = '\t')
			writer.writeheader()
			writer.writerow(parsed_info)