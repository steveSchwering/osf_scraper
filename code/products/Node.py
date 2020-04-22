import json
import logging
import pathlib

class Node():
	def __init__(self, *args, **kwargs):
		self.data = kwargs
		try:
			logging.debug(f'Generated Node object for node {self.data["attributes"]["title"]} ({self.data["attributes"]["id"]})')
		except KeyError:
			logging.debug(f'Generated empty Node object')

	def describe(self,
				 max_title_chars = 75,
				 max_tags = 5):
		"""
		Prints information about the Node
		"""
		# Print the header of the node
		#-- Title
		if len(self.data["attributes"]["title"]) > max_title_chars:
			title = f'{self.data["attributes"]["title"][:max_title_chars]}...'
		else:
			title = self.data["attributes"]["title"]
		#-- Node id info
		if self.data["parent"]:
			parent_info = f'{self.data["attributes"]["id"]} --> {self.data["parent"]}'
		else:
			parent_info = f'{self.data["attributes"]["id"]}'

		print(f'{title} -- ({parent_info})')

		# Print info about the node
		print(f'\tTags: {self.data["attributes"]["tags"][:max_tags]}')
		print(f'\tDate created: {self.data["attributes"]["date_created"]}')
		
		print(f'\tChildren: {self.data["children"]}')
		print(f'\tAncestory: {self.sata["ancestors"]}')
		print(f'\tFiles:')
		for file in self.data['files']:
			print(f'\t\t{file["attributes"]["name"]} -- ({file["attributes"]["size"]})')

	def save(self, 
			 parent_directory = '../data/nodes',
			 nest_in_parent_directory = True,
			 filename = 'node_information',
			 mkdir = True,
			 overwrite = False):
		"""
		Creates a folder and file for Node
		"""
		# Set what the directory should be...
		if nest_in_parent_directory:
			# If we want each Node to have its own folder
			directory = pathlib.Path(parent_directory).joinpath('/'.join(self.data["ancestors"])).joinpath(self.data["attributes"]["id"])
		else:
			# If we want all Node data stored in the same file
			directory = pathlib.Path(parent_directory).joinpath(self.data["attributes"]["id"])

		# Create directory
		if not directory.exists() and mkdir:
			directory.mkdir(parents = True)

		# Create the json file for the Node
		filepath = directory.joinpath(self.data['attributes']['id']).with_suffix('.json')

		# If file exists and we do not want to overwrite, do not create file
		if filepath.exists() and not overwrite:
			logging.debug(f'Write Node failed: Node ({self.data["attributes"]["id"]}) exists on disk in {directory}')
			return False
		# else write file
		else:
			logging.debug(f'Writing node {self.data["attributes"]["title"]} ({self.data["attributes"]["title"]}) to disk')
			with open(filepath, 'w') as f:
				json.dump(obj = self.data, fp = f, indent = 4, sort_keys = True)
			return True