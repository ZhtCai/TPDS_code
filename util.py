'''
Various functions
'''

import config


def get_shard(node):
	'''
	return the shard id of a node
	'''
	return int(node/config.NODES_PER_SHARD)


def get_nodes_by_shard(shard_id, nodes):
	'''
	return the list of all nodes in the shard with id equalling to shard_id
	'''
	shard = [node for node in nodes if get_shard(node) == shard_id]
	return shard