'''
Functions for nodes with TEE
'''

from communicator import Communicator
from random import choice, uniform, seed, shuffle
from transaction import Tx
from block import Block, TxBlock, VoteBlock
from merkletools import MerkleTools
from sqldb import *
from queue import Queue
import hashlib
import logging
import config
import util
import threading
import time
import copy
import pickle
import sys
import ecdsa

class TEE():
	def verify_txblock(self, block, previous_hash):
		'''
		Check whether a block is a valid candidate proposer block:
		1. whether the previous_hash field is consistent with the stored block
		2. whether the block satisfies the pow target
		3. whether all transactions are valid
		'''
		logging.info("Verifying block...")
		difficulty = block.difficulty
		b_hash = block.combine_hash_data()
		shard_id = block.get_shard_id()
		if block.previous_hash != previous_hash:
			logging.info('Previous hash fault! %s and %s!' % (block.previous_hash, previous_hash))
			return False

		if b_hash[0:difficulty] != ''.join(['0'] * difficulty):
			logging.info('PoW fault!')
			return False

		merkles = []
		for i in range(config.SHARD_NUM):
			merkles.append(MerkleTools(hash_type="md5"))
			for tx in block.txs[i]:
				merkles[i].add_leaf(tx.get_str().decode(), True)
			merkles[i].make_tree()
			if merkles[i].get_merkle_root() != block.merkles[i]:
				logging.info('Txs verified fault!')
				return False

		logging.info("Verified")
		return True


	def verify_voteblock(self, block):
		'''
		Check whether a block is a valid vote block:
		1. whether the previous_hash field is consistent with the stored block
		2. whether the block satisfies the pow target
		'''
		difficulty = block.difficulty
		b_hash = block.combine_hash_data()
		shard_id = block.get_shard_id()
		if b_hash[0:difficulty] != ''.join(['0'] * difficulty):
			logging.info('PoW fault! (Vote)')
			return False
		return True