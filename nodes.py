'''
Ordinary full nodes
'''

from communicator import Communicator
from random import choice, uniform, seed, shuffle, randrange
from transaction import Tx
from block import Block, TxBlock, VoteBlock, ProBlock
from merkletools import MerkleTools
from sqldb import *
from queue import Queue
from copy import deepcopy
import hashlib
import tees
import logging
import config
import util
import threading
import time
import copy
import pickle
import sys
import ecdsa
import math

class NodeStorage():
	def __init__(self, node_id, shard_id, nodes, shard_num):
		self.node_id = node_id
		self.shard_id = shard_id
		self.nodes = nodes
		self.shard_num = shard_num
		self.longest_number = 3

		# candidate proposer blocks
		self.candidate_blocks = []
		for i in range(shard_num):
			self.candidate_blocks.append([])		# store proposer blocks in shards
		self.candidate_block_count = []
		for i in range(shard_num):
			self.candidate_block_count.append({})	# record votes based on the hash value

		# block database
		self.dbname = str(node_id) + '.db'
		dbConnect(self.dbname, shard_id, shard_num)

		# store vote blockchain
		self.votechain = []
		for i in range(shard_num):
			self.votechain.append([])
			for j in range(self.longest_number):
				self.votechain[i].append([])

		# whether the node has voted
		self.isVotes = False

		# the latest block hash value in each shard
		self.previous_hash = [0 for i in range(shard_num)]

		# transaction pool
		self.txs = {}

		# public keys of all nodes
		self.public_keys = {}

		# received transaction blocks
		self.txblocks = [[] for i in range(shard_num)]

	def set_isvote(self, i):
		self.isVotes = i

	def append_tx(self, tx):
		write_tx(self.dbname, tx)
		self.txs[tx.md5_hash()] = tx

	def get_txs_from_db(self):
		t = get_all_txs(self.dbname)
		for i in range(len(t)):
			tx = Tx(t[i][0], t[i][1], t[i][2], t[i][5], None)
			tx.set_sig(t[i][6])
			tx.set_pubkey(t[i][8])
			self.txs[tx.md5_hash()] = tx

	def get_txs(self, shard_id, to_shard_id, tx_num):
		result = []
		i = 0
		for tx in list(self.txs.values()):
			sender_shard = util.get_shard(tx.get_sender())
			recipient_shard = util.get_shard(tx.get_recipient())
			if shard_id == sender_shard and to_shard_id == recipient_shard:
				result.append(tx)
				i += 1
			if i >= tx_num:
				break
		return result

	def append_candidate_block(self, block, shard_id):
		self.candidate_blocks[shard_id].append(block)

	def empty_candidate_block(self, shard_id):
		self.candidate_blocks[shard_id].clear()
		self.candidate_block_count[shard_id].clear()

	def append_problock(self, block):
		write_problock(self.dbname, block, self.shard_num)

	def append_voteblock(self, block):
		shard_id = block.shard_id
		for i in range(self.longest_number-1):
			push = False
			if i == 0 and block.previous_hash == 0:
				self.votechain[shard_id][i].append(block)
				self.votechain[shard_id][i].sort(key=lambda x:x.timestamp)
				break
			for b in self.votechain[shard_id][i]:
				if b.combine_hash_data() == block.previous_hash:
					push = True
					self.votechain[shard_id][i+1].append(block)
					self.votechain[shard_id][i+1].sort(key=lambda x:x.timestamp)
					break
			if push:
				break

	def longest_chain(self, shard_id):
		block = None
		if len(self.votechain[shard_id][-1]) > 0:
			last_block = self.votechain[shard_id][-1][0]
			for i in range(len(self.votechain[shard_id])-2, -1, -1):
				for b in self.votechain[shard_id][i]:
					if b.combine_hash_data() == last_block.previous_hash:
						last_block = b
						break
			write_voteblock(self.dbname, last_block)
			del self.votechain[shard_id][0]
			self.votechain[shard_id].append([])

	def clean_txs(self, block):
		inner_tx = block.get_inner_tx()
		cross_tx = block.get_cross_tx()
		shard_id = block.get_shard_id()
		for tx in inner_tx:
			delete_tx(self.dbname, tx)
		for tx in cross_tx:
			delete_tx(self.dbname, tx)

	def get_candidate_blocks(self):
		return self.candidate_blocks

	def get_isvote(self):
		return self.isVotes

	def get_previous_hash(self):
		return self.previous_hash

	def get_txblocks(self, shard_id):
		return self.txblocks[shard_id][:]

	def set_previous_hash(self, previous_hash, shard_id):
		self.previous_hash[shard_id] = previous_hash


class Node():
	def __init__(self, node_id, shard_id, nodes, shard_num):
		# seeds for random numbers
		timeseed = int(str(time.time())[-4:])
		seed(timeseed)

		self.node_storage = NodeStorage(node_id, shard_id, nodes, shard_num)
		self.communicator = Communicator(shard_num, 'node')
		self.private_key = ecdsa.SigningKey.generate(curve=ecdsa.SECP256k1)

		# store received blocks
		self.recv_buffer = Queue()
		self.vote_buffer = Queue()
		#self.tee = tees.TEE()

		# record votes from each shard
		self.vote_from = [False for i in range(shard_num)]

		# get the transaction pool
		self.node_storage.get_txs_from_db()

		# record the number of verified transactions
		self.verified_txs = 0

	def send_public_key(self):
		public_key = self.private_key.verifying_key.to_string()
		self.communicator.send_key(public_key, self.node_storage.nodes)

	def recv_public_key(self):
		public_key, addr = self.communicator.recv_key()
		public_key = public_key.hex()
		return public_key, addr

	def send_tx(self, node):
		sender = self.node_storage.node_id
		recipient = node
		amount = choice(range(1, config.TX_MAX_PAY))
		tx = Tx(sender, recipient, amount, time.time(), self.private_key)
		#self.node_storage.append_tx(tx)
		self.communicator.send_tx(tx, recipient)
	
	def recv_tx(self):
		tx, addr = self.communicator.recv_tx()
		return tx

	def broadcast_tx(self, tx, shard_id):
		recipients = util.get_nodes_by_shard(shard_id, self.node_storage.nodes)
		self.communicator.send_txbc(tx, recipients)

	def recv_broadcast_tx(self):
		tx, addr = self.communicator.recv_txbc()
		if tx.verify_tx():
			self.node_storage.append_tx(tx)
		return tx

	def broadcast_problock(self, block):
		self.communicator.send_problock(block, self.node_storage.nodes)

	def recv_problock(self):
		block, addr = self.communicator.recv_problock()
		return block, addr

	def broadcast_txblock(self, block):
		block_header = deepcopy(block)
		block_header.txs.clear()
		shard_id = block.get_shard_id()
		recipient = util.get_nodes_by_shard(shard_id, self.node_storage.nodes)
		self.communicator.send_txblock(block, self.node_storage.nodes)

	def recv_txblock(self):
		block, addr = self.communicator.recv_txblock()
		return block, addr

	def broadcast_voteblock(self, block):
		self.communicator.send_voteblock(block, self.node_storage.nodes)

	def recv_voteblock(self):
		block, addr = self.communicator.recv_voteblock()
		return block, addr
	
	def verify_problock(self, block):
		'''
		Verify whether a proposer block:
		1. has the correct hash value
		2. satisfies the PoW target
		3. has the correct Merkle tree
		'''
		difficulty = block.difficulty
		b_hash = block.combine_hash_data()
		shard_id = block.get_shard_id()

		if block.previous_hash != self.node_storage.get_previous_hash()[shard_id]:
			logging.info('Previous hash fault of %s and %s!' % (block.previous_hash, self.node_storage.get_previous_hash()[shard_id]))
			return False
		merkle = MerkleTools(hash_type="md5")
		txb_hashes = [tb.combine_hash_data() for tb in self.node_storage.get_txblocks(shard_id)]
		for tb in block.txlinks:
			merkle.add_leaf(tb.get_header_str(), True)
			if tb.combine_hash_data() not in txb_hashes:
				# logging.info('Txs verified fault in proposer block!')
				# return False
				pass
		merkle.make_tree()
		if merkle.get_merkle_root() != block.txmerkle:
			logging.info('Txs merkle verified fault in proposer block!')
			return False

		return True

	def verify_txblock(self, block):
		'''
		Verify whether a transaction block:
		1. satisfies the PoW target
		2. has the correct Merkle tree of valid transactions
		'''
		difficulty = block.difficulty
		b_hash = block.combine_hash_data()
		shard_id = block.get_shard_id()
		merkle = MerkleTools(hash_type="md5")
		for tx in block.txs:
			merkle.add_leaf(tx.get_str().decode(), True)
			if tx.md5_hash() not in self.node_storage.txs.keys():
				#if not tx.verify_tx():
				#logging.info('Txs verified fault in tx block!')
				#return False
				pass
		merkle.make_tree()
		if merkle.get_merkle_root() != block.merkle:
			logging.info('Txs verified fault!')
			return False

		return True

	def verify_voteblock(self, block):
		'''
		Verify whether a vote block:
		1. matches the stored hash values of the latest blocks
		2. satisfies the PoW target
		'''
		difficulty = block.difficulty
		b_hash = block.combine_hash_data()
		shard_id = block.get_shard_id()
		if b_hash[0:difficulty] != ''.join(['0'] * difficulty):
			logging.info('PoW fault! (Vote)')
			return False
		return True

	def mine_problock(self, shard_id):
		# generate a Merkle Tree and append it into the block
		nodes_num = config.TOTAL_NUMBER_OF_NODES
		tbs = self.node_storage.get_txblocks(shard_id)[:]
		merkle = MerkleTools(hash_type="md5")
		for i in range(len(tbs)):
			merkle.add_leaf(tbs[i].get_header_str(), True)
			tbs[i].txs.clear()
		merkle.make_tree()

		# calculate the hash value of the previous block and generate a new proposer block
		previous_hash = self.node_storage.get_previous_hash()[shard_id]
		block = ProBlock(shard_id, 
						config.NODES_PER_SHARD,
						previous_hash,
						time.time(),
						config.DIFFICULTY,
						0,
						False,
						merkle.get_merkle_root(),
						tbs)

		# block mining
		difficulty = block.get_difficulty()
		length = len(self.node_storage.get_candidate_blocks()[shard_id])
		while length <= 0:
			b_hash = block.combine_hash_data()
			length = len(self.node_storage.get_candidate_blocks()[shard_id])
			if b_hash[0:difficulty] == ''.join(['0'] * difficulty):
				if length > 0:
					break
				block.set_public_key(self.private_key)
				logging.info("I mine a proposer block in shard {} {}".format(block.get_shard_id(), b_hash))
				time.sleep(0.1*uniform(1, math.log(5*nodes_num, 2)))
				self.broadcast_problock(block)
				break
			block.plus_nonce()
		
		
	def mine_txblock(self, shard_id, tx_num):
		# generate a Merkle Tree of transactions and append it into the block
		nodes_num = config.TOTAL_NUMBER_OF_NODES
		to_shard_id = randrange(0, self.node_storage.shard_num)
		t = self.node_storage.get_txs(shard_id, to_shard_id, tx_num)
		num = len(t)
		if num >= config.TX_PER_BLOCK:
			num = config.TX_PER_BLOCK
		txs = []
		for i in range(num):
			txs.append(t[i])
			
		shuffle(txs)
		merkle = MerkleTools(hash_type="md5")
		for tx in txs:
			merkle.add_leaf(tx.get_str().decode(), True)
		merkle.make_tree()

		# calculate the hash value of the previous block and generate a new transaction block
		previous_hash = self.node_storage.get_previous_hash()[shard_id]
		block = TxBlock(shard_id,
						config.NODES_PER_SHARD,
						previous_hash,
						time.time(),
						config.TX_DIFFICULTY,
						0,
						False,
						merkle.get_merkle_root(),
						txs,
						None)

		# block mining
		difficulty = block.get_difficulty()
		length = len(self.node_storage.get_candidate_blocks()[shard_id])
		time.sleep(choice([0.7, 1, 1.3]) + 0.1*uniform(1, math.log(5*nodes_num, 2)))
		if self.verify_txblock(block):
			self.broadcast_txblock(block)

	def mine_voteblock(self, shard_id, index):
		# generate a Merkle Tree of proposer blocks and append it into the block
		nodes_num = config.TOTAL_NUMBER_OF_NODES
		vote_hashes = []
		for sid in range(self.node_storage.shard_num):
			length = len(self.node_storage.get_candidate_blocks()[sid])
			if length == 0 or index >= length:
				return
			candidate_block = sorted(self.node_storage.get_candidate_blocks()[sid], key=lambda x:x.timestamp)[index]
			vote_hashes.append(candidate_block.combine_hash_data())

		# calculate the hash value of the previous block and generate a new vote block
		previous_hash = 0
		for i in range(len(self.node_storage.votechain[shard_id])-1, -1, -1):
			if len(self.node_storage.votechain[shard_id][i]) > 0:
				previous_hash = self.node_storage.votechain[shard_id][i][0].combine_hash_data()
				break
		block = VoteBlock(self.node_storage.shard_id,
						config.NODES_PER_SHARD,
						previous_hash,
						time.time(),
						config.VOTE_DIFFICULTY,
						0,
						vote_hashes)

		# block mining
		from_shard = block.get_shard_id()
		difficulty = block.get_difficulty()
		while not self.node_storage.get_isvote():
			b_hash = block.combine_hash_data()
			if b_hash[0:difficulty] == ''.join(['0'] * difficulty):
				if self.node_storage.get_isvote():
					break
				if not self.verify_voteblock(block):
					logging.info("Verified voteblock failed.")
					continue
				block.set_public_key(self.private_key)
				time.sleep(0.1*uniform(1, math.log(5*nodes_num, 2)))
				self.broadcast_voteblock(block)
				logging.info("I mine a voteblock in shard %d." % from_shard)
				break
			block.plus_nonce()
	
	'''
	Check the votes and confirm one proposer blocks
	'''
	def arrange_votes(self, shard_id):
		i = shard_id
		block_hash = None

		# check whether all shards have mined new blocks
		if len(self.node_storage.candidate_block_count[i].values()) == 0:
			return False

		# check whether all shards have voted
		for vote in self.vote_from:
			if vote == False:
				return False

		# confirm the proposer block with the most votes
		max_vote = max(self.node_storage.candidate_block_count[i].values())
		for h in self.node_storage.candidate_block_count[i].keys():
			count = self.node_storage.candidate_block_count[i][h]
			if count == max_vote:
				block_hash = h
				break
		if block_hash == None:
			return False
		for block in self.node_storage.get_candidate_blocks()[i]:
			if block.combine_hash_data() == block_hash:
				logging.info('A block is pushed in shard ' + str(i))
				self.node_storage.set_previous_hash(block_hash, i)
				
				# delete related transaction blocks
				hashes = [tb.combine_hash_data() for tb in block.txlinks]
				
				for txb in self.node_storage.get_txblocks(shard_id):
					if (txb.combine_hash_data() in hashes and
							shard_id != self.node_storage.shard_id and
							util.get_shard(txb.txs[0].get_sender()) != self.node_storage.shard_id and
							util.get_shard(txb.txs[0].get_recipient()) == self.node_storage.shard_id):
						self.node_storage.txblocks[self.node_storage.shard_id].append(txb)
				
				f = filter(lambda x: x.combine_hash_data() not in hashes, self.node_storage.get_txblocks(shard_id))
				self.node_storage.txblocks[shard_id] = list(f)

				if self.node_storage.shard_id == i:
					self.node_storage.append_problock(block)
					logging.info('Length is %d in shard %d' % (get_problock_count(self.node_storage.dbname), self.node_storage.shard_id))
				self.node_storage.empty_candidate_block(i)
				break
		self.node_storage.set_isvote(False)
		for sid in range(self.node_storage.shard_num):
			self.node_storage.longest_chain(sid)
		return True
