'''
Block Class
'''

from transaction import Tx
import hashlib
import random
import sys
import ecdsa

class Block():
	def __init__(self,
				shard_id,					
				shard_length,				
				previous_hash,				# hash value of the previous block
				timestamp,					
				difficulty,					# Pow target
				nonce						# Pow random number
				):
		self.shard_id = shard_id
		self.shard_length = shard_length
		self.previous_hash = previous_hash
		self.timestamp = timestamp
		self.difficulty = difficulty
		self.nonce = nonce
		self.sig = None

	def get_shard_id(self):
		return self.shard_id

	def get_difficulty(self):
		return self.difficulty

	def plus_nonce(self):
		self.nonce += 1
		return self.nonce

	def random_nonce(self):
		self.nonce = random.randint(0, 100000)
		return self.nonce

	def get_str(self):
		shard_data = str(self.shard_id) + str(self.shard_length)
		time_data = str(self.timestamp)
		data = shard_data + time_data + str(self.nonce)
		return data.encode()

	def set_public_key(self, sk):
		self.sig = sk.sign(self.get_str()).hex()

	def verify_block(self, public_key):
		vk = ecdsa.VerifyingKey.from_string(bytes.fromhex(public_key), curve=ecdsa.SECP256k1)
		try:
			if vk.verify(bytes.fromhex(self.sig), self.get_str()):
				return True
		except:
			return False


'''
Proposer block
'''
class ProBlock(Block):
	def __init__(self, 
				shard_id,
				shard_length,
				previous_hash,
				timestamp,
				difficulty,
				nonce,
				validity_proof,
				txmerkle,
				txlinks):
		super(ProBlock, self).__init__(shard_id,
									shard_length,
									previous_hash,
									timestamp,
									difficulty,
									nonce)
		self.validity_proof = validity_proof
		self.txmerkle = txmerkle
		self.txlinks = txlinks

	def combine_hash_data(self):
		shard_data = str(self.shard_id) + str(self.shard_length)
		time_data = str(self.timestamp)
		tx_data = ''
		if self.txmerkle != None:
			tx_data = self.txmerkle
		data = shard_data + time_data + tx_data + str(self.nonce)
		return hashlib.sha256(data.encode('utf-8')).hexdigest()


'''
Transaction block
'''
class TxBlock(Block):
	def __init__(self,
				shard_id,
				shard_length,
				previous_hash,
				timestamp,
				difficulty,
				nonce,
				validity_proof,				# prove the correctness of the block
				merkle,						# Merkle Tree root of transactions
				txs,						# transactions
				links						# link to other blocks
				):
		super(TxBlock, self).__init__(shard_id,
									shard_length,
									previous_hash,
									timestamp,
									difficulty,
									nonce)
		self.validity_proof = validity_proof
		self.merkle = merkle
		self.txs = txs
		self.links = links

	def get_header_str(self):
		shard_data = str(self.shard_id) + str(self.shard_length)
		time_data = str(self.timestamp)
		tx_data = ''
		if self.merkle != None:
			tx_data = self.merkle
		data = shard_data + time_data + tx_data + str(self.nonce)
		return data

	def combine_hash_data(self):
		data = self.get_header_str()
		return hashlib.sha256(data.encode('utf-8')).hexdigest()

	def get_txs(self):
		return self.txs


'''
Vote block
'''
class VoteBlock(Block):
	def __init__(self,
				shard_id,
				shard_length,
				previous_hash,
				timestamp,
				difficulty,
				nonce,
				txblock_hash,				# hash of the linked block
				):
		super(VoteBlock, self).__init__(shard_id,
									shard_length,
									previous_hash,
									timestamp,
									difficulty,
									nonce)
		self.txblock_hash = txblock_hash

	def combine_hash_data(self):
		shard_data = str(self.shard_id) + str(self.shard_length)
		time_data = str(self.timestamp)
		data = shard_data + time_data + str(self.nonce)
		return hashlib.sha256(data.encode('utf-8')).hexdigest()
