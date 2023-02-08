'''
Transaction class
'''

import hashlib
import util
import random
import ecdsa

class Tx():
	'''
	Each transation contains: id, sender, recipient, amount, intershard
	'''
	def __init__(self,
				sender,
				recipient,
				amount,
				timestamp,
				sk):
		self.id = random.randrange(10**30, 10**31)
		self.sender = sender
		self.recipient = recipient
		self.amount = amount
		self.timestamp = timestamp
		self.intershard = self._is_intershard()
		if sk == None:
			self.sig = None
		else:
			self.sig = sk.sign(self.get_str()).hex()
			self.public_key = sk.verifying_key.to_string().hex()

	def set_sig(self, sig):
		self.sig = sig

	def set_pubkey(self, k):
		self.public_key = k

	def get_sig(self):
		return self.sig

	def get_pubkey(self):
		return self.public_key

	def get_sender(self):
		return self.sender

	def get_recipient(self):
		return self.recipient

	def get_str(self):
		result = '%s %s %d %f'%(self.sender, self.recipient, self.amount, self.timestamp)
		return result.encode()

	def md5_hash(self):
		return hashlib.md5(self.get_str()).hexdigest()

	def _is_intershard(self):
		'''
		Indicates that a transaction is an inner-shard transaction
		'''
		if util.get_shard(self.sender) == util.get_shard(self.recipient):
			return True
		else:
			return False

	def verify_tx(self):
		vk = ecdsa.VerifyingKey.from_string(bytes.fromhex(self.public_key), curve=ecdsa.SECP256k1)
		try:
			if vk.verify(bytes.fromhex(self.sig), self.get_str()):
				return True
		except:
			return False