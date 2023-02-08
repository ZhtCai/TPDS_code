'''
Interface for network communication
'''

from mpi4py import MPI
import pickle

'''
Tag value:
1. Peer-to-peer transaction transmission: 20
2. Broadcast and receive transactions: 21
3. Broadcast and receive transaction blocks: 2
4. Broadcast and receive vote blocks: 22
5. Send and receive public key: 16
6. Broadcast and receive proposer blocks: 1
'''

class Communicator():
	def __init__(self, shard_num, node_type):
		self.comm = MPI.COMM_WORLD
		self.rank = self.comm.Get_rank()
		self.size = self.comm.Get_size()

		BUFSISE = 64 * (2**20)
		buf = bytearray(BUFSISE)
		MPI.Attach_buffer(buf)
	
	def recv_key(self):
		return self.comm.recv(tag=16)

	def recv_tx(self):
		return self.comm.recv(tag=20)

	def recv_txbc(self):
		return self.comm.recv(tag=21)

	def recv_problock(self):
		content = pickle.loads(self.comm.recv(tag=1))
		return content

	def recv_txblock(self):
		content = pickle.loads(self.comm.recv(tag=2))
		return content

	def recv_voteblock(self):
		content = pickle.loads(self.comm.recv(tag=22))
		return content

	def send_key(self, public_key, peers):
		for ip in peers:
			self.comm.ibsend((public_key, self.rank), dest=ip, tag=16)

	def send_tx(self, tx, ip):
		self.comm.isend((tx, self.rank), dest=ip, tag=20)

	def send_txbc(self, tx, peers):
		for ip in peers:
			self.comm.isend((tx, self.rank), dest=ip, tag=21)

	def send_problock(self, block, peers):
		for ip in peers:
			content = pickle.dumps((block, self.rank))
			self.comm.ibsend(content, dest=ip, tag=1)

	def send_txblock(self, block, peers):
		for ip in peers:
			content = pickle.dumps((block, self.rank))
			self.comm.ibsend(content, dest=ip, tag=2)

	def send_voteblock(self, block, peers):
		for ip in peers:
			content = pickle.dumps((block, self.rank))
			self.comm.ibsend(content, dest=ip, tag=22)