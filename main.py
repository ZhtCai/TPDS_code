'''
Codes of full nodes
'''

import nodes
import threading
import config
import time
import random
import logging
import argparse
import sys
import pickle
import util
from communicator import Communicator
from transaction import Tx
from mpi4py import MPI
from sqldb import *

'''
Threads including the following functions:
1. Initializing transactions
2. Receiving transactions
3. Mining blocks
4. Receiving blocks
'''

'''
Threads receiving public keys
'''
class KeyRecvThread(threading.Thread):
	def __init__(self, node, *args, **kwargs):
		super(KeyRecvThread, self).__init__(*args, **kwargs)
		self.node = node
		self.__flag = threading.Event()
		self.__flag.set()
		self.__running = threading.Event()
		self.__running.set()

	def run(self):
		while self.__running.isSet():
			public_key, addr = self.node.recv_public_key()
			self.node.node_storage.public_keys[addr] = public_key

	def stop(self):
		self.__flag.set()
		self.__running.clear()


'''
Threads handling transactions
1. Sending transactions
2. Receiving and broadcasting transactions
3. Broadcasting received transactions
'''
class TxSendThread(threading.Thread):
	def __init__(self, node, recipients, *args, **kwargs):
		super(TxSendThread, self).__init__(*args, **kwargs)
		self.node = node
		self.recipients = recipients
		self.__flag = threading.Event()
		self.__flag.set()
		self.__running = threading.Event()
		self.__running.set()

	def run(self):
		while self.__running.isSet():
			self.__flag.wait()
			sender = self.node.node_storage.node_id
			recipient = random.choice(self.recipients)
			#recipient = self.recipients[0]
			amount = random.choice(range(1, config.TX_MAX_PAY))
			tx = Tx(sender, recipient, amount, time.time(), self.node.private_key)
			self.node.broadcast_tx(tx, util.get_shard(sender))

	def stop(self):
		self.__flag.set()
		self.__running.clear()


class TxRecvThread(threading.Thread):
	def __init__(self, node, *args, **kwargs):
		super(TxRecvThread, self).__init__(*args, **kwargs)
		self.node = node
		self.__flag = threading.Event()
		self.__flag.set()
		self.__running = threading.Event()
		self.__running.set()

	def run(self):
		while self.__running.isSet():
			tx = self.node.recv_tx()
			if tx.sender == -1:
				self.__running.clear()
			if tx.verify_tx():
				self.node.broadcast_tx(tx, util.get_shard(tx.sender))
			else:
				logging.info("Tx verified fault!")


class TxbcRecvThread(threading.Thread):
	def __init__(self, node, *args, **kwargs):
		super(TxbcRecvThread, self).__init__(*args, **kwargs)
		self.node = node
		self.__flag = threading.Event()
		self.__flag.set()
		self.__running = threading.Event()
		self.__running.set()

	def run(self):
		while self.__running.isSet():
			tx = self.node.recv_broadcast_tx()
			if tx.sender == -1:
				self.__running.clear()


'''
Threads receiving proposer blocks
'''
class ProBlockRecvThread(threading.Thread):
	def __init__(self, node, *args, **kwargs):
		super(ProBlockRecvThread, self).__init__(*args, **kwargs)
		self.node = node
		self.__flag = threading.Event()
		self.__flag.set()
		self.__running = threading.Event()
		self.__running.set()

	def run(self):
		while self.__running.isSet():
			exist = False
			block, addr = self.node.recv_problock()
			shard_id = util.get_shard(addr)
			block_hash = block.combine_hash_data()
			logging.info("Recv a proposer block %s in shard %d" % (block_hash, shard_id))
			for b in self.node.node_storage.get_candidate_blocks()[shard_id]:
				if b.combine_hash_data() == block_hash:
					exist = True
					break
			if exist:
				continue
			if self.node.verify_problock(block):
				self.node.node_storage.append_candidate_block(block, shard_id)
				self.node.node_storage.candidate_block_count[shard_id][block_hash] = 0
				logging.info("Now shard {} has {} candidate blocks".format(shard_id, len(self.node.node_storage.get_candidate_blocks()[shard_id])))
			else:
				logging.info("Proposer block verified failed.")


'''
Threads receiving transaction blocks
'''
class TxBlockRecvThread(threading.Thread):
	def __init__(self, node, *args, **kwargs):
		super(TxBlockRecvThread, self).__init__(*args, **kwargs)
		self.node = node
		self.__flag = threading.Event()
		self.__flag.set()
		self.__running = threading.Event()
		self.__running.set()

	def run(self):
		while self.__running.isSet():
			exist = False
			block, addr = self.node.recv_txblock()
			self.node.recv_buffer.put((block, addr))

	def stop(self):
		self.__flag.set()
		self.__running.clear()


class ArrangeTxblockThread(threading.Thread):
	def __init__(self, node, *args, **kwargs):
		super(ArrangeTxblockThread, self).__init__(*args, **kwargs)
		self.node = node
		self.__flag = threading.Event()
		self.__flag.set()
		self.__running = threading.Event()
		self.__running.set()

	def run(self):
		while True:
			if self.node.recv_buffer.qsize() <= 0:
				time.sleep(0.5)
				continue
			block, addr = self.node.recv_buffer.get()
			shard_id = util.get_shard(addr)
			#logging.info("Recv a txblock %s in shard %d" % (txblock_hash, shard_id))
			#logging.info('Receive a tx block %s in shard %d' % (txblock_hash, shard_id))
			#if self.node.verify_txblock(block):
			self.node.node_storage.txblocks[shard_id].append(block)
			#else:
			#logging.info("Txblock verified failed.")


'''
Threads receiving vote blocks
'''
class VoteBlockRecvThread(threading.Thread):
	def __init__(self, node, *args, **kwargs):
		super(VoteBlockRecvThread, self).__init__(*args, **kwargs)
		self.node = node
		self.__flag = threading.Event()
		self.__flag.set()
		self.__running = threading.Event()
		self.__running.set()

	def run(self):
		while self.__running.isSet():
			block, addr = self.node.recv_voteblock()
			self.node.vote_buffer.put((block, addr))

	def stop(self):
		self.__flag.set()
		self.__running.clear()


class ArrangeVoteblockThread(threading.Thread):
	def __init__(self, node, *args, **kwargs):
		super(ArrangeVoteblockThread, self).__init__(*args, **kwargs)
		self.node = node
		self.__flag = threading.Event()
		self.__flag.set()
		self.__running = threading.Event()
		self.__running.set()

	def run(self):
		while True:
			if self.node.vote_buffer.qsize() <= 0:
				time.sleep(0.5)
				continue
			block, addr = self.node.vote_buffer.get()
			shard_id = self.node.node_storage.shard_id
			from_shard = block.shard_id
			if block.verify_block(self.node.node_storage.public_keys[addr]) == False:
				logging.info("Vote Fails...")
				continue
			if shard_id == from_shard:
				self.node.node_storage.set_isvote(True)
			self.node.node_storage.append_voteblock(block)
			block_hashes = block.txblock_hash
			self.node.vote_from[from_shard] = True
			for i in range(config.SHARD_NUM):
				for candidate_block in self.node.node_storage.get_candidate_blocks()[i]:
					candidate_block_hash = candidate_block.combine_hash_data()
					if candidate_block_hash in block_hashes:
						self.node.node_storage.candidate_block_count[i][candidate_block_hash] += 1
						votes = str(self.node.node_storage.candidate_block_count[i][candidate_block_hash])
						logging.info('Receive a vote block from shard '+str(from_shard))
						logging.info('Now block '+candidate_block_hash+' gets '+votes+' votes in shard '+str(i))
						break

'''
Threads for mining blocks:
1. Mining proposer blocks and transaction blocks
2. Mining vote blocks
3. Handling votes
'''
class MiningProThread(threading.Thread):
	def __init__(self, node, shard_id, *args, **kwargs):
		super(MiningProThread, self).__init__(*args, **kwargs)
		self.node = node
		self.shard_id = shard_id
		self.__flag = threading.Event()
		self.__flag.set()
		self.__running = threading.Event()
		self.__running.set()

	def run(self):
		counter = 0
		ct = 0
		n = 0
		max_time = 15
		while counter < config.BLOCKS_NUMBER:
			spend_t = time.time()
			counter += 1
			logging.info('Mining Proposer block...')
			t = time.time()
			self.node.mine_problock(self.shard_id)
			spend_t = time.time() - spend_t
			if max_time > spend_t:
				time.sleep(max_time - spend_t)
			t = time.time()-t
			ct += t
			if self.node.node_storage.shard_id == self.shard_id:
				b = get_problock(self.node.node_storage.dbname)
				if b != None:
					n += b[10]
			if self.node.node_storage.shard_id == self.shard_id:
				logging.info('There are %d txs' % n)
				logging.info('Throughput: ' + str(n/ct))


class MiningTxThread(threading.Thread):
	def __init__(self, node, shard_id, *args, **kwargs):
		super(MiningTxThread, self).__init__(*args, **kwargs)
		self.node = node
		self.shard_id = shard_id
		self.__flag = threading.Event()
		self.__flag.set()
		self.__running = threading.Event()
		self.__running.set()

	def run(self):
		max_time = 1.5
		while True:
			spend_t = time.time()
			#logging.info('Mining tx block...')
			self.node.mine_txblock(self.shard_id, config.TX_PER_BLOCK)
			spend_t = time.time() - spend_t
			if max_time > spend_t:
				time.sleep(max_time - spend_t)

	def stop(self):
		self.__flag.set()
		self.__running.clear()


class VoteThread(threading.Thread):
	def __init__(self, node, shard_id, *args, **kwargs):
		super(VoteThread, self).__init__(*args, **kwargs)
		self.node = node
		self.shard_id = shard_id
		self.__flag = threading.Event()
		self.__flag.set()
		self.__running = threading.Event()
		self.__running.set()

	def run(self):
		while self.__running.isSet():
			time.sleep(0.5)
			waiting = False
			for i in range(config.SHARD_NUM):
				if len(self.node.node_storage.get_candidate_blocks()[i]) == 0:
					waiting = True
					break
			if waiting:
				continue
			for i in range(config.SHARD_NUM):
				logging.info('Arrange block in shard %d...' % (i))
				self.node.arrange_pro(i)

	def stop(self):
		self.__flag.set()
		self.__running.clear()


if __name__ == "__main__":
	'''
	parser = argparse.ArgumentParser(description='Blockchain simulation')
	parser.add_argument('-i', '--ip', metavar='ip', dest='ipaddr',help='Specify listen IP address', default='127.0.0.1')
	#parser.add_argument('-p', '--port', metavar='port', dest='port', help='Specify listen port', default=9000)
	parser.add_argument('-s', '--shard', metavar='shard', dest='shard', help='Specify shard id', default=0)
	parser.add_argument('--peers', dest='peers', nargs='*', help='Specify peers IP addresses', default=[])
	args = parser.parse_args()
	'''

	comm = MPI.COMM_WORLD
	all_nodes = [i for i in range(comm.Get_size())]
	node_ip = comm.Get_rank()
	shard_id = util.get_shard(node_ip)

	# Logging
	logging.basicConfig(filename=str(node_ip)+'.log', filemode='w', level=logging.INFO,
		format='%(asctime)s: %(message)s', datefmt='%d/%m/%Y %I:%M:%S')
	console = logging.StreamHandler()
	logging.getLogger('').addHandler(console)

	# Initializing network
	logging.info(str(all_nodes))
	logging.info('Createing node %d in %d...' % (node_ip, shard_id))
	node = nodes.Node(node_ip, shard_id, all_nodes, config.SHARD_NUM)

	try:
		# Wait until all nodes have received public keys
		recv_key = KeyRecvThread(node)
		recv_key.start()
		node.send_public_key()
		
		# Initializing transactions
		logging.info("Generating and accumulating transactions...")
		txbc_recv = TxbcRecvThread(node)
		tx_send = TxSendThread(node, all_nodes)
		txbc_recv.start()
		tx_send.start()
		
		# Start the process for receiving transactions
		recv_txblock = TxBlockRecvThread(node)
		recv_problock = ProBlockRecvThread(node)
		recv_txblock.start()
		recv_problock.start()

		# Start mining blocks
		arrange_txb = ArrangeTxblockThread(node)
		arrange_txb.start()
		mine_tx_thread = MiningTxThread(node, shard_id)
		mine_pro_thread = MiningProThread(node, shard_id)
		mine_tx_thread2 = MiningTxThread(node, shard_id)
		vote_thread = VoteThread(node, shard_id)
		logging.info('Start mining...')
		mine_tx_thread.start()
		mine_tx_thread2.start()
		vote_thread.start()
		if node_ip % config.NODES_PER_SHARD == 0:
			mine_pro_thread.start()
		
	except KeyboardInterrupt as e:
		node.close()
		MPI.Detach_buffer()
		print(e)
		sys.exit()