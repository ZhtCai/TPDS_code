'''
Store different types of data in databases
'''

import sqlite3
import logging
import util
import config

def dbConnect(filename, shard_id, shard_num):
	db = sqlite3.connect(filename)
	cursor = db.cursor()
	cursor.execute("""CREATE TABLE IF NOT EXISTS problocks (
		bid integer NOT NULL,
		shard_id integer NOT NULL,
		shard_length integer NOT NULL,
		previous_hash text,
		b_time real NOT NULL,
		difficulty integer,
		nonce integer,
		validity_proof integer,
		txmerkle text,
		txlink text,
		tx_num integer,
		CONSTRAINT c_txblock PRIMARY KEY (bid, previous_hash))""")
	for i in range(shard_num):
		name = 'voteblocks%d' % i
		cursor.execute("""CREATE TABLE IF NOT EXISTS %s (
			bid integer NOT NULL,
			shard_id integer NOT NULL,
			shard_length integer NOT NULL,
			previous_hash text,
			b_time real,
			difficulty integer NOT NULL,
			nonce integer,
			txblock_hash text,
			CONSTRAINT c_voteblock PRIMARY KEY (bid, previous_hash))""" % (name))
	cursor.execute("""CREATE TABLE IF NOT EXISTS txs (
		sender integer,
		recipient integer NOT NULL,
		amount integer,
		from_shard_id integer,
		to_shard_id integer,
		tx_time real,
		sig text,
		txid integer,
		public_key text)""")
	db.commit()
	db.close()


def write_tx(filename, tx):
	db = sqlite3.connect(filename)
	cursor = db.cursor()
	number = get_txs_count(filename, util.get_shard(tx.sender))
	cursor.execute('INSERT INTO txs VALUES (?,?,?,?,?,?,?,?,?)', (
		tx.sender,
		tx.recipient,
		tx.amount,
		util.get_shard(tx.sender),
		util.get_shard(tx.recipient),
		tx.timestamp,
		tx.get_sig(),
		number,
		tx.get_pubkey()))
	db.commit()
	db.close()


def delete_tx(filename, tx):
	db = sqlite3.connect(filename)
	cursor = db.cursor()
	sender = tx.get_sender()
	recipient = tx.get_recipient()
	timestamp = tx.timestamp
	sig = tx.sig
	cursor.execute("DELETE FROM txs WHERE sender like '%s' and recipient like '%s' and sig like '%s' and tx_time=%f" % (sender, recipient, sig, timestamp))
	db.commit()
	db.close()


def write_problock(filename, b, shard_num):
	db = sqlite3.connect(filename)
	cursor = db.cursor()
	bid = get_problock_count(filename)
	tx_num = 0
	for tb in b.txlinks:
		tx_num += config.TX_PER_BLOCK
	cursor.execute('INSERT INTO problocks VALUES (?,?,?,?,?,?,?,?,?,?,?)',(
		bid,
		b.shard_id,
		b.shard_length,
		b.previous_hash,
		b.timestamp,
		b.difficulty,
		b.nonce,
		b.validity_proof,
		b.txmerkle,
		str([h.combine_hash_data() for h in b.txlinks]),
		tx_num))
	db.commit()
	db.close()


def write_voteblock(filename, b):
	db = sqlite3.connect(filename)
	cursor = db.cursor()
	name = 'voteblocks%d' % (b.shard_id)
	bid = get_voteblock_count(filename, b.shard_id)
	cursor.execute('INSERT INTO %s VALUES (?,?,?,?,?,?,?,?)' % name, (
		bid,
		b.shard_id,
		b.shard_length,
		b.previous_hash,
		b.timestamp,
		b.difficulty,
		b.nonce,
		str(b.txblock_hash)))
	db.commit()
	db.close()


def change_voteblock(filename, b):
	db = sqlite3.connect(filename)
	cursor = db.cursor()
	name = 'voteblocks%d' % (b.shard_id)
	bid = get_voteblock_count(filename, b.shard_id)-1
	cursor.execute('DELETE FROM %s WHERE bid=%d' % (name, bid))
	db.commit()
	db.close()
	write_voteblock(filename, b)


def get_txs(filename, shard_id, number):
	db = sqlite3.connect(filename)
	cursor = db.cursor()
	cursor.execute('SELECT * FROM txs WHERE (from_shard_id=%d or to_shard_id=%d) and txid>=%d' % (shard_id, shard_id, number))
	l = cursor.fetchall()
	db.close()
	return l


def get_all_txs(filename):
	db = sqlite3.connect(filename)
	cursor = db.cursor()
	cursor.execute('SELECT * FROM txs')
	l = cursor.fetchall()
	db.close()
	return l


def get_txs_count(filename, shard_id):
	db = sqlite3.connect(filename)
	cursor = db.cursor()
	cursor.execute('SELECT COUNT(*) FROM txs WHERE from_shard_id=%d or to_shard_id=%d' % (shard_id, shard_id))
	l = cursor.fetchone()
	db.close()
	return l[0]


def get_problock(filename):
	db = sqlite3.connect(filename)
	cursor = db.cursor()
	cursor.execute('SELECT * FROM problocks WHERE bid=(SELECT max(bid) FROM problocks)')
	l = cursor.fetchone()
	db.close()
	return l


def get_problock_count(filename):
	db = sqlite3.connect(filename)
	cursor = db.cursor()
	cursor.execute('SELECT COUNT(*) FROM problocks')
	l = cursor.fetchone()
	db.close()
	return l[0]


def get_voteblock(filename, from_shard_id):
	db = sqlite3.connect(filename)
	cursor = db.cursor()
	name = 'voteblocks%d' % from_shard_id
	cursor.execute('SELECT * FROM %s WHERE bid=(SELECT max(bid) FROM %s)' % (name, name))
	l = cursor.fetchone()
	db.close()
	return l


def get_voteblock_count(filename, from_shard_id):
	db = sqlite3.connect(filename)
	cursor = db.cursor()
	name = 'voteblocks%d' % from_shard_id
	cursor.execute('SELECT COUNT(*) FROM %s' % (name))
	l = cursor.fetchone()
	db.close()
	return l[0]