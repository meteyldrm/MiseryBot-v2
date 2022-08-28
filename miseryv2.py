from __future__ import annotations

import json
import signal
import base64
import math
from typing import List, Set, Tuple, Dict, Optional, Union

import nextcord
import bmemcached
import os

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


class Memcachier:
	def __init__(self):
		self.endpoint = os.getenv("MEMCACHIER_ENDPOINT")
		self.username = os.getenv("MEMCACHIER_USERNAME")
		self.password = os.getenv("MEMCACHIER_PASSWORD")
		self.startup = self.memcachierStartup
		self.service = None
	
	def memcachierStartup(self):
		self.service = bmemcached.DistributedClient([self.endpoint], self.username, self.password)
		print("Memcachier startup")
		return self
	
	async def read(self, key):
		pass
	
	async def write(self, key, value):
		pass


class Firestore:
	"""
	This class should abstract away IDs and utilize assembly/disassembly while operating.
	
	The data structure uses the following pattern:
	GUILDNAME_COMMAND_SPECIFICNAME_ID
	
	We have a collection of guilds, a guild being a document. The document has a subcollection of commands, each command is a document.
	Specific name is a subcollection, and each data part is ordered by its name, the ID.
	db.collection(u"Guilds").document(u"ServerName").collection(u"Commands").document(u"Sticker").collection(u"TouchSomeGrass").document(u"1")
	"""
	
	def __init__(self):
		self.json = base64.urlsafe_b64decode(os.getenv("FIRESTORE") + "==").decode("utf-8")
		self.startup = self.firestoreStartup
		self.service = None
	
	def firestoreStartup(self):
		js = json.loads(self.json)
		cred = credentials.Certificate(js)
		firebase_admin.initialize_app(cred)
		self.service = firestore.client()
		print("Firestore startup")
		return self
	
	async def read(self, key):
		pass
	
	async def write(self, key, value):
		pass


class DataPartition:
	@staticmethod
	def assemble(data: dict) -> bytearray:
		dat = bytearray()
		for k, v in dict(sorted(data.items())).items():
			dat.extend(v)
		return dat
	
	@staticmethod
	def disassemble(data: Union[bytes, bytearray], *, partition_limit = 900000) -> [bytearray]:
		qdata = data
		if isinstance(data, bytes):
			qdata = bytearray(data)
		dat = {}
		len_data = len(qdata)
		temp_bytearray = bytearray()
		p_count = 0
		for limit in range(len_data):
			if int(limit) % partition_limit == 0 and limit != 0:
				dat[str(p_count)] = temp_bytearray
				temp_bytearray = bytearray()
				p_count += 1
			temp_bytearray.append(qdata[limit])
		if len(temp_bytearray) > 0:
			dat[str(p_count)] = temp_bytearray
		return dat


mc = Memcachier().startup()
fs = Firestore().startup()
client = nextcord.Client(intents=nextcord.Intents.all())
oauth = os.getenv("MISERYBOT_OAUTH_KEY")


@client.event
async def on_ready():
	print('We have logged in as {0.user}'.format(client))


@client.event
async def on_message(message: nextcord.Message):
	if message.author == client.user:
		return
	
	msg = str(message.content).lstrip().rstrip()
	_msg_channel = str(message.channel.id)
	
	if message.content == "ping":
		await message.channel.send("pong")
	
	if message.content == "misery shutdown" and message.author.id == 295951409239556096:
		await message.channel.send("Shutting down")
		await client.close()
		signal.raise_signal(signal.SIGINT)

client.run(oauth)
