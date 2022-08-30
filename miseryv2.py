from __future__ import annotations

import json
import signal
import base64
import math
from typing import List, Set, Tuple, Dict, Optional, Union
import requests

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
		self.service = bmemcached.DistributedClient
	
	def memcachierStartup(self):
		# noinspection PyTypeChecker
		self.service = bmemcached.DistributedClient([self.endpoint], self.username, self.password)
		print("Memcachier startup")
		return self
	
	async def read(self, key):
		# noinspection PyTypeChecker
		self.service.get(self.service, key)
	
	async def write(self, key, value):
		# noinspection PyTypeChecker
		self.service.set(self.service, key, value)


class Firestore:
	"""
	This class should abstract away IDs and utilize assembly/disassembly while operating.
	
	The storage data structure uses the following pattern:
	GUILD_NAME/COMMAND/SPECIFIC_NAME/ID%DATA where DATA is a field and everything before it will be parsed as a relative path.
	Every whitespace in names should be converted to _ and the relative paths should be defined as /.
	
	We have a collection of guilds, a guild being a document. The document has a subcollection of commands, each command is a document.
	Specific name is a subcollection, and each data part is ordered by its name, the ID.
	db.collection(u"Guilds").document(u"ServerName").collection(u"Commands").document(u"Sticker").collection(u"TouchSomeGrass").document(u"1")
	"""
	
	def __init__(self):
		self.json = base64.urlsafe_b64decode(os.getenv("FIRESTORE") + "==").decode("utf-8")
		self.startup = self.firestoreStartup
		self.service = firebase_admin.firestore.firestore.Client
	
	def firestoreStartup(self):
		js = json.loads(self.json)
		cred = credentials.Certificate(js)
		firebase_admin.initialize_app(cred)
		self.service = firestore.client()
		print("Firestore startup")
		return self
	
	async def read(self, key):
		rkey = key.split("%")[0]
		document = self.service.document(rkey).get()
		if "%" in key:
			return document.to_dict()[rkey[1]]
		return document.to_dict()
	
	async def write(self, key, value, *, merge = True):
		"""write('Misery/Config%test_key', 'test_value') -> write('Misery/Config', {'test_key': 'test_value'})"""
		rkey = key.split("%")[0]
		if "%" in key:
			data = {rkey[1]: value}
		else:
			data = value
		self.service.document(key).set(document_data = data, merge = merge)


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


class MiseryBot:
	"""The persistent bot superstructure for environment variable bindings, caching and management. Command behavior will be defined somewhere in this class as well as slash command bindings."""
	
	def __init__(self, *, m_firestore: Firestore, m_memcachier: Memcachier):
		self.config = {"Firestore": m_firestore, "Memcachier": m_memcachier}
		self.startup = self.miseryStartup
	
	def miseryStartup(self):
		def dispatch_deploy_startup(bot_instance: MiseryBot, fs: Firestore):
			bot_instance.config["dispatch_deploy_endpoint"] = fs.read("Misery/Config%dispatch_deploy_endpoint")
			token = os.getenv("DISPATCH_DEPLOY")
			bot_instance.config["dispatch_deploy_header"] = {"Accept": "application/vnd.github.everest-preview+json", "Authorization": f"token {token}"}
			bot_instance.config["dispatch_deploy_data"] = {"event_type": "deploy"}
		
		dispatch_deploy_startup(self, self.config["Firestore"])
		return self
	
	def dispatch_deploy(self):
		requests.post(self.config["dispatch_deploy_endpoint"], headers = self.config["dispatch_deploy_header"], json = self.config["dispatch_deploy_data"])
	
	
def main():
	mc = Memcachier().startup()
	fs = Firestore().startup()
	client = nextcord.Client(intents = nextcord.Intents.all())
	oauth = os.getenv("MISERYBOT_OAUTH_KEY")
	misery = MiseryBot(m_firestore = fs, m_memcachier = mc).startup()
	
	@client.event
	async def on_ready():
		print('We have logged in as {0.user}'.format(client))
	
	@client.event
	async def on_message(message: nextcord.Message):
		if message.author == client.user:
			return
		
		if message.content == "ping":
			await message.channel.send("pong")
		
		if message.content == "misery shutdown" and message.author.id == 295951409239556096:
			await message.channel.send("Shutting down")
			await client.close()
			signal.raise_signal(signal.SIGINT)
		
		if message.content == "misery dispatch deploy":
			misery.dispatch_deploy()
			await message.channel.send("Deploy request sent")
	
	client.run(oauth)


if __name__ == "__main__":
	main()
