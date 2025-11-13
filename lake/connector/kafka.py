from lake.connector.core import DuckLakeManager
from lake.util.logger import logger
from duckdb import DuckDBPyConnection
from confluent_kafka import Consumer,KafkaException,KafkaError
from collections.abc import Generator
import json
import pandas as pd
from typing import Optional


class Connector(DuckLakeManager):
	bootstrap_servers: str = None
	base_config: dict = None
	consumer_config:dict = None
	_consumers: list[Consumer] = []
	def __init__(self,config_path):
		super(Connector,self).__init__(config_path)
		self.duckdb_connection.execute(f"use {self.Lake.DEST.catalog.lake_alias};")
		"""Initialize the Kafka client."""
		self.bootstrap_servers = self.BrokerCnn.url
		self.base_config = {"bootstrap.servers": self.bootstrap_servers}
		self.consumer_config = {**self.base_config,
						   "auto.offset.reset": "earliest",
						   "enable.auto.commit": False,
						   'max.poll.interval.ms': 600000,
						   'auto.offset.reset': 'earliest',
						   'heartbeat.interval.ms': 600000
						   }
		self._consumers: list[Consumer] = []
		logger.warning(f"initializing kafka client {self.BrokerCnn.url} topics={self.BrokerCnn.ingest_topics} group={self.BrokerCnn.group_id}")

	@property
	def consumers(self) -> list[Consumer]:
		"""Get the list of Kafka consumers."""
		return self._consumers

	@consumers.setter
	def consumers(self, consumers: list[Consumer]) -> None:
		"""Set the list of Kafka consumers."""
		logger.warning(f"Cannot set {consumers} for consumers attribute, as it's read-only")

	def on_assign_seek_to_beginning(self,consumer, partitions):
		"""
		This function is called when partitions are assigned to the consumer.
		It will seek to the beginning of each assigned partition.
		"""
		print("Partitions assigned, seeking to the beginning...")
		for partition in partitions:
			# Set the offset to the beginning of the partition
			partition.offset = 0
		# The consumer.assign() call is what actually applies the new assignments
		consumer.assign(partitions)
	def open_consumer(self, group: str, topics: list[str]) -> Consumer | None:
		"""Open a Kafka consumer."""
		try:
			consumer = Consumer({**self.consumer_config, "group.id": group})
			self._consumers.append(consumer)
			if consumer:
				consumer.subscribe(topics, on_assign=self.on_assign_seek_to_beginning)
				# consumer.subscribe(topics)
				logger.info(f"Opened Kafka consumer to broker at {self.bootstrap_servers} listening to {topics=}")
		except KafkaException as e:
			logger.error(f"Failed to open consumer to broker at {self.bootstrap_servers} listening to {topics=}: {e}")
			return None
		else:
			return consumer

	def consume_messages(self, consumer: Consumer, timeout: float = 4.0) -> Generator[str] | None:
		"""Continuously consume messages from Kafka."""
		if not consumer or len(self._consumers) == 0:
			logger.error(f"Kafka consumer to broker at {self.bootstrap_servers} is not open")
			return None
		try:
			while True:
				msg = consumer.poll(timeout=timeout)
				if msg is None:
					continue
				topic, partition, key, value, offset = (
					msg.topic(),
					msg.partition(),
					msg.key(),
					msg.value().decode("utf-8"),
					msg.offset(),
				)
				logger.warning(f"{topic}|  OFFSET:{offset}")
				if msg.error():
					if msg.error().code() == KafkaError._PARTITION_EOF:  # noqa: SLF001
						logger.warning(f"Reached end of {partition=} in {topic=}")
					else:
						logger.error(f"Failed to consume message from {partition=}, {topic=}, {key=}: {msg.error()}")
				else:
					logger.info(
						f"Consumed message with {key=}, {value=} from {topic=}, {partition=}, {offset=} successfully",
					)
					consumer.commit(msg)
					yield value
		except KafkaException as e:
			logger.error(f"Failed to consume messages: {e}")
		except KeyboardInterrupt:
			logger.info("Consumer loop interrupted by user")
	def consume_batch(
    self,
    consumer: Consumer,
    timeout: float = 10.0,
    batch_size: int = 10000
	) -> Optional[Generator[pd.DataFrame, None, None]]:
		if not consumer or len(self._consumers) == 0:
			logger.error(f"Kafka consumer to broker at {self.bootstrap_servers} is not open")
			return None

		try:
			while True:
				message_batch = consumer.consume(num_messages=batch_size, timeout=timeout)
				if message_batch is None:
					continue

				valid_messages = []
				for msg in message_batch:
					if msg is None:
						continue
					if msg.error():
						if msg.error().code() == KafkaError._PARTITION_EOF:
							continue
						else:
							logger.error(f"Kafka error received: {msg.error()}")
							continue

					try:
						loaded_msg = json.loads(msg.value())
						valid_messages.append(loaded_msg)
					except Exception as fail:
						logger.critical(f"failed to collect message bytes: {msg.value()} {fail}")
						continue
					# consumer.commit(msg)  # enable if you want manual commits

				if not valid_messages or len(valid_messages) < 2:
					logger.warning("not enough messages gathered in this poll session (continue collecting...)")
					continue

				# Flatten nested JSON; use pd.DataFrame(valid_messages) if you don't want flattening
				df = pd.json_normalize(valid_messages, sep='.')
				yield df

		except KafkaException as e:
			logger.error(f"Failed to consume messages: {e}")
		except KeyboardInterrupt:
			logger.info("Consumer loop interrupted by user")

	def close_consumer(self, consumer: Consumer) -> bool:
		"""
		Close the Kafka consumer.
		Returns true if the consumer is closed successfully.
		"""
		if consumer and consumer in self._consumers:
			consumer.unsubscribe()
			consumer.close()
			self._consumers.remove(consumer)
			logger.info(f"Kafka consumer on broker at {self.bootstrap_servers} closed successfully.")
		return consumer not in self._consumers
	
	def infer_type(self,value):
		"""Infer PostgreSQL data type based on Python value"""
		if isinstance(value, int):
			return 'INTEGER'
		elif isinstance(value, float):
			return 'FLOAT'
		elif isinstance(value, str):
			return 'VARCHAR'  # or TEXT based on your requirements
		elif isinstance(value, bool):
			return 'BOOLEAN'
		elif isinstance(value, list):
			return 'JSONB'  # Handle arrays or lists as JSONB
		elif isinstance(value, dict):
			return 'JSONB'  # Handle objects as JSONB
		else:
			return 'TEXT'  # Fallback type


	def template_adapter(self,consumer):
		while True:
			msg = consumer.poll(timeout=4.0)
			if msg is None:
				continue
			else:
				sample_msg = json.loads(msg.value().decode('utf-8'))
				logger.info(f"sample message value= ({sample_msg})")
				columns = []
				for key, value in sample_msg.items():
					pg_type = self.infer_type(value)
					columns.append(f"{key} {pg_type}")
				columns_sql = ', '.join(columns)
				create_statement = f"CREATE TABLE IF NOT EXISTS {self.BrokerCnn.ingest_table} ({columns_sql});"
				table_generate_result = self.duckdb_connection.sql(create_statement)
				print(self.duckdb_connection.table(self.BrokerCnn.ingest_table).show())
				logger.info(f"{create_statement} Returned -> {table_generate_result}")
				return
	
	def attach(self):
		consumer = self.open_consumer(self.BrokerCnn.group_id,self.BrokerCnn.ingest_topics)
		self.template_adapter(consumer)
		try:
			for messages_frame in self.consume_batch(consumer,batch_size=self.BrokerCnn.batch_size):
				logger.warning(f"inserting new frame ({messages_frame.shape}) into {self.BrokerCnn.ingest_table}")
				logger.info(messages_frame)
				self.duckdb_connection.sql(f"INSERT INTO {self.BrokerCnn.ingest_table} (SELECT * FROM messages_frame)")
		finally:
			self.close_consumer(consumer)
	def single_message(self,batch_size:int):
		consumer = self.open_consumer(self.BrokerCnn.group_id,self.BrokerCnn.ingest_topics)
		self.template_adapter(consumer)
		try:
			for message in self.consume_messages(consumer):
				data = json.loads(message)
				keys = ', '.join(data.keys())
				placeholders = ', '.join(['?'] * len(data)) 
				values = tuple(data.values())
				insert_statement = f"INSERT INTO {self.BrokerCnn.ingest_table} ({keys}) VALUES ({placeholders})"
				insert_result = self.duckdb_connection.execute(insert_statement, values)
				logger.info(f"{insert_statement} Results-> {insert_result.fetchall()}")
		finally:
			self.close_consumer(consumer)
	def exec(self,cmd:str):
		return self.duckdb_connection.execute(cmd)