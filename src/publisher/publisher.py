import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import kafka as kafka_client

from src.publisher.base import PublisherInterface
from src.utils import datetime_json_serial, synchronous_retry


class KafkaPublisher(PublisherInterface):
    def __init__(
        self,
        bootstrap_servers,
        topic,
        acks="all",
        logger=None,
        retries=3,
        delay=1,
        sasl_plain_username: Optional[str] = None,
        sasl_plain_password: Optional[str] = None,
        security_protocol: Optional[str] = None,
        sasl_mechanism: Optional[str] = None,
        ssl_context: Optional[Any] = None,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._acks = acks
        self._topic = topic
        self._retries = retries
        self._delay = delay
        self.sasl_plain_username = sasl_plain_username
        self.sasl_plain_password = sasl_plain_password
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.ssl_context = ssl_context
        self._value_serializer = lambda x: json.dumps(
            x, default=datetime_json_serial
        ).encode("utf-8")
        self._logger = logger
        if not logger:
            self._logger = logging.getLogger(__name__)
        self._publisher = None

    def connect_publisher(self):
        @synchronous_retry(retries=self._retries, delay=self._delay)
        def _connect():
            self._logger.info("Connecting to %s", self._bootstrap_servers)
            config = dict(
                bootstrap_servers=self._bootstrap_servers,
                value_serializer=self._value_serializer,
                acks=self._acks,
            )
            if self.security_protocol:
                config.update(
                    dict(
                        sasl_plain_username=self.sasl_plain_username,
                        sasl_plain_password=self.sasl_plain_password,
                        security_protocol=self.security_protocol,
                        ssl_context=self.ssl_context,
                        sasl_mechanism=self.sasl_mechanism,
                    )
                )
            self._publisher = kafka_client.KafkaProducer(**config)

        try:
            return _connect()
        except kafka_client.errors.KafkaError as e:
            self._logger.exception("Could not initialize publisher: %s", e)
            raise e

    def publish(
        self, event_type: str, payload: Dict[str, Any], key: Optional[str] = ""
    ):
        issued_at = datetime.now(tz=timezone.utc)
        message = dict(
            event_type=event_type or "",
            payload=payload or {},
            issued_at=issued_at,
        )

        @synchronous_retry(
            retries=self._retries,
            delay=self._delay
        )
        def _publish():
            self._logger.info("Publishing message: %s", message)
            self._publisher.send(
                topic=self._topic, key=bytes(key, "utf-8"), value=message
            )
            self._publisher.flush()

        try:
            return _publish()
        except Exception as e:
            self._logger.exception("Send message failed - %s - %s", message, e)
            raise e
