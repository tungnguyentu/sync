from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional


class PublisherInterface(ABC):
    @abstractmethod
    def publish(
        self,
        event_type: str,
        payload: Dict[str, Any],
        key: Optional[str] = None,
        issued_at: Optional[datetime] = None,
    ):
        ...
