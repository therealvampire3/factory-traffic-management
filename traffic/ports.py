from abc import ABC, abstractmethod


class ControllerPort(ABC):
    """Port towards the physical signal controller. Implemented by the REST simulator now,
    and by an MQTT adapter later (publish `send()` payload, call the registered callback
    on each ack/status message). The domain engine never sees this interface: it only
    emits command dicts, which the application layer hands to the port."""

    @abstractmethod
    def send(self, command: dict) -> None:
        """Deliver (or re-deliver) a command. Must not block. Delivery != execution."""

    def set_event_callback(self, callback) -> None:
        """callback(event: dict) is invoked for acks / status events from the device."""
        self._callback = callback
