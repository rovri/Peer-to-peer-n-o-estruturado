from utils import parse_address, address_to_string

class Message:
    def __init__(self, message: str, sender: str | None = None):
        self.sender = sender
        parts = message.split()
        self.origin, self.seqno, self.ttl, self.operation, *self.args = (
            parse_address(parts[0]),
            int(parts[1]),
            int(parts[2]),
            parts[3].upper(),
            parts[4:]
        )

    def __str__(self) -> str:
        msg = f"{address_to_string(self.origin)} {self.seqno} {self.ttl} {self.operation}"
        return ' '.join([msg] + self.args)

    def create_response(self) -> str:
        return f"{address_to_string(self.origin)} {self.seqno} 1 {self.operation}_OK"
    
    def to_unique_tuple(self) -> tuple[str, int, str]:
        key = f"SEARCH_{self.args[0]}" if self.operation == "SEARCH" else self.operation
        return address_to_string(self.origin), self.seqno, key
    
    def forward_message(self, new_last_hop_ip: tuple[str, int]) -> 'Message':
        mode, _, key, hop_count = self.args
        hop_count = int(hop_count) + 1
        return Message(
            f"{address_to_string(self.origin)} {self.seqno} {self.ttl - 1} {self.operation} {mode} {new_last_hop_ip[1]} {key} {hop_count}"
        )

    def is_ttl_expired(self) -> bool:
        return self.ttl <= 0
