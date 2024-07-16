from argparse import ArgumentParser
import asyncio
import socket
import random
import sys

def parse_address(addr_string: str) -> tuple[str, int]:
    host, port = addr_string.split(':')
    info = socket.getaddrinfo(host, int(port), family=socket.AF_INET)
    return info[0][4][0], info[0][4][1]

def address_to_string(addr: tuple[str, int]) -> str:
    return f"{addr[0]}:{addr[1]}"

def split_key_value(entry: str) -> tuple[str, str]:
    return entry.split()

async def get_async_input(prompt: str = "") -> str:
    return await asyncio.to_thread(input, prompt)

def log(*args, **kwargs):
    kwargs.setdefault('file', sys.stderr)
    print(*args, **kwargs)

async def request_key():
    return await get_async_input("Enter the key to search for:\n")

class Packet:
    def __init__(self, data: str, sender: str | None = None):
        self.sender = sender
        parts = data.split()
        self.source = parse_address(parts[0])
        self.sequence = int(parts[1])
        self.time_to_live = int(parts[2])
        self.action = parts[3].upper()
        self.parameters = parts[4:]

    def __str__(self) -> str:
        return ' '.join([f"{address_to_string(self.source)} {self.sequence} {self.time_to_live} {self.action}", *self.parameters])

    def generate_response(self) -> str:
        return f"{address_to_string(self.source)} {self.sequence} 1 {self.action}_OK"

    def get_unique_id(self) -> tuple[str, int, str]:
        action_key = f"{self.action}_{self.parameters[0]}" if self.action == "SEARCH" else self.action
        return address_to_string(self.source), self.sequence, action_key

    def forward_search(self, new_hop: tuple[str, int]) -> 'Packet':
        mode, _, key, hop_count = self.parameters
        new_hop_count = int(hop_count) + 1
        return Packet(f"{address_to_string(self.source)} {self.sequence} {self.time_to_live - 1} {self.action} {mode} {new_hop[1]} {key} {new_hop_count}")

    def should_discard(self) -> bool:
        if self.time_to_live <= 0:
            print("Time to live reached zero, discarding packet")
            return True
        return False

class Metrics:
    def __init__(self):
        self.flood_count = 0
        self.random_walk_count = 0
        self.depth_first_count = 0
        self.flood_hops = 0
        self.flood_hops_squared = 0
        self.flood_samples = 0
        self.random_walk_hops = 0
        self.random_walk_hops_squared = 0
        self.random_walk_samples = 0
        self.depth_first_hops = 0
        self.depth_first_hops_squared = 0
        self.depth_first_samples = 0

    def update_stats(self, hops: int, attr_prefix: str):
        setattr(self, f"{attr_prefix}_hops", getattr(self, f"{attr_prefix}_hops") + hops)
        setattr(self, f"{attr_prefix}_hops_squared", getattr(self, f"{attr_prefix}_hops_squared") + hops * hops)
        setattr(self, f"{attr_prefix}_samples", getattr(self, f"{attr_prefix}_samples") + 1)

    def update_flood_stats(self, hops: int):
        self.update_stats(hops, 'flood')

    def update_random_walk_stats(self, hops: int):
        self.update_stats(hops, 'random_walk')

    def update_depth_first_stats(self, hops: int):
        self.update_stats(hops, 'depth_first')

    def compute_stats(self, samples, total_hops, total_hops_squared) -> str:
        if samples == 0:
            return "N/A"
        mean = total_hops / samples
        variance = (total_hops_squared / samples) - (mean * mean)
        std_dev = variance ** 0.5
        return f"{mean:.3f} (std dev {std_dev:.3f})"

    def get_flood_stats(self) -> str:
        return self.compute_stats(self.flood_samples, self.flood_hops, self.flood_hops_squared)

    def get_random_walk_stats(self) -> str:
        return self.compute_stats(self.random_walk_samples, self.random_walk_hops, self.random_walk_hops_squared)

    def get_depth_first_stats(self) -> str:
        return self.compute_stats(self.depth_first_samples, self.depth_first_hops, self.depth_first_hops_squared)

class Peer:
    def __init__(self, address: str, neighbors_file: str = None, keys_file: str = None):
        self.address = parse_address(address)
        self.neighbors = []
        self.data = {}
        self.sequence = 1
        self.default_ttl = 100
        self.processed = set()
        self.metrics = Metrics()
        self.parent = None
        self.unexplored = []
        self.current = None

    def increment_sequence(self) -> int:
        self.sequence += 1
        return self.sequence - 1

    async def init_neighbors(self, neighbors_file: str):
        with open(neighbors_file, 'r') as file:
            origin = address_to_string(self.address)
            packet = Packet(f"{origin} {self.increment_sequence()} 1 HELLO")
            for line in file.readlines():
                neighbor = parse_address(line.strip())
                print(f"Attempting to add neighbor {address_to_string(neighbor)}")
                if await self.transmit(neighbor, packet):
                    print(f"\tAdding neighbor to list: {address_to_string(neighbor)}")
                    self.neighbors.append(neighbor)

    async def init_data(self, keys_file: str):
        with open(keys_file, 'r') as file:
            for line in file.readlines():
                key, value = split_key_value(line)
                print(f"Adding pair ({key}, {value}) to local data")
                self.data[key] = value

    def has_key(self, key: str) -> bool:
        if key in self.data:
            print("Key found!")
            return True
        return False

    def get_local_key(self, key: str) -> bool:
        if self.has_key(key):
            print("Value in local data!")
            print(f"\tkey: {key} value: {self.data[key]}")
            return True
        return False

    def remove_unexplored(self, node: tuple[str, int]):
        if node in self.unexplored:
            self.unexplored.remove(node)

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        data = await reader.readline()
        addr: str = writer.get_extra_info("peername")[0]
        packet = Packet(data.decode(), addr)

        print(f"Received packet: \"{packet}\"")
        asyncio.create_task(self.process_packet(packet))

        writer.write(packet.generate_response().encode())
        await writer.drain()

        writer.close()
        await writer.wait_closed()

    async def process_packet(self, packet: Packet):
        if packet.action == "HELLO":
            await self.handle_hello(packet)
        elif packet.action == "SEARCH":
            if packet.parameters[0] == "FL":
                await self.handle_flood_search(packet)
            elif packet.parameters[0] == "RW":
                await self.handle_random_walk_search(packet)
            elif packet.parameters[0] == "BP":
                await self.handle_depth_first_search(packet)
        elif packet.action == "VAL":
            await self.handle_value_response(packet)
        elif packet.action == "BYE":
            await self.handle_bye(packet)
        else:
            log(f"Unknown action: \"{packet.action}\"")

    async def handle_hello(self, packet: Packet):
        if packet.source in self.neighbors:
            print(f"\tNeighbor {address_to_string(packet.source)} already in list")
        else:
            self.neighbors.append(packet.source)
            print(f"\tAdding neighbor to list: {address_to_string(packet.source)}")

    async def handle_flood_search(self, packet: Packet):
        _, last_hop_port, key, hop_count = packet.parameters
        last_hop = packet.sender, int(last_hop_port)
        if packet.get_unique_id() in self.processed:
            print("Flooding: Duplicate packet!")
            return
        self.processed.add(packet.get_unique_id())
        self.metrics.flood_count += 1
        if self.has_key(key):
            reply = Packet(f"{address_to_string(self.address)} {self.increment_sequence()} 1 VAL FL {key} {self.data[key]} {int(hop_count)}")
            await self.transmit(packet.source, reply)
            return
        forward = packet.forward_search(self.address)
        if forward.should_discard():
            return
        for neighbor in self.neighbors:
            if neighbor == last_hop:
                continue
            await self.transmit(neighbor, forward)

    async def handle_random_walk_search(self, packet: Packet):
        _, last_hop_port, key, hop_count = packet.parameters
        last_hop = packet.sender, int(last_hop_port)
        self.processed.add(packet.get_unique_id())
        self.metrics.random_walk_count += 1
        if self.has_key(key):
            reply = Packet(f"{address_to_string(self.address)} {self.increment_sequence()} 1 VAL RW {key} {self.data[key]} {int(hop_count)}")
            await self.transmit(packet.source, reply)
            return
        forward = packet.forward_search(self.address)
        if forward.should_discard():
            return
        options = [n for n in self.neighbors if n != last_hop]
        next_hop = random.choice(options) if options else last_hop
        await self.transmit(next_hop, forward)

    async def handle_depth_first_search(self, packet: Packet):
        _, last_hop_port, key, hop_count = packet.parameters
        last_hop = packet.sender, int(last_hop_port)
        self.metrics.depth_first_count += 1
        if self.has_key(key):
            reply = Packet(f"{address_to_string(self.address)} {self.increment_sequence()} 1 VAL BP {key} {self.data[key]} {int(hop_count)}")
            await self.transmit(packet.source, reply)
            return
        forward = packet.forward_search(self.address)
        if forward.should_discard():
            return
        if packet.get_unique_id() not in self.processed:
            log(f"\033[33mPacket {packet.get_unique_id()} not yet processed\033[m")
            self.parent = last_hop
            self.unexplored = [*self.neighbors]
            self.current = None
        self.remove_unexplored(last_hop)
        self.processed.add(packet.get_unique_id())
        if self.parent == self.address and self.current == last_hop and not self.unexplored:
            print(f"BP: Unable to locate key {key}")
            return
        if self.current is not None and self.current != last_hop:
            print("BP: cycle detected, returning message...")
            await self.transmit(last_hop, forward)
        elif not self.unexplored:
            print("BP: no neighbor found the key, backtracking...")
            await self.transmit(self.parent, forward)
        else:
            self.current = self.unexplored.pop()
            await self.transmit(self.current, forward)

    async def handle_value_response(self, packet: Packet):
        mode, key, value, hop_count = packet.parameters
        print("\tValue found!")
        print(f"\t\tKey: {key} value: {value}")
        if mode == "FL":
            self.metrics.update_flood_stats(int(hop_count))
        elif mode == "RW":
            self.metrics.update_random_walk_stats(int(hop_count))
        elif mode == "BP":
            self.metrics.update_depth_first_stats(int(hop_count))

    async def handle_bye(self, packet: Packet):
        if packet.source in self.neighbors:
            self.neighbors.remove(packet.source)
            print(f"\tRemoving neighbor from list: {address_to_string(packet.source)}")

    async def start_server(self):
        addr, port = self.address
        server = await asyncio.start_server(self.handle_connection, addr, port)
        async with server:
            await server.serve_forever()

    async def show_menu(self):
        while True:
            print(
                "\n"
                "Choose an option:\n"
                "\t[0] List neighbors\n"
                "\t[1] HELLO\n"
                "\t[2] SEARCH (flooding)\n"
                "\t[3] SEARCH (random walk)\n"
                "\t[4] SEARCH (depth-first)\n"
                "\t[5] Statistics\n"
                "\t[6] Change default TTL\n"
                "\t[9] Exit"
            )
            choice = (await get_async_input()).strip()
            if choice == '0':
                await self.show_neighbors()
            elif choice == '1':
                await self.send_hello()
            elif choice == '2':
                await self.start_flood_search()
            elif choice == '3':
                await self.start_random_walk_search()
            elif choice == '4':
                await self.start_depth_first_search()
            elif choice == '5':
                await self.display_stats()
            elif choice == '6':
                await self.set_default_ttl()
            elif choice == '9':
                break
            else:
                log("Error! Invalid option")
        await self.send_bye()

    async def show_neighbors(self):
        print(f"\nThere are {len(self.neighbors)} neighbors:")
        for i, neighbor in enumerate(self.neighbors):
            print(f"\t[{i}] {address_to_string(neighbor)}")

    async def send_hello(self):
        if not self.neighbors:
            log("Error! No neighbors")
            return
        print("\nChoose a neighbor:")
        await self.show_neighbors()
        choice = int(await get_async_input())
        if choice < 0 or choice >= len(self.neighbors):
            log("Error! Invalid neighbor")
            return
        target = self.neighbors[choice]
        origin = address_to_string(self.address)
        packet = Packet(f"{origin} {self.increment_sequence()} 1 HELLO")
        await self.transmit(target, packet)

    async def start_flood_search(self):
        key = await request_key()
        if self.get_local_key(key):
            return
        origin = address_to_string(self.address)
        port = self.address[1]
        packet = Packet(f"{origin} {self.increment_sequence()} {self.default_ttl} SEARCH FL {port} {key} 1")
        self.processed.add(packet.get_unique_id())
        for neighbor in self.neighbors:
            await self.transmit(neighbor, packet)

    async def start_random_walk_search(self):
        key = await request_key()
        if self.get_local_key(key):
            return
        origin = address_to_string(self.address)
        port = self.address[1]
        packet = Packet(f"{origin} {self.increment_sequence()} {self.default_ttl} SEARCH RW {port} {key} 1")
        self.processed.add(packet.get_unique_id())
        neighbor = random.choice(self.neighbors)
        await self.transmit(neighbor, packet)

    async def start_depth_first_search(self):
        key = await request_key()
        if self.get_local_key(key):
            return
        origin = address_to_string(self.address)
        port = self.address[1]
        packet = Packet(f"{origin} {self.increment_sequence()} {self.default_ttl} SEARCH BP {port} {key} 1")
        self.processed.add(packet.get_unique_id())
        self.parent = self.address
        self.unexplored = [*self.neighbors]
        self.current = self.unexplored.pop()
        await self.transmit(self.current, packet)

    async def display_stats(self):
        print("Statistics:") 
        print(f"\tTotal flood messages seen: {self.metrics.flood_count}")
        print(f"\tTotal random walk messages seen: {self.metrics.random_walk_count}")
        print(f"\tTotal depth-first messages seen: {self.metrics.depth_first_count}")
        print(f"\tAverage hops to find destination by flooding: {self.metrics.get_flood_stats()}")
        print(f"\tAverage hops to find destination by random walk: {self.metrics.get_random_walk_stats()}")
        print(f"\tAverage hops to find destination by depth-first: {self.metrics.get_depth_first_stats()}")

    async def set_default_ttl(self):
        self.default_ttl = int(await get_async_input("\nEnter new TTL value\n"))
        if self.default_ttl < 1:
            log("Error! TTL must be greater than 0")

    async def send_bye(self):
        origin = address_to_string(self.address)
        packet = Packet(f"{origin} {self.increment_sequence()} 1 BYE")
        for neighbor in self.neighbors:
            await self.transmit(neighbor, packet)

    async def transmit(self, address: tuple[str, int], packet: Packet) -> bool:
        addr, port = address
        print(f"Forwarding packet \"{packet}\" to {address_to_string(address)}")
        try:
            reader, writer = await asyncio.open_connection(addr, port)
            writer.write(f"{packet}\n".encode())
            await writer.drain()

            data = await reader.read(100)
            response = data.decode().strip()

            writer.close()
            await writer.wait_closed()

            response_packet = Packet(response, addr)
            if response_packet.action == f"{packet.action}_OK":
                print(f"\tTransmission successful: \"{packet}\"")
                return True
        except ConnectionRefusedError:
            pass
        print("\tConnection error!")
        return False

def get_arguments() -> tuple[str, str, str]:
    parser = ArgumentParser()
    parser.add_argument(
        "address", type=str,
        help="This peer's IP address in the format <addr>:<port>"
    )
    parser.add_argument(
        "neighbors_file", type=str, nargs='?', default=None,
        help="Path to the neighbors file (optional)"
    )
    parser.add_argument(
        "keys_file", type=str, nargs='?', default=None,
        help="Path to the keys file (optional)"
    )
    args = parser.parse_args()
    return args.address, args.neighbors_file, args.keys_file

async def run() -> int:
    address, neighbors, keys = get_arguments()
    peer = Peer(address, neighbors, keys)
    asyncio.create_task(peer.start_server())
    print(f"Server started: {address_to_string(peer.address)}")

    print()

    if neighbors:
        await peer.init_neighbors(neighbors)

    print()

    if keys:
        await peer.init_data(keys)

    await peer.show_menu()
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
