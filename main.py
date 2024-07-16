from argparse import ArgumentParser
import asyncio
import socket
import random
import sys

def parse_address(addr_str: str) -> tuple[str, int]:
    host, port = addr_str.split(':')
    info = socket.getaddrinfo(host, int(port), family=socket.AF_INET)[0]
    return info[4][0], info[4][1]

def format_address(addr: tuple[str, int]) -> str:
    return f"{addr[0]}:{addr[1]}"

def split_entry(entry: str) -> tuple[str, str]:
    return tuple(entry.split())

async def get_async_input(prompt: str = "") -> str:
    return await asyncio.to_thread(input, prompt)

def log(*args, **kwargs):
    kwargs.setdefault('file', sys.stderr)
    print(*args, **kwargs)

async def key_prompt():
    return await get_async_input("Digite a chave a ser buscada\n")

class Packet:
    def __init__(self, msg: str, sender: str | None = None):
        self.sender = sender
        parts = msg.split()
        self.origin = parse_address(parts[0])
        self.seqno = int(parts[1])
        self.ttl = int(parts[2])
        self.operation = parts[3].upper()
        self.args = parts[4:]

    def __str__(self) -> str:
        return ' '.join([format_address(self.origin), str(self.seqno), str(self.ttl), self.operation] + self.args)

    def reply(self) -> str:
        return f"{format_address(self.origin)} {self.seqno} 1 {self.operation}_OK"

    def get_unique_id(self) -> tuple[str, int, str]:
        op_key = f"{self.operation}_{self.args[0]}" if self.operation == "SEARCH" else self.operation
        return format_address(self.origin), self.seqno, op_key

    def forward_search(self, new_hop: tuple[str, int]) -> 'Packet':
        mode, _, key, hop_count = self.args
        new_hop_count = int(hop_count) + 1
        return Packet(f"{format_address(self.origin)} {self.seqno} {self.ttl - 1} {self.operation} {mode} {new_hop[1]} {key} {new_hop_count}")

    def should_discard(self) -> bool:
        if self.ttl <= 0:
            print("TTL igual a zero, descartando mensagem")
            return True
        return False

class Statistics:
    def __init__(self):
        self.counters = {'fl': 0, 'rw': 0, 'bp': 0}
        self.metrics = {
            'fl': {'sum': 0, 'sum_sq': 0, 'count': 0},
            'rw': {'sum': 0, 'sum_sq': 0, 'count': 0},
            'bp': {'sum': 0, 'sum_sq': 0, 'count': 0}
        }

    def increment_counter(self, counter_type: str):
        self.counters[counter_type] += 1

    def add_metric(self, metric_type: str, value: int):
        self.metrics[metric_type]['sum'] += value
        self.metrics[metric_type]['sum_sq'] += value ** 2
        self.metrics[metric_type]['count'] += 1

    def calculate_stats(self, metric_type: str) -> str:
        m = self.metrics[metric_type]
        if m['count'] == 0:
            return "N/A"
        avg = m['sum'] / m['count']
        variance = (m['sum_sq'] / m['count']) - (avg ** 2)
        std_dev = variance ** 0.5
        return f"{avg:.3} (dp {std_dev:.3})"

class Node:
    def __init__(self, address: str, neighbours_file: str = None, keys_file: str = None):
        self.address = parse_address(address)
        self.neighbours = []
        self.keys = {}
        self.seqno = 1
        self.default_ttl = 100
        self.seen = set()
        self.stats = Statistics()
        self.search_state = {'parent': None, 'candidates': [], 'active': None}

    def get_next_seqno(self) -> int:
        self.seqno += 1
        return self.seqno - 1

    async def load_neighbours(self, file_path: str):
        with open(file_path, 'r') as file:
            origin = format_address(self.address)
            hello_msg = Packet(f"{origin} {self.get_next_seqno()} 1 HELLO")
            for line in file:
                neighbour = parse_address(line.strip())
                print(f"Tentando adicionar vizinho {format_address(neighbour)}")
                if await self.send_packet(neighbour, hello_msg):
                    print(f"\tAdicionando vizinho na tabela: {format_address(neighbour)}")
                    self.neighbours.append(neighbour)

    async def load_keys(self, file_path: str):
        with open(file_path, 'r') as file:
            for line in file:
                key, value = split_entry(line)
                print(f"Adicionando par ({key}, {value}) na tabela local")
                self.keys[key] = value

    def has_key(self, key: str) -> bool:
        if key in self.keys:
            print("Chave encontrada!")
            return True
        return False

    def get_local_key(self, key: str) -> bool:
        if self.has_key(key):
            print("Valor na tabela local!")
            print(f"\tchave: {key} valor: {self.keys[key]}")
            return True
        return False

    def remove_candidate(self, candidate: tuple[str, int]):
        if candidate in self.search_state['candidates']:
            self.search_state['candidates'].remove(candidate)

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        data = await reader.readline()
        addr = writer.get_extra_info("peername")[0]
        packet = Packet(data.decode(), addr)

        print(f"Mensagem recebida: \"{packet}\"")
        asyncio.create_task(self.process_packet(packet))

        writer.write(packet.reply().encode())
        await writer.drain()

        writer.close()
        await writer.wait_closed()

    async def process_packet(self, packet: Packet):
        handlers = {
            "HELLO": self.process_hello,
            "SEARCH": {
                "FL": self.process_flooding_search,
                "RW": self.process_random_walk_search,
                "BP": self.process_depth_first_search
            },
            "VAL": self.process_values,
            "BYE": self.process_bye
        }
        
        if packet.operation in handlers:
            if packet.operation == "SEARCH":
                await handlers[packet.operation][packet.args[0]](packet)
            else:
                await handlers[packet.operation](packet)
        else:
            log(f"Unknown operation: \"{packet.operation}\"")

    async def process_hello(self, packet: Packet):
        if packet.origin in self.neighbours:
            print(f"\tVizinho {format_address(packet.origin)} já está na tabela")
        else:
            self.neighbours.append(packet.origin)
            print(f"\tAdicionando vizinho na tabela: {format_address(packet.origin)}")

    async def process_flooding_search(self, packet: Packet):
        mode, last_hop_port, key, hop_count = packet.args
        last_hop = packet.sender, int(last_hop_port)
        if packet.get_unique_id() in self.seen:
            print("Flooding: Mensagem repetida!")
            return
        self.seen.add(packet.get_unique_id())
        self.stats.increment_counter('fl')
        if self.has_key(key):
            reply = Packet(f"{format_address(self.address)} {self.get_next_seqno()} 1 VAL {mode} {key} {self.keys[key]} {int(hop_count)}")
            await self.send_packet(packet.origin, reply)
            return
        forward = packet.forward_search(self.address)
        if forward.should_discard():
            return
        for neighbour in self.neighbours:
            if neighbour != last_hop:
                await self.send_packet(neighbour, forward)

    async def process_random_walk_search(self, packet: Packet):
        mode, last_hop_port, key, hop_count = packet.args
        last_hop = packet.sender, int(last_hop_port)
        self.seen.add(packet.get_unique_id())
        self.stats.increment_counter('rw')
        if self.has_key(key):
            reply = Packet(f"{format_address(self.address)} {self.get_next_seqno()} 1 VAL {mode} {key} {self.keys[key]} {int(hop_count)}")
            await self.send_packet(packet.origin, reply)
            return
        forward = packet.forward_search(self.address)
        if forward.should_discard():
            return
        available_neighbours = [n for n in self.neighbours if n != last_hop]
        next_hop = random.choice(available_neighbours) if available_neighbours else last_hop
        await self.send_packet(next_hop, forward)

    async def process_depth_first_search(self, packet: Packet):
        mode, last_hop_port, key, hop_count = packet.args
        last_hop = packet.sender, int(last_hop_port)
        self.stats.increment_counter('bp')
        if self.has_key(key):
            reply = Packet(f"{format_address(self.address)} {self.get_next_seqno()} 1 VAL {mode} {key} {self.keys[key]} {int(hop_count)}")
            await self.send_packet(packet.origin, reply)
            return
        forward = packet.forward_search(self.address)
        if forward.should_discard():
            return
        if packet.get_unique_id() not in self.seen:
            log(f"\033[33mMensagem {packet.get_unique_id()} ainda vista\033[m")
            self.search_state = {'parent': last_hop, 'candidates': list(self.neighbours), 'active': None}
        self.remove_candidate(last_hop)
        self.seen.add(packet.get_unique_id())
        if self.search_state['parent'] == self.address and self.search_state['active'] == last_hop and not self.search_state['candidates']:
            print(f"BP: Nao foi possivel localizar a chave {key}")
            return
        if self.search_state['active'] is not None and self.search_state['active'] != last_hop:
            print("BP: ciclo detectado, devolvendo a mensagem...")
            await self.send_packet(last_hop, forward)
        elif not self.search_state['candidates']:
            print("BP: nenhum vizinho encontrou a chave, retrocedendo...")
            await self.send_packet(self.search_state['parent'], forward)
        else:
            self.search_state['active'] = self.search_state['candidates'].pop()
            await self.send_packet(self.search_state['active'], forward)

    async def process_values(self, packet: Packet):
        mode, key, value, hop_count = packet.args
        print("\tValor encontrado!")
        print(f"\t\tChave: {key} valor: {value}")
        self.stats.add_metric(mode.lower(), int(hop_count))

    async def process_bye(self, packet: Packet):
        if packet.origin in self.neighbours:
            self.neighbours.remove(packet.origin)
            print(f"\tRemovendo vizinho da tabela: {format_address(packet.origin)}")

    async def start_server(self):
        addr, port = self.address
        server = await asyncio.start_server(self.handle_connection, addr, port)
        async with server:
            await server.serve_forever()

    async def run_menu(self):
        menu_options = {
            '0': self.list_of_neighbours,
            '1': self.send_hello,
            '2': self.new_flooding_search,
            '3': self.new_random_walk_search,
            '4': self.new_depth_first_search,
            '5': self.show_statistics,
            '6': self.change_default_ttl
        }
        while True:
            print(
                "\n"
                "Escolha o comando\n"
                "\t[0] Listar vizinhos\n"
                "\t[1] HELLO\n"
                "\t[2] SEARCH (flooding)\n"
                "\t[3] SEARCH (random walk)\n"
                "\t[4] SEARCH (busca em profundidade)\n"
                "\t[5] Estatisticas\n"
                "\t[6] Alterar valor padrao de TTL\n"
                "\t[9] Sair"
            )
            choice = (await get_async_input()).strip()
            if choice == '9':
                break
            elif choice in menu_options:
                await menu_options[choice]()
            else:
                log("Erro! Opção inválida")
        await self.send_bye()

    async def list_of_neighbours(self):
        print(f"\nHá {len(self.neighbours)} vizinhos na tabela:")
        for index, neighbour in enumerate(self.neighbours):
            print(f"\t[{index}] {format_address(neighbour)}")

    async def send_hello(self):
        if not self.neighbours:
            log("Erro! Não há vizinhos")
            return
        print("\nEscolha o vizinho:")
        await self.list_of_neighbours()
        index = int(await get_async_input())
        if 0 <= index < len(self.neighbours):
            address = self.neighbours[index]
            origin = format_address(self.address)
            message = Packet(f"{origin} {self.get_next_seqno()} 1 HELLO")
            await self.send_packet(address, message)
        else:
            log("Erro! Vizinho inválido")

    async def new_flooding_search(self):
        key = await key_prompt()
        if self.get_local_key(key):
            return
        origin = format_address(self.address)
        port = self.address[1]
        message = Packet(f"{origin} {self.get_next_seqno()} {self.default_ttl} SEARCH FL {port} {key} 1")
        self.seen.add(message.get_unique_id())
        for neighbour in self.neighbours:
            await self.send_packet(neighbour, message)

    async def new_random_walk_search(self):
        key = await key_prompt()
        if self.get_local_key(key):
            return
        origin = format_address(self.address)
        port = self.address[1]
        message = Packet(f"{origin} {self.get_next_seqno()} {self.default_ttl} SEARCH RW {port} {key} 1")
        self.seen.add(message.get_unique_id())
        neighbour = random.choice(self.neighbours)
        await self.send_packet(neighbour, message)

    async def new_depth_first_search(self):
        key = await key_prompt()
        if self.get_local_key(key):
            return
        origin = format_address(self.address)
        port = self.address[1]
        message = Packet(f"{origin} {self.get_next_seqno()} {self.default_ttl} SEARCH BP {port} {key} 1")
        self.seen.add(message.get_unique_id())
        self.search_state = {'parent': self.address, 'candidates': list(self.neighbours), 'active': None}
        self.search_state['active'] = self.search_state['candidates'].pop()
        await self.send_packet(self.search_state['active'], message)

    async def show_statistics(self):
        print("Estatísticas:") 
        print(f"\tTotal de mensagens de flooding vistas: {self.stats.counters['fl']}")
        print(f"\tTotal de mensagens de random walk vistas: {self.stats.counters['rw']}")
        print(f"\tTotal de mensagens de busca em profundidade vistas: {self.stats.counters['bp']}")
        print(f"\tMédia de saltos até encontrar destino por flooding: {self.stats.calculate_stats('fl')}")
        print(f"\tMédia de saltos até encontrar destino por random walk: {self.stats.calculate_stats('rw')}")
        print(f"\tMédia de saltos até encontrar destino por busca em profundidade: {self.stats.calculate_stats('bp')}")

    async def change_default_ttl(self):
        new_ttl = int(await get_async_input("\nDigite novo valor de TTL\n"))
        if new_ttl > 0:
            self.default_ttl = new_ttl
        else:
            log("Erro! TTL deve ser maior que 0")

    async def send_bye(self):
        origin = format_address(self.address)
        message = Packet(f"{origin} {self.get_next_seqno()} 1 BYE")
        for neighbour in self.neighbours:
            await self.send_packet(neighbour, message)

    async def send_packet(self, address: tuple[str, int], packet: Packet) -> bool:
        addr, port = address
        print(f"Encaminhando mensagem \"{packet}\" para {format_address(address)}")
        try:
            reader, writer = await asyncio.open_connection(addr, port)
            writer.write(f"{packet}\n".encode())
            await writer.drain()

            data = await reader.read(100)
            reply = data.decode().strip()

            writer.close()
            await writer.wait_closed()

            reply_packet = Packet(reply, addr)
            if reply_packet.operation == f"{packet.operation}_OK":
                print(f"\tEnvio feito com sucesso: \"{packet}\"")
                return True
        except ConnectionRefusedError:
            pass
        print("\tErro ao conectar!")
        return False

def parse_args() -> tuple[str, str, str]:
    parser = ArgumentParser()
    parser.add_argument("address", type=str, help="This node's IP address in the format <addr>:<port>")
    parser.add_argument("neighbours_file", type=str, nargs='?', default=None, help="Path to the neighbours file (optional)")
    parser.add_argument("keys_file", type=str, nargs='?', default=None, help="Path to the neighbours file (optional)")
    args = parser.parse_args()
    return args.address, args.neighbours_file, args.keys_file

async def main() -> int:
    address, neighbours, keys = parse_args()
    node = Node(address, neighbours, keys)
    asyncio.create_task(node.start_server())
    print(f"Servidor criado: {format_address(node.address)}")

    print()

    if neighbours:
        await node.load_neighbours(neighbours)

    print()

    if keys:
        await node.load_keys(keys)

    await node.run_menu()
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
