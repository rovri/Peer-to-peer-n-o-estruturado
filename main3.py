from argparse import ArgumentParser
import asyncio
import socket
import random
import sys

def adress_split(address: str) -> tuple[str, int]:
    addr, port = address.split(':')
    entries = socket.getaddrinfo(addr, int(port), family=socket.AF_INET)
    first_entry = entries[0]
    host, port = first_entry[4][0], first_entry[4][1]
    return host, port

def adress_string(address: tuple[str, int]) -> str:
    addr, port = address
    return f"{addr}:{port}"

def entry_split(key_val: str) -> tuple[str, str]:
    return key_val.split()

async def async_input(prompt: str = "") -> str:
    return await asyncio.to_thread(input, prompt)

def debug_print(*args, **kwargs):
    kwargs.setdefault('file', sys.stderr)
    print(*args, **kwargs)

async def key_prompt():
    return await async_input("Digite a chave a ser buscada\n")

class Message:
    sender: str | None
    origin: tuple[str, int]
    seqno: int
    ttl: int
    operation: str
    args: list[str]

    def __init__(self, message: str, sender: str | None = None):
        self.sender = sender
        [origin, seqno, ttl, operation, *args] = message.split()
        self.origin = adress_split(origin)
        self.seqno = int(seqno)
        self.ttl = int(ttl)
        self.operation = operation.upper()
        self.args = args

    def __str__(self) -> str:
        msg = f"{adress_string(self.origin)} {self.seqno} {self.ttl} {self.operation}"
        return ' '.join([msg, *self.args])

    def reply(self) -> str:
        return f"{adress_string(self.origin)} {self.seqno} 1 {self.operation}_OK"

    def tuple_unique(self) -> tuple[str, int, str]:
        op_key = f"{self.operation}_{self.args[0]}" if self.operation == "SEARCH" else self.operation
        return adress_string(self.origin), self.seqno, op_key

    def fw_search(self, new_last_hop_ip: tuple[str, int]) -> 'Message':
        mode, _, key, hop_count = self.args
        last_hop_port = new_last_hop_ip[1]
        hop_count = int(hop_count) + 1
        return Message(f"{adress_string(self.origin)} {self.seqno} {self.ttl - 1} {self.operation} {mode} {last_hop_port} {key} {hop_count}")

    def discard_msg_ttl(self) -> bool:
        if self.ttl <= 0:
            print("TTL igual a zero, descartando mensagem")
            return True
        return False

class Stats:
    fl_count: int
    rw_count: int
    bp_count: int
    fl_avg_hops: float
    fl_avgsqr_hops: float
    fl_val_count: int
    rw_avg_hops: float
    rw_avgsqr_hops: float
    rw_val_count: int
    bp_avg_hops: float
    bp_avgsqr_hops: float
    bp_val_count: int

    def __init__(self):
        self.fl_count = 0
        self.rw_count = 0
        self.bp_count = 0
        self.fl_avg_hops = 0
        self.fl_avgsqr_hops = 0
        self.fl_val_count = 0
        self.rw_avg_hops = 0
        self.rw_avgsqr_hops = 0
        self.rw_val_count = 0
        self.bp_avg_hops = 0
        self.bp_avgsqr_hops = 0
        self.bp_val_count = 0

    def record_value(self, hops: int, avg_hops_attr, avgsqr_hops_attr, val_count_attr):
        setattr(self, avg_hops_attr, getattr(self, avg_hops_attr) + hops)
        setattr(self, avgsqr_hops_attr, getattr(self, avgsqr_hops_attr) + hops * hops)
        setattr(self, val_count_attr, getattr(self, val_count_attr) + 1)

    def record_fl_value(self, hops: int):
        self.record_value(hops, 'fl_avg_hops', 'fl_avgsqr_hops', 'fl_val_count')

    def record_rw_value(self, hops: int):
        self.record_value(hops, 'rw_avg_hops', 'rw_avgsqr_hops', 'rw_val_count')

    def record_bp_value(self, hops: int):
        self.record_value(hops, 'bp_avg_hops', 'bp_avgsqr_hops', 'bp_val_count')

    def calculate_stats(self, val_count, avg_hops, avgsqr_hops) -> str:
        if val_count == 0:
            return "N/A"
        avg = avg_hops / val_count
        avgsqr = avgsqr_hops / val_count
        variance = avgsqr - avg * avg
        deviation = variance ** 0.5
        return f"{avg:.3} (dp {deviation:.3})"

    def fl_stats_calc(self) -> str:
        return self.calculate_stats(self.fl_val_count, self.fl_avg_hops, self.fl_avgsqr_hops)

    def rw_stats_calc(self) -> str:
        return self.calculate_stats(self.rw_val_count, self.rw_avg_hops, self.rw_avgsqr_hops)

    def bp_stats_calc(self) -> str:
        return self.calculate_stats(self.bp_val_count, self.bp_avg_hops, self.bp_avgsqr_hops)


class Node:
    address: tuple[str, int]
    neighbours: list[tuple[str, int]]
    keys: dict[str, str]
    seqno: int
    default_ttl: int
    seen: set[tuple[str, int, str]]
    stats: Stats
    # Depth-first search state:
    parent_node: tuple[str, int] | None
    candidate_nodes: list[tuple[str, int]]
    active_node: tuple[str, int] | None

    def __init__(self, address: str, neighbours_file: str = None, keys_file: str = None):
        self.address = adress_split(address)
        self.neighbours = []
        self.keys = {}
        self.seqno = 1
        self.default_ttl = 100
        self.seen = set()
        self.stats = Stats()
        self.parent_node = None
        self.candidate_nodes = []
        self.active_node = None

    def next_seqno(self) -> int:
        self.seqno += 1
        return self.seqno - 1

    async def load_neighbours(self, neighbours_file: str):
        with open(neighbours_file, 'r') as file:
            origin = adress_string(self.address)
            message = Message(f"{origin} {self.next_seqno()} 1 HELLO")
            for line in file.readlines():
                address = adress_split(line.strip())
                print(f"Tentando adicionar vizinho {adress_string(address)}")
                sent_successfully = await self.send_msg(address, message)
                if sent_successfully:
                    print(f"\tAdicionando vizinho na tabela: {adress_string(address)}")
                    self.neighbours.append(address)

    async def load_keys(self, keys_file: str):
        with open(keys_file, 'r') as file:
            for line in file.readlines():
                key, value = entry_split(line)
                print(f"Adicionando par ({key}, {value}) na tabela local")
                self.keys[key] = value

    def key_found(self, key: str) -> bool:
        if key in self.keys:
            print("Chave encontrada!")
            return True
        return False

    def local_key(self, key: str) -> bool:
        if self.key_found(key):
            print("Valor na tabela local!")
            print(f"\tchave: {key} valor: {self.keys[key]}")
            return True
        return False

    def delete_candidate(self, candidate: tuple[str, int]):
        try:
            self.candidate_nodes.remove(candidate)
        except ValueError:
            pass

    async def process_client_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        data = await reader.readline()
        addr: str = writer.get_extra_info("peername")[0]
        message = Message(data.decode(), addr)

        print(f"Mensagem recebida: \"{message}\"")
        asyncio.create_task(self.process_msg(message))

        writer.write(message.reply().encode())
        await writer.drain()

        writer.close()
        await writer.wait_closed()

    async def process_msg(self, message: Message):
        match message.operation, *message.args:
            case ("HELLO",): await self.process_hello(message)
            case ("SEARCH", "FL", *_): await self.process_flooding_search(message)
            case ("SEARCH", "RW", *_): await self.process_random_walk_search(message)
            case ("SEARCH", "BP", *_): await self.process_depth_first_search(message)
            case ("VAL", *_): await self.process_values(message)
            case ("BYE",): await self.process_bye(message)
            case op: debug_print(f"Unknown operation: \"{' '.join(op)}\"")

    async def process_hello(self, message: Message):
        if message.origin in self.neighbours:
            print(f"\tVizinho {adress_string(message.origin)} já está na tabela")
        else:
            self.neighbours.append(message.origin)
            print(f"\tAdicionando vizinho na tabela: {adress_string(message.origin)}")

    async def process_flooding_search(self, message: Message):
        mode, last_hop_port, key, hop_count = message.args
        last_hop_ip = message.sender, int(last_hop_port)
        if message.tuple_unique() in self.seen:
            print("Flooding: Mensagem repetida!")
            return
        self.seen.add(message.tuple_unique())
        self.stats.fl_count += 1
        if self.key_found(key):
            reply = Message(f"{adress_string(self.address)} {self.next_seqno()} 1 VAL {mode} {key} {self.keys[key]} {int(hop_count)}")
            await self.send_msg(message.origin, reply)
            return
        reply = message.fw_search(self.address)
        if reply.discard_msg_ttl():
            return
        for neighbour in self.neighbours:
            if neighbour == last_hop_ip:
                continue
            await self.send_msg(neighbour, reply)

    async def process_random_walk_search(self, message: Message):
        mode, last_hop_port, key, hop_count = message.args
        last_hop_ip = message.sender, int(last_hop_port)
        self.seen.add(message.tuple_unique())
        self.stats.rw_count += 1
        if self.key_found(key):
            reply = Message(f"{adress_string(self.address)} {self.next_seqno()} 1 VAL {mode} {key} {self.keys[key]} {int(hop_count)}")
            await self.send_msg(message.origin, reply)
            return
        reply = message.fw_search(self.address)
        if reply.discard_msg_ttl():
            return
        neighbours = [*self.neighbours]
        neighbours.remove(last_hop_ip)
        neighbour = random.choice(neighbours) if neighbours else last_hop_ip
        await self.send_msg(neighbour, reply)

    async def process_depth_first_search(self, message: Message):
        mode, last_hop_port, key, hop_count = message.args
        last_hop_ip = message.sender, int(last_hop_port)
        self.stats.bp_count += 1
        if self.key_found(key):
            reply = Message(f"{adress_string(self.address)} {self.next_seqno()} 1 VAL {mode} {key} {self.keys[key]} {int(hop_count)}")
            await self.send_msg(message.origin, reply)
            return
        reply = message.fw_search(self.address)
        if reply.discard_msg_ttl():
            return
        if message.tuple_unique() not in self.seen:
            debug_print(f"\033[33mMensagem {message.tuple_unique()} ainda vista\033[m")
            self.parent_node = last_hop_ip
            self.candidate_nodes = [*self.neighbours]
            self.active_node = None
        self.delete_candidate(last_hop_ip)
        self.seen.add(message.tuple_unique())
        if self.parent_node == self.address and self.active_node == last_hop_ip and not self.candidate_nodes:
            print(f"BP: Nao foi possivel localizar a chave {key}")
            return
        if self.active_node is not None and self.active_node != last_hop_ip:
            print("BP: ciclo detectado, devolvendo a mensagem...")
            await self.send_msg(last_hop_ip, reply)
        elif not self.candidate_nodes:
            print("BP: nenhum vizinho encontrou a chave, retrocedendo...")
            await self.send_msg(self.parent_node, reply)
        else:
            self.active_node = self.candidate_nodes.pop()
            await self.send_msg(self.active_node, reply)

    async def process_values(self, message: Message):
        mode, key, value, hop_count = message.args
        print("\tValor encontrado!")
        print(f"\t\tChave: {key} valor: {value}")
        match mode:
            case "FL": self.stats.record_fl_value(int(hop_count))
            case "RW": self.stats.record_rw_value(int(hop_count))
            case "BP": self.stats.record_bp_value(int(hop_count))

    async def process_bye(self, message: Message):
        try:
            self.neighbours.remove(message.origin)
            print(f"\tRemovendo vizinho da tabela: {adress_string(message.origin)}")
        except ValueError:
            pass

    async def initiate_server(self):
        addr, port = self.address
        server = await asyncio.start_server(self.process_client_connection, addr, port)
        async with server:
            await server.serve_forever()

    async def display_menu(self):
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
            option = (await async_input()).strip()
            match option:
                case '0': await self.list_of_neighbours()
                case '1': await self.send_hello()
                case '2': await self.new_flooding_search()
                case '3': await self.new_random_walk_search()
                case '4': await self.new_depth_first_search()
                case '5': await self.show_statistics()
                case '6': await self.change_default_ttl()
                case '9': break
                case _: debug_print("Erro! Opção inválida")
        await self.send_bye()

    async def list_of_neighbours(self):
        print(f"\nHá {len(self.neighbours)} vizinhos na tabela:")
        for index, neighbour in enumerate(self.neighbours):
            print(f"\t[{index}] {adress_string(neighbour)}")

    async def send_hello(self):
        if not self.neighbours:
            debug_print("Erro! Não há vizinhos")
            return
        print("\nEscolha o vizinho:")
        await self.list_of_neighbours()
        neighbour = int(await async_input())
        if neighbour < 0 or neighbour >= len(self.neighbours):
            debug_print("Erro! Vizinho inválido")
            return
        address = self.neighbours[neighbour]
        origin = adress_string(self.address)
        message = Message(f"{origin} {self.next_seqno()} 1 HELLO")
        await self.send_msg(address, message)

    async def new_flooding_search(self):
        key = await key_prompt()
        if self.local_key(key):
            return
        origin = adress_string(self.address)
        port = self.address[1]
        message = Message(f"{origin} {self.next_seqno()} {self.default_ttl} SEARCH FL {port} {key} 1")
        self.seen.add(message.tuple_unique())
        for neighbour in self.neighbours:
            await self.send_msg(neighbour, message)

    async def new_random_walk_search(self):
        key = await key_prompt()
        if self.local_key(key):
            return
        origin = adress_string(self.address)
        port = self.address[1]
        message = Message(f"{origin} {self.next_seqno()} {self.default_ttl} SEARCH RW {port} {key} 1")
        self.seen.add(message.tuple_unique())
        neighbour = random.choice(self.neighbours)
        await self.send_msg(neighbour, message)

    async def new_depth_first_search(self):
        key = await key_prompt()
        if self.local_key(key):
            return
        origin = adress_string(self.address)
        port = self.address[1]
        message = Message(f"{origin} {self.next_seqno()} {self.default_ttl} SEARCH BP {port} {key} 1")
        self.seen.add(message.tuple_unique())
        self.parent_node = self.address
        self.candidate_nodes = [*self.neighbours]
        self.active_node = self.candidate_nodes.pop()
        await self.send_msg(self.active_node, message)

    async def show_statistics(self):
        print("Estatísticas:") 
        print(f"\tTotal de mensagens de flooding vistas: {self.stats.fl_count}")
        print(f"\tTotal de mensagens de random walk vistas: {self.stats.rw_count}")
        print(f"\tTotal de mensagens de busca em profundidade vistas: {self.stats.bp_count}")
        print(f"\tMédia de saltos até encontrar destino por flooding: {self.stats.fl_stats_calc()}")
        print(f"\tMédia de saltos até encontrar destino por random walk: {self.stats.rw_stats_calc()}")
        print(f"\tMédia de saltos até encontrar destino por busca em profundidade: {self.stats.bp_stats_calc()}")

    async def change_default_ttl(self):
        self.default_ttl = int(await async_input("\nDigite novo valor de TTL\n"))
        if self.default_ttl < 1:
            debug_print("Erro! TTL deve ser maior que 0")

    async def send_bye(self):
        origin = adress_string(self.address)
        message = Message(f"{origin} {self.next_seqno()} 1 BYE")
        for neighbour in self.neighbours:
            await self.send_msg(neighbour, message)

    async def send_msg(self, address: tuple[str, int], message: Message) -> bool:
        addr, port = address
        print(f"Encaminhando mensagem \"{message}\" para {adress_string(address)}")
        try:
            reader, writer = await asyncio.open_connection(addr, port)
            writer.write(f"{message}\n".encode())
            await writer.drain()

            data = await reader.read(100)
            reply = data.decode().strip()

            writer.close()
            await writer.wait_closed()

            reply_msg = Message(reply, addr)
            if reply_msg.operation == f"{message.operation}_OK":
                print(f"\tEnvio feito com sucesso: \"{message}\"")
                return True
        except ConnectionRefusedError:
            pass
        print("\tErro ao conectar!")
        return False

def parse_args() -> tuple[str, str, str]:
    parser = ArgumentParser()
    parser.add_argument(
        "address", type=str,
        help="This node's IP address in the format <addr>:<port>"
    )
    parser.add_argument(
        "neighbours_file", type=str, nargs='?', default=None,
        help="Path to the neighbours file (optional)"
    )
    parser.add_argument(
        "keys_file", type=str, nargs='?', default=None,
        help="Path to the neighbours file (optional)"
    )
    args = parser.parse_args()
    return args.address, args.neighbours_file, args.keys_file

async def main() -> int:
    address, neighbours, keys = parse_args()
    node = Node(address, neighbours, keys)
    asyncio.create_task(node.initiate_server())
    print(f"Servidor criado: {adress_string(node.address)}")

    print()

    if neighbours:
        await node.load_neighbours(neighbours)

    print()

    if keys:
        await node.load_keys(keys)

    await node.display_menu()
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))