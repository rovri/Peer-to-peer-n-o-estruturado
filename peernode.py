import asyncio
import random
from message import Message
from statistics_p2p import Statistics
from utils import address_to_string, parse_address, split_key_value, async_input, debug_print, get_search_key

class PeerNode:
    def __init__(self, address: str, neighbours_file: str = None, keys_file: str = None):
        self.address = parse_address(address)
        self.neighbours = []
        self.keys = {}
        self.seqno = 1
        self.default_ttl = 100
        self.seen_messages = set()
        self.stats = Statistics()
        self.parent_node = None
        self.candidates = []
        self.active_node = None

    def get_next_seqno(self) -> int:
        seqno = self.seqno
        self.seqno += 1
        return seqno

    async def load_neighbours(self, neighbours_file: str):
        with open(neighbours_file, 'r') as file:
            origin = address_to_string(self.address)
            hello_message = Message(f"{origin} {self.get_next_seqno()} 1 HELLO")
            await asyncio.gather(*(self._add_neighbour(line.strip(), hello_message) for line in file))

    async def _add_neighbour(self, line, hello_message):
        neighbour_address = parse_address(line)
        neighbour_str = address_to_string(neighbour_address)
        print(f"Tentando adicionar vizinho {neighbour_str}")
        if await self.send_message(neighbour_address, hello_message):
            if neighbour_address not in self.neighbours:
                print(f"\tAdicionando vizinho na tabela: {neighbour_str}")
                self.neighbours.append(neighbour_address)
            else:
                print(f"\tVizinho {neighbour_str} já está na tabela")

    async def load_keys(self, keys_file: str):
        with open(keys_file, 'r') as file:
            for line in file:
                key, value = split_key_value(line)
                print(f"Adicionando par ({key}, {value}) na tabela local")
                self.keys[key] = value

    def has_key(self, key: str) -> bool:
        return key in self.keys

    def check_local_key(self, key: str) -> bool:
        if self.has_key(key):
            print(f"Chave: {key}, Valor: {self.keys[key]}")
            return True
        return False
    
    def remove_candidate(self, candidate: tuple[str, int]):
        if candidate in self.candidates:
            self.candidates.remove(candidate)

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        data = await reader.readline()
        addr: str = writer.get_extra_info("peername")[0]
        message = Message(data.decode(), addr)
        print(f"Mensagem recebida: \"{message}\"")
        asyncio.create_task(self.process_message(message))
        writer.write(message.create_response().encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def process_message(self, message: Message):
        operations = {
            "HELLO": self.process_hello,
            "SEARCH": self._process_search,
            "VAL": self.process_val,
            "BYE": self.process_bye
        }
        await operations.get(message.operation, self._unknown_operation)(message)

    async def _process_search(self, message: Message):
        search_modes = {
            "FL": self.process_flood_search,
            "RW": self.process_random_search,
            "BP": self.process_depth_search
        }
        search_mode = message.args[0]
        if search_mode in search_modes:
            self._update_stats(search_mode)
            await search_modes[search_mode](message)

    def _update_stats(self, mode):
        if mode == "FL":
            self.stats.flood_count += 1
        elif mode == "RW":
            self.stats.random_count += 1
        elif mode == "BP":
            self.stats.depth_count += 1

    async def _unknown_operation(self, message: Message):
        debug_print(f"Unknown operation: {message.operation}")

    async def process_hello(self, message: Message):
        if message.origin not in self.neighbours:
            self.neighbours.append(message.origin)
            print(f"\tAdicionando vizinho na tabela: {address_to_string(message.origin)}")
        else:
            print(f"\tVizinho {address_to_string(message.origin)} já está na tabela")

    async def process_search_message(self, message: Message) -> Message | None:
        mode, last_hop_port, key, hop_count = message.args
        last_hop_ip = (message.sender, int(last_hop_port))
        if message.to_unique_tuple() in self.seen_messages:
            print(f"{mode}: Mensagem repetida!")
            return None
        self.seen_messages.add(message.to_unique_tuple())
        if self.has_key(key):
            response = Message(f"{address_to_string(self.address)} {self.get_next_seqno()} 1 VAL {mode} {key} {self.keys[key]} {hop_count}")
            await self.send_message(message.origin, response)
            return None
        new_message = message.forward_message(self.address)
        if new_message.is_ttl_expired():
            print("TTL igual a zero, descartando mensagem")
            return None
        return new_message

    async def process_flood_search(self, message: Message):
        new_message = await self.process_search_message(message)
        if new_message:
            await asyncio.gather(*(self.send_message(neighbour, new_message) for neighbour in self.neighbours if neighbour != new_message.sender))

    

    async def process_random_search(self, message: Message):
        new_message = await self.process_search_message(message)
        if new_message:
            neighbours = [n for n in self.neighbours if n != new_message.sender]
            next_hop = random.choice(neighbours) if neighbours else new_message.sender
            await self.send_message(next_hop, new_message)

    async def process_depth_search(self, message: Message):
        new_message = await self.process_search_message(message)
        if new_message:
            if message.to_unique_tuple() not in self.seen_messages:
                self.parent_node = new_message.sender
                self.candidates = [n for n in self.neighbours if n != new_message.sender]
                self.active_node = None
            self.remove_candidate(new_message.sender)
            await self._handle_depth_search_response(new_message, message)

    async def _handle_depth_search_response(self, new_message, message):
        if self.parent_node is None:
            print(f"Erro: Nó pai é None. Mensagem: {new_message}")
            return
        
        if self.parent_node == self.address and self.active_node == new_message.sender and not self.candidates:
            print(f"BP: Nao foi possivel localizar a chave {message.args[2]}")
        elif self.active_node and self.active_node != new_message.sender:
            print("BP: ciclo detectado, devolvendo a mensagem...")
            await self.send_message(new_message.sender, new_message)
        elif not self.candidates:
            print("BP: nenhum vizinho encontrou a chave, retrocedendo...")
            if self.parent_node:
                await self.send_message(self.parent_node, new_message)
            else:
                print("Erro: nó pai não definido.")
        else:
            self.active_node = self.candidates.pop()
            await self.send_message(self.active_node, new_message)


    async def process_val(self, message: Message):
        mode, key, value, hop_count = message.args
        print(f"Valor encontrado! Chave: {key}, Valor: {value}")
        self._update_hops(mode, int(hop_count))

    def _update_hops(self, mode, hop_count):
        if mode == "FL":
            self.stats.add_flood_hops(hop_count)
        elif mode == "RW":
            self.stats.add_random_hops(hop_count)
        elif mode == "BP":
            self.stats.add_depth_hops(hop_count)

    async def process_bye(self, message: Message):
        if message.origin in self.neighbours:
            self.neighbours.remove(message.origin)
            print(f"Removendo vizinho da tabela: {address_to_string(message.origin)}")

    async def start_server(self):
        addr, port = self.address
        server = await asyncio.start_server(self.handle_connection, addr, port)
        async with server:
            await server.serve_forever()

    async def display_menu(self):
        menu_options = {
            '0': self.list_neighbours,
            '1': self.send_hello,
            '2': self.new_flood_search,
            '3': self.new_random_walk_search,
            '4': self.new_depth_search,
            '5': self.display_stats,
            '6': self.change_default_ttl,
            '9': self._exit_program
        }
        while True:
            print(self._menu_text())
            option = (await async_input()).strip()
            if option in menu_options:
                await menu_options[option]()
            else:
                debug_print("Erro! Opção inválida")

    def _menu_text(self):
        return (
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

    async def _exit_program(self):
        await self.send_bye()
        exit()

    async def list_neighbours(self):
        print(f"\nHá {len(self.neighbours)} vizinhos na tabela:")
        for index, neighbour in enumerate(self.neighbours):
            print(f"\t[{index}] {address_to_string(neighbour)}")

    async def send_hello(self):
        if not self.neighbours:
            debug_print("Erro! Não há vizinhos")
            return
        print("\nEscolha o vizinho:")
        await self.list_neighbours()
        neighbour_index = int(await async_input())
        if 0 <= neighbour_index < len(self.neighbours):
            neighbour = self.neighbours[neighbour_index]
            origin = address_to_string(self.address)
            hello_message = Message(f"{origin} {self.get_next_seqno()} 1 HELLO")
            await self.send_message(neighbour, hello_message)
        else:
            debug_print("Erro! Vizinho inválido")

    async def new_flood_search(self):
        await self._new_search("FL")

    async def new_random_walk_search(self):
        await self._new_search("RW")

    async def new_depth_search(self):
        await self._new_search("BP")

    async def _new_search(self, search_type):
        key = await get_search_key()
        if self.check_local_key(key):
            return
        origin = address_to_string(self.address)
        port = self.address[1]
        search_message = Message(f"{origin} {self.get_next_seqno()} {self.default_ttl} SEARCH {search_type} {port} {key} 1")
        self.seen_messages.add(search_message.to_unique_tuple())
        if search_type == "FL":
            await asyncio.gather(*(self.send_message(neighbour, search_message) for neighbour in self.neighbours))
        elif search_type == "RW":
            neighbour = random.choice(self.neighbours)
            await self.send_message(neighbour, search_message)
        elif search_type == "BP":
            self.parent_node = self.address  # Set parent_node here
            self.candidates = [*self.neighbours]
            self.active_node = self.candidates.pop()
            await self.send_message(self.active_node, search_message)


    async def display_stats(self):
        print("Estatísticas:")
        stats = [
            f"\tTotal de mensagens de flooding vistas: {self.stats.flood_count}",
            f"\tTotal de mensagens de random walk vistas: {self.stats.random_count}",
            f"\tTotal de mensagens de busca em profundidade vistas: {self.stats.depth_count}",
            f"\tMédia de saltos até encontrar destino por flooding: {self.stats.calculate_stats(self.stats.flood_hops)}",
            f"\tMédia de saltos até encontrar destino por random walk: {self.stats.calculate_stats(self.stats.random_hops)}",
            f"\tMédia de saltos até encontrar destino por busca em profundidade: {self.stats.calculate_stats(self.stats.depth_hops)}"
        ]
        print("\n".join(stats))

    async def change_default_ttl(self):
        new_ttl = int(await async_input("\nDigite novo valor de TTL\n"))
        if new_ttl > 0:
            self.default_ttl = new_ttl
        else:
            debug_print("Erro! TTL deve ser maior que 0")

    async def send_bye(self):
        origin = address_to_string(self.address)
        bye_message = Message(f"{origin} {self.get_next_seqno()} 1 BYE")
        await asyncio.gather(*(self.send_message(neighbour, bye_message) for neighbour in self.neighbours))

    async def send_message(self, address: tuple[str, int], message: Message) -> bool:
        if address is None:
            print("Erro: endereço é None.")
            return False
        addr, port = address
        print(f"Encaminhando mensagem \"{message}\" para {address_to_string(address)}")
        try:
            reader, writer = await asyncio.open_connection(addr, port)
            writer.write(f"{message}\n".encode())
            await writer.drain()
            data = await reader.read(100)
            response = data.decode().strip()
            writer.close()
            await writer.wait_closed()
            return Message(response, addr).operation == f"{message.operation}_OK"
        except ConnectionRefusedError:
            pass
        print("\tErro ao conectar!")
        return False