import socket
import asyncio
import sys

def parse_address(address: str) -> tuple[str, int]:
    addr, port = address.split(':')
    entries = socket.getaddrinfo(addr, int(port))
    for entry in entries:
        if entry[4]:
            host, port = entry[4]
            return host, port
    raise ValueError(f"Invalid address: {address}")

def address_to_string(address: tuple[str, int]) -> str:
    addr, port = address
    return f"{addr}:{port}"

def split_key_value(line: str) -> tuple[str, str]:
    return line.split()

async def async_input(prompt: str = "") -> str:
    return await asyncio.to_thread(lambda: input(prompt))

def debug_print(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

async def get_search_key():
    return await async_input("Digite a chave a ser buscada\n")
