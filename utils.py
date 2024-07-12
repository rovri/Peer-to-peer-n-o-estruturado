import socket
import asyncio
import sys

def parse_address(address: str) -> tuple[str, int]:
    try:
        addr, port = address.split(':')
        info = socket.getaddrinfo(addr, int(port), socket.AF_INET, socket.SOCK_STREAM)
        return info[0][4]
    except (ValueError, IndexError, socket.gaierror) as e:
        raise ValueError(f"Invalid address: {address}") from e

def address_to_string(address: tuple[str, int]) -> str:
    return f"{address[0]}:{address[1]}"

def split_key_value(line: str) -> tuple[str, str]:
    key, value = line.split()
    return key, value

async def async_input(prompt: str = "") -> str:
    return await asyncio.to_thread(lambda p=prompt: input(p))

def debug_print(*args, **kwargs):
    sys.stderr.write(' '.join(map(str, args)) + '\n')

async def get_search_key():
    return await async_input("Por favor, digite a chave a ser buscada:\n")
