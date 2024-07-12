import sys
import asyncio
from argparse import ArgumentParser
from peernode import PeerNode
from utils import address_to_string

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
        help="Path to the keys file (optional)"
    )
    args = parser.parse_args()
    return args.address, args.neighbours_file, args.keys_file

async def main() -> int:
    address, neighbours, keys = parse_args()
    node = PeerNode(address, neighbours, keys)
    asyncio.create_task(node.start_server())
    print(f"Servidor criado: {address_to_string(node.address)}")

    if neighbours:
        await node.load_neighbours(neighbours)
    
    if keys:
        await node.load_keys(keys)
    
    await node.display_menu()
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
