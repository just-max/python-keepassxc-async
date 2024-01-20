# keepassxc-async

Asyncio-compatible client for the [KeePassXC browser protocol](https://github.com/keepassxreboot/keepassxc-browser/blob/develop/keepassxc-protocol.md).

Tested with KeePassXC 2.7.6. Please open an issue if there is an incompatibility with a different version.

## Usage

Minimal example to connect to a database for the first time:

```python
import asyncio
from keepassxc.database import Client, generate_identification_key

async def main():
    async with await Client.create() as client:
        await client.wait_unlocked()
        identification_key = generate_identification_key()
        assoc, meta = await client.associate(identification_key)
        print(f"Connected to KeePassXC {meta.version} with ID {assoc.id!r}.")

if __name__ == '__main__':
    asyncio.run(main())
```

For a complete example with different operations and including error handling, see `examples/example.py`.

## License

MIT License. Copyright © 2024 Max Lang. See `LICENSE` for details.
