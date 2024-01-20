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
