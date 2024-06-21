import asyncio
import websockets


async def test_client():
    uri = "ws://localhost:8765"
    while True:
        async with websockets.connect(uri) as websocket:
            while True:
                try:
                    # Wait for a response from the server
                    await websocket.send("ping")
                    response = await websocket.recv()
                    print(f"Received from server: {response}")

                except websockets.ConnectionClosed:
                    print("Connection with the server closed.")
                    break


if __name__ == "__main__":
    asyncio.run(test_client())
