import asyncio
import websockets


async def test_client():
    uri = "ws://localhost:8765"
    while True:

        print("here we go again")
        try:
            async with websockets.connect(uri) as websocket:
                try:
                    # Wait for a response from the server
                    await websocket.send("ping")
                    response = await websocket.recv()
                    print(f"Received from server: {response}")

                except websockets.ConnectionClosed:
                    print("Connection with the server closed.")

        except Exception as e:
            print(e)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_client())

#    asyncio.run(test_client())
