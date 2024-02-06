import websocket
import threading
import json


class Binance(websocket.WebSocketApp):
    def __init__(self, url):
        super().__init__(url=url, on_open = self.on_open)
        self.on_message = lambda ws, msg: self.message(msg)
        self.on_error = lambda ws, er: print('Error', er)
        self.on_close = lambda ws: print('### closed ###')
        self.run_forever()

    def on_open(self, ws,):
        print('Websocket was opened')

    def connectDB(self):
        pass

    def message(self, msg):
        data = json.loads(msg)

        if 'stream' in data:
            if data['stream'] == '!ticker@arr':
                self.all_market_stream(data['data'], p=False)

            elif data['stream'].endswith('@arr'):
                print(f"Цена фьючерса: {data['data']['s']} = {data['data']['c']} на {data['data']['E']}\n")

            elif data['stream'].endswith('Price'):
                print(f"Цена фьючерса: {data['data']['s']} = {data['data']['p']} на {data['data']['E']}\n")

            else:
                print()
                print(data)

        else:
            self.all_market_stream(data, p=True)

        def all_market_tickers():
            return threading.Thread(target=Binance, args=('wss://fstream.binance.com:443/ws/!ticker@arr',)).start()
