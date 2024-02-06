# загрузка основных библиотек
import pandas as pd
import websocket
import threading
import json
import pymysql


# Создадим класс Binance, наследующий библиотеу WebSocket с приложением WebSocketApp
class Binance(websocket.WebSocketApp):
    '''Класс загрузки цен c биржи Binance с помощью WebSocket и сохранение в MySQL базу '''
    # инициализируем подключение к бирже Binance
    # пропишем свойства внутри класса, для открытия соединения, обработки сообщения, обработать возможную ошибку и закрытие
    def __init__(self, url):
        super().__init__(url=url, on_open = self.on_open)

        self.on_message = lambda ws, msg: self.message(msg)
        self.on_error = lambda ws, er: print('Error', er)
        self.on_close = lambda ws: print('### closed ###')
        # подключимся к базе данных
        self.run_forever()
    # подключение к бирже
    # открытие соединения
    def on_open(self, ws,):
        print('Websocket was opened')

    # подключение к базе данных
    def connectDB(self):
        return pymysql.connect(host='127.0.0.1', port=int(3306), user="root", password='745088Vt', db="binance", charset='utf8mb4')

    # создание запроса к Binance
    def message(self, msg):
        data = json.loads(msg) # выгрузим json в массив
        # переберем массив данных формата json
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

    # загрузка потока котировок всех фьючерсных пар
    def all_market_stream(self, new_data, p):
        # подключимся к базе данных
        conn = self.connectDB()
        cursor = conn.cursor()
        if isinstance(new_data, list):
            for crypto_symbol in new_data:
                    if p:
                        print(f"{crypto_symbol['s']} = {crypto_symbol['c']}, {crypto_symbol['E']}")

                    cursor.execute(f"INSERT INTO binance_price (symbols, price, time_update)\
                                   VALUES ('{crypto_symbol['s']}', '{crypto_symbol['c']}', '{crypto_symbol['E']}')\
                                   ON DUPLICATE KEY UPDATE price='{crypto_symbol['c']}', time_update=UNIX_TIMESTAMP();")
        else:
            print(f"Цена фьючерса: {new_data['s']} = {new_data['c']} на {new_data['E']}\n")
        conn.commit() # фиксация транзакции
        conn.close() # закрытие подключения
        if not p:
            print('ИДЕТ ЗАГРУЗКА ОСНОВНОГО ПОТОКА В БАЗУ ДАННЫХ\n')


    # функции потоков
    def all_market_tickers():
        # загрузка url и параметров
        return threading.Thread(target=Binance, args=('wss://fstream.binance.com:443/ws/!ticker@arr',)).start()

    # данные конкретного фьючерса
    def symbol_ticker(symbol):
        return threading.Thread(target=Binance, args=(f'wss://fstream.binance.com:443/ws/{symbol}@ticker',)).start()

    # создание многопоточности
    def threads(query1, query2):
        return threading.Thread(target=Binance, args=(f'wss://fstream.binance.com:443/stream?streams={query1}/{query2}',)).start()