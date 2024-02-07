# загрузка основных библиотек
import pandas as pd
import websocket
import threading
import json
import pymysql

PASSWORD = '#######'


# Создадим класс Binance, наследующий библиотеу WebSocket с приложением WebSocketApp
class Binance(websocket.WebSocketApp):

    """Класс Binance для загрузки цен c биржи Binance

    Класс Binance является подклассом  принимает поток данных с помощью WebSocket и сохранение в MySQL базу данных

    Attributes
    ----------
    on_message: str
        получает поток данных с биржи
    on_error: str
        присылает сообщение об ошибке
    on_close: str
        присылает сообщение о закрытии подключения
    run_forever
        запускает основной поток данных

    Methods
    -------
    on_open(): str
        метод открытия соединения
    connectDB()
        метод создания подключения к базе данных
    message()
        метод для получения и обработки данных
    all_market_stream()
        загружает данные в базу данных
    """

    def __init__(self, url):
        """
        Инициализируем подключение к бирже Binance

        пропишем свойства внутри класса, для открытия соединения, обработки сообщения,
        обработать возможную ошибку и закрытие

        Attributes
        ----------
        url:
            получение url адреса для подключения к бирже
        """
        super().__init__(url=url, on_open=self.on_open)  # обращаемся к атрибутам родительского класса websoket
        '''функции для событий открытия соединения, получения сообщений, закрытия соединения'''
        self.on_message = lambda ws, msg: self.message(msg)
        self.on_error = lambda ws, er: print('Error', er)
        self.on_close = lambda ws: print('### closed ###')
        self.run_forever()  # метод для запуска главного цикла обработки сообщений

    def on_open(self, ws: str,):
        """
        Открытие соединения

        Parameters
        ----------
        ws: str
            объект WebSocketApp
        """
        print('Websocket was opened')

    def connectDB(self, password: str):
        """
        Подключение к базе данных

        Parameters
        ----------
        password: str
            пароль
        """
        return pymysql.connect(host='127.0.0.1', port=int(3306), user="root", password=password, db="binance", charset='utf8mb4')

    # создание запроса к Binance
    def message(self, msg: str):
        """
        Метод получает поток данных, конвертирует данные с биржи в json объект, а затем в  и фильтрует данные

        Parameters
        ----------
        msg: str
            поток данных
        """
        data = json.loads(msg)  # выгрузим json
        # переберем данных формата json
        if 'stream' in data:
            if data['stream'] == '!ticker@arr':
                self.all_market_stream(data['data'], p=False)

            elif data['stream'].endswith('@arr'):
                print(f"Цена фьючерса: {data['data']['s']} = {data['data']['c']} на {data['data']['E']}\n")

            elif data['stream'].endswith(('Price', 'aggTrade')):
                print(f"Цена фьючерса: {data['data']['s']} = {data['data']['p']} на {data['data']['E']}\n")

            else:
                print()
                print(data)

        else:
            self.all_market_stream(data, p=True)

    def all_market_stream(self, new_data: list, p: bool):
        """
        Метод для загрузки данных в базу при разрешении флага p
        или вывод текущих данных по каждому фьючерсу

        Parameters
        ----------
        new_data: list
            список данных по инструменту
        p: bool
            флаг отвечающий за загрузку данных  в базу или вывод из в терминал
        """
        conn = self.connectDB(PASSWORD)  # создание соединения с базой данных
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
        conn.commit()  # фиксация транзакции
        conn.close()  # закрытие подключения
        if not p:
            print('ИДЕТ ЗАГРУЗКА ОСНОВНОГО ПОТОКА В БАЗУ ДАННЫХ\n')


# функции потоков
def all_market_tickers():
    """Функция для получения потока данных по всем фьючерсам"""
    return threading.Thread(target=Binance, args=('wss://fstream.binance.com:443/ws/!ticker@arr',)).start()


def symbol_ticker(symbol: str):
    """Функция для получения данных по конкретному фьючерсу"""
    return threading.Thread(target=Binance, args=(f'wss://fstream.binance.com:443/ws/{symbol}@ticker',)).start()


def threads(query1: str, query2: str):
    """Функция для создания многопоточного запроса по нескольким инструментам"""
    return threading.Thread(target=Binance, args=(f'wss://fstream.binance.com:443/stream?streams={query1}/{query2}',)).start()
