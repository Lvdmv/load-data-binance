# Программа для получение котировок по всем фьючерсам Binance с использованием WebSocket и потоков (threads) с возможностью загрузки данных в базу MySQL
   - Программа использует WebSocket для подключения к API Binance и получения котировок по фьючерсам.
   - Реализует многопоточность для обработки данных в параллельных потоках.
   - Есть возможность загрузки данных в базу MySQL.
   - Программа имеет возможность получать данные котировок в реальном времени и обновлять их при появлении новых данных.
   - Обеспечивает стабильное подключение к WebSocket и обработку возможных ошибок и исключений.
   - Программа фильтрует движения по параметрам: цены, объемы, время (параметры можно поменять).

## Примеры работы программы:

### Загрузка данных по всем фьючерсам

<img src="https://github.com/Lvdmv/load-data-binance/blob/main/requests/thread1.png" width="1700" height="800" />



### Загрузка данных по конкретному фьючерсу

<img src="https://github.com/Lvdmv/load-data-binance/blob/main/requests/thread2.png" width="1500" height="600" />



### Загрузка данных из нескольких потоков(многопоточность)

<img src="https://github.com/Lvdmv/load-data-binance/blob/main/requests/thread3.png" width="1700" height="800" />



### Обращение к полученной базе. Посмотрим на данные цены и времени после последнего обновления фьючерса

<img src="https://github.com/Lvdmv/load-data-binance/blob/main/requests/querydb.png" width="1700" height="1000" />




### Посмотрим на данные по фьючерсу BTCUSDT

<img src="https://github.com/Lvdmv/load-data-binance/blob/main/requests/querydb1.png" width="1000" height="400" />
