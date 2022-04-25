import re
import json
import redis
import psycopg2
import socket
import queue
from .plc import thread
from abc import ABC, abstractmethod


class Receiver:
    """
    'Приёмник' данных с так называемого 'контроллера'.
    """

    def __init__(self, source_host, source_port):
        self.__source_host = source_host
        self.__source_port = source_port
        self.__run = False
        self.__socket = None
        self.__storages = []

    @thread
    def start(self):
        """
        Запуск 'прослушивания' приёмником источника.
        """

        self.__run = True

        for s in self.__storages:
            s.listen()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            self.__socket = s
            s.connect((self.__source_host, self.__source_port))

            while self.__run == True:
                data = s.recv(1024)
                if data:
                    for d in re.findall('{.*?}', data.decode('utf-8')):
                        try:
                            d = json.loads(d)
                        except:
                            print('error', d)
                        else:
                            for _s in self.__storages:
                                _s.queue_data(d)
                elif data == b'':
                    print('Disconnected by peer')
                    for _s in self.__storages:
                        _s.stop()
                    break

    def stop(self):
        """
        Остановка прослушивания источника.
        """

        self.__run = False
        self.__socket.shutdown(socket.SHUT_RDWR)
        self.__socket.close()

    def add_storage(self, storage):
        """
        Добавление хранилища для получаемых данных.
        """

        self.__storages.append(storage)


class Storage(ABC):
    """
    Абстрактное хранилище.
    """

    @abstractmethod
    def __init__(self, connection_options):
        pass

    @abstractmethod
    def _get_connection(self, connection_options):
        """
        Получение соединения с хранилищем.
        """
        pass

    @abstractmethod
    def _store_data(self, data):
        """
        Сохранение данных в хранилище.
        """
        pass

    @abstractmethod
    def listen(self):
        """
        Прослушивание очереди для получения данных для сохранения.
        """
        pass

    @abstractmethod
    def stop(self):
        """
        Остановка прослушивания очереди.
        """
        pass


class PostgresStorage(Storage):
    """
    Хранилище в postgres.

    Предполагается, что уже существует таблица с полями:
    int id, int iec, varchar value, int timestamp.
    """

    def __init__(self, connection_options):
        self.__target_name = connection_options.get('target', None)
        self.__conn = self._get_connection(connection_options)
        self.__queue = queue.Queue()
        self.__listening = False

    def queue_data(self, data):
        self.__queue.put(data)

    def _get_connection(self, connection_options):
        dbname = connection_options['dbname']
        host = connection_options['host']
        port = connection_options['port']
        user = connection_options['user']
        password = connection_options['password']

        conn = psycopg2.connect(
            dbname=dbname,
            host=host,
            port=port,
            user=user,
            password=password,
        )

        conn.autocommit = True

        return conn

    def _store_data(self, data):
        iec, value, timestamp = data['iec'], data['value'], data['timestamp']
        q = f'insert into {self.__target_name} (iec, value, timestamp) values ({iec}, {value}, {timestamp});'
        
        with self.__conn.cursor() as cur:
            cur.execute(q)

    @thread
    def listen(self):
        self.__listening = True

        while self.__listening == True:
            d = self.__queue.get()
            self._store_data(d)

    def stop(self):
        self.__listening = False


class RedisStorage(Storage):
    """
    Хранилище в redis.

    Хэш-таблица хранит последние значения по параметрам.
    """

    def __init__(self, connection_options):
        self.__target_name = connection_options.get('target', None)
        self.__conn = self._get_connection(connection_options)
        self.__queue = queue.Queue()

    def queue_data(self, data):
        self.__queue.put(data)

    def _get_connection(self, connection_options):
        conn = redis.Redis(connection_options['host'], connection_options['port'])

        return conn

    def _store_data(self, data):
        conn = self.__conn

        if conn is not None:
            pass

        iec = data['iec']
        conn.hset(self.__target_name, iec, bytes(f'{data}', 'utf-8'))

    @thread
    def listen(self):
        self.__listening = True

        while self.__listening == True:
            d = self.__queue.get()
            self._store_data(d)

    def stop(self):
        self.__listening = False
