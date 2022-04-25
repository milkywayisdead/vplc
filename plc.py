import threading
import random
import socket
import selectors
import time
import types
import queue
from abc import ABC, abstractmethod, abstractproperty


class Parameter(ABC):
    """
    Абстрактный параметр.
    """

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def randomize_value(self):
        """
        Изменение значения параметра на случайное.
        """
        pass

    @abstractproperty
    def value(self):
        """
        Значение параметра.
        """
        pass

    def get_message(self):
        """
        Получение сообщения по параметру.
        """

        message = f'{{"iec":"{self.address}",'
        message += f'"value":"{str(self.value).lower()}",'
        message += f'"timestamp":"{int(time.time())}"}}'

        return bytes(message, 'utf-8')


class IntegerParameter(Parameter):
    """
    Целочисленный параметр.
    """

    def __init__(self):
        self.address = 0
        self.name = 'IntegerParameter'
        self.__value = 0

    def randomize_value(self):
        self.__value = random.randint(0, 100)

    @property
    def value(self):
        return float(self.__value)


class FloatParameter(Parameter):
    """
    Параметр с дробной частью.
    """

    def __init__(self):
        self.address = 0
        self.name = 'FloatParameter'
        self.__value = 0.0

    def randomize_value(self):
        self.__value = random.uniform(0, 100)

    @property
    def value(self):
        return self.__value


class BooleanParameter(Parameter):
    """
    Булевый параметр.
    """

    def __init__(self):
        self.address = 0
        self.name = 'BooleanParameter'
        self.__value = False

    def randomize_value(self):
        self.__value = random.choice([True, False])

    @property
    def value(self):
        return self.__value


class ParameterFabric:
    """
    Фабрика параметров.
    """

    def __init__(self):
        # счётчики параметров
        self.__boolean_counter = 0
        self.__integer_counter = 0
        self.__float_counter = 0

    def create_integer(self):
        i = IntegerParameter()
        i.name += str(self.__integer_counter)
        self.__integer_counter += 1

        return i

    def create_float(self):
        f = FloatParameter()
        f.name += str(self.__float_counter)
        self.__float_counter += 1

        return f

    def create_boolean(self):
        b = BooleanParameter()
        b.name += str(self.__boolean_counter)
        self.__boolean_counter += 1

        return b


def thread(func):
    """
    Декоратор для выполнения функций в потоке.
    """

    def wrapper(*args, **kwargs):
        t = threading.Thread(target=func, args=args)
        t.start()
    return wrapper


class PseudoController:
    """
    Псевдо-контроллер.
    """

    def __init__(self, ip='127.0.0.1', port=5000):
        self.ip = ip
        self.port = port
        self.connection = None
        self.parameters = []
        self.updating = False
        self.__selector = None
        self.__address_counter = 1
        self.__queues = {}
        self.__run_socket = False

    @property
    def queues(self):
        return self.__queues         

    def add_parameter(self, parameter):
        """
        Добавление параметра.
        """

        parameter.address = self.__address_counter
        self.__address_counter += 1
        self.parameters.append(parameter)  

    @thread
    def run_updating(self, period=5):
        """
        Запуск периодического изменения значений параметров.

        period - период изменения, сек.
        """

        if self.updating == True:
            return

        self.updating = True
        
        while self.updating == True:
            for p in self.parameters:
                if random.choice([True, False]):
                    _value = p.value
                    p.randomize_value()

                    if p.value != _value:
                        message = p.get_message()
                        for key, q in self.__queues.items():
                            q.put(message) 

            time.sleep(period)

    def stop_updating(self):
        """
        Остановка периодического изменения значений параметров.
        """

        self.updating = False

    @thread
    def start_socket(self):
        """
        Запуск сокет-сервера.
        """

        self.__selector = selectors.DefaultSelector()
        self.__run_socket = True
        sel = self.__selector

        host, port = self.ip, self.port
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind((host, port))
        lsock.listen()
        lsock.setblocking(False)
        sel.register(lsock, selectors.EVENT_READ, data=None)

        def _accept(sock):
            """
            Приём соединения.
            """

            conn, addr = sock.accept()
            conn.setblocking(False)
            data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
            events = selectors.EVENT_READ | selectors.EVENT_WRITE
            sel.register(conn, events, data=data)
            self.__queues[addr] = queue.Queue()

        def _serve_connection(key, mask):
            """
            Отправка данных по параметрам клиентам.
            """

            sock = key.fileobj
            data = key.data

            if mask & selectors.EVENT_READ:
                recv_data = sock.recv(1024)
                if recv_data:
                    data.outb += recv_data
                else:
                    self.__queues.pop(sock.getpeername())
                    sel.unregister(sock)
                    sock.close()                    

            if mask & selectors.EVENT_WRITE:
                try:
                    data.outb += self.__queues[sock.getpeername()].get_nowait()
                except queue.Empty:
                    pass

                if data.outb:
                    sent = sock.send(data.outb)
                    data.outb = data.outb[sent:]

        while self.__run_socket == True:
            events = sel.select(timeout=0)
            for key, mask in events:
                if key.data is None:
                    _accept(key.fileobj)
                else:
                    try:
                        _serve_connection(key, mask)
                    except OSError:
                        pass

        events = sel.select(timeout=0)
        for key, mask in events:
            sock = key.fileobj
            self.__queues.pop(sock.getpeername())
            sel.unregister(sock)
            sock.close()

        sel.close()

        self.__selector = None

    def stop_socket(self):
        """
        Остановка сокет-сервера.
        """

        if self.__run_socket == False or self.__selector is None:
            return

        self.__run_socket = False


def get_test_plc():
    """
    Создание тестового контроллера.
    """

    fabric = ParameterFabric()
    plc = PseudoController()

    plc.add_parameter(fabric.create_integer())
    plc.add_parameter(fabric.create_float())
    plc.add_parameter(fabric.create_integer())
    plc.add_parameter(fabric.create_float())
    plc.add_parameter(fabric.create_integer())
    plc.add_parameter(fabric.create_float())
    plc.add_parameter(fabric.create_integer())
    plc.add_parameter(fabric.create_float())
    plc.add_parameter(fabric.create_boolean())
    plc.add_parameter(fabric.create_boolean())

    return plc
