# -*- coding: utf-8 -*-

import os
import queue
from collections import defaultdict
from multiprocessing import Process, Queue


class FileParser(Process):
    """
    Use python3.7

    Парсит файлы "*.csv" с итогами торгов, вычисляет процент волатильности тикера.
    Каждый файл содержит информацию об одном тикере.
    Передает результаты парсинга в очередь в виде тьюпла ('имя биржевого тикера', 'процент волатильности').

    """

    def __init__(self, parsing_result, files_for_scan, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.list_with_files = files_for_scan
        self.parsing_result = parsing_result

    def volatility_result(self, _min_price, _max_price):
        """
        Вычисяет процент волатильности тикера

        :param _min_price: минимальная цена за торговую сессию
        :param _max_price: максимальная цена за торговую сессию
        :return: процент волатильности тикера (float)

        """
        half_sum = (_max_price + _min_price) / 2
        volatility_for_ticker = ((_max_price - _min_price) / half_sum) * 100
        return round(volatility_for_ticker, 2)

    def run(self):
        """
        Передает в очередь результаты парсинга файла
        :return: ('имя биржевого тикера', 'процент волатильности') - tuple

        """
        current_file = self.list_with_files
        with open(file=current_file, mode='r', encoding='utf8') as ff:
            min_price, max_price = 0, 0
            for line in ff:
                _name, _time, price, quantity = line.strip().split(',')
                if not price.isalpha():
                    if min_price == 0:
                        min_price = float(price)
                    if float(price) > max_price:
                        max_price = float(price)
                    elif float(price) < min_price:
                        min_price = float(price)
            volatility = self.volatility_result(_min_price=min_price, _max_price=max_price)
            final_data_from_file = (_name, volatility)
            self.parsing_result.put(final_data_from_file)


class VolatilityCounter(Process):
    """
    Use python3.7

    Вычисляет тикеры с максимальной, минимальной и нулевой волатильностью.

        Результаты выводит на консоль в виде:
      Максимальная волатильность:
          ТИКЕР1 - ХХХ.ХХ %
          ТИКЕР2 - ХХХ.ХХ %
          ТИКЕР3 - ХХХ.ХХ %
      Минимальная волатильность:
          ТИКЕР4 - ХХХ.ХХ %
          ТИКЕР5 - ХХХ.ХХ %
          ТИКЕР6 - ХХХ.ХХ %
      Нулевая волатильность:
          ТИКЕР7, ТИКЕР8, ТИКЕР9, ТИКЕР10, ТИКЕР11, ТИКЕР12

    """

    def __init__(self, _dir=None, ticker_quantity=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dir = 'trades' if _dir is None else _dir
        self.full_dir_path = os.path.join(os.path.dirname(__file__), self.dir) if _dir is None \
            else os.path.normpath(_dir)
        self.list_with_files = []
        self.result_sort_list = []
        self.parsers = []
        self.volatility_for_print = defaultdict(list)
        self.volatility_for_print['Максимальная волатильность:'] = []
        self.volatility_for_print['Минимальная волатильность:'] = []
        self.volatility_for_print['Нулевая волатильность:'] = []
        self.get_parsing_result = Queue(maxsize=5)
        self.send_files_paths = Queue(maxsize=5)
        self.parsing_results = defaultdict(int)
        self.ticker_quantity = 3 if ticker_quantity is None else ticker_quantity

    def scan_folder(self):
        """
        Сканирует папку с файлами "*.csv" и сохраняет полные пути к файлам.
        """
        if os.path.isdir(self.full_dir_path):
            for directory, dir, file in os.walk(self.full_dir_path):
                if any(file):
                    for _file in file:
                        if _file.endswith('.csv'):
                            full_file_path = os.path.join(directory, _file)
                            self.list_with_files.append(full_file_path)
                        else:
                            print(f'{_file} - неизвестный формат файла. Работаю только с "*.csv"')
                else:
                    raise Exception('Папка пуста или не содержит файлов формата "*.csv"!')
        else:
            raise Exception('Нет такой директории!')

    def add_parser(self, files):
        """
        Создает список из экземпляров класса FileParser.
        """
        parser = FileParser(files_for_scan=files, parsing_result=self.get_parsing_result)
        self.parsers.append(parser)

    def get_sorted_volatility_results(self):
        """
        Создает отсортированный список из результатов парсинга файлов.
        """
        self.result_sort_list = list(self.parsing_results.items())
        self.result_sort_list.sort(key=lambda volatility: volatility[1], reverse=True)

    def get_statistic(self):
        """
        Добавлет в словарь по разделам тикеры с максимальной, минимальной и нулевой волатильностью.
        Количество тикеров определяется атрибутом класса self.ticker_quantity (передается при инициализации).

        """
        count = self.ticker_quantity

        if len(self.result_sort_list) > self.ticker_quantity * 3:
            for _index in range(self.ticker_quantity):
                ticker = self.result_sort_list[_index][0]
                volatility = self.result_sort_list[_index][1]
                self.volatility_for_print['Максимальная волатильность:'].append((ticker, volatility))
            for _index in range(len(self.result_sort_list) - 1, 0, -1):
                ticker = self.result_sort_list[_index][0]
                volatility = self.result_sort_list[_index][1]
                if count > 0:
                    if volatility > 0:
                        self.volatility_for_print['Минимальная волатильность:'].append((ticker, volatility))
                        count -= 1
                    elif volatility == 0:
                        self.volatility_for_print['Нулевая волатильность:'].append(ticker)
                else:
                    break

        self.volatility_for_print['Нулевая волатильность:'].sort()
        self.volatility_for_print['Минимальная волатильность:'].sort(key=lambda _index: _index[1], reverse=True)

    def print_result(self):
        """
        Выводит на консоль результаты.

        """
        if len(self.result_sort_list) > self.ticker_quantity * 3:
            for element in self.volatility_for_print:
                print(element)
                if 'Нулевая' in element:
                    for value in self.volatility_for_print[element]:
                        if value == self.volatility_for_print[element][-1]:
                            print(f'{value}', end='')
                        elif value == self.volatility_for_print[element][0]:
                            print(f'\t{value}', end=', ')
                        else:
                            print(f'{value}', end=', ')
                else:
                    for number, value in enumerate(self.volatility_for_print[element]):
                        print(f'\t{value[0]} - {value[1]} %')
        else:
            for element in self.result_sort_list:
                print(f'{element[0]} - {element[1]} %')

    def run(self):
        try:
            self.scan_folder()
        except Exception as exc:
            print(exc)

        if any(self.list_with_files):
            for file in self.list_with_files:
                self.add_parser(files=file)
            for parser in self.parsers:
                parser.start()
            while True:
                try:
                    parsing_result = self.get_parsing_result.get(timeout=1)
                    ticker_name, volatility = parsing_result[0], parsing_result[1]
                    self.parsing_results[ticker_name] = volatility
                except queue.Empty:
                    if not any(parser.is_alive() for parser in self.parsers):
                        break
            for parser in self.parsers:
                parser.join()

        self.get_sorted_volatility_results()
        self.get_statistic()
        self.print_result()


if __name__ == '__main__':
    counter = VolatilityCounter()
    counter.start()
