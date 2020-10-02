#!/usr/bin/python3.6
"""
Этот модуль собирает данные по навескам с MySQL сервера, пульта дозировочного отделения.
Для работы этого модуля должен быть установлен ODBC драйвер MySQL ODBC 3.51 Driver!
И окружение должно быть х86-32 для работы с этим драйвером
"""
import pyodbc
from contextlib import closing
import datetime
import logging


class DO:
    def __init__(self, config):
        self.config = config

    def read(self, date=None):
        try:
            print("Дозировочное отделение. Навески.")
            table = 'TECHNOLOG'
            if date is None:
                date = datetime.datetime.now() - datetime.timedelta(days=1)
                dbname = "Dozav2_" + date.strftime("%Y") + "_" + date.strftime("%m")
                query = "SELECT * FROM " + table + " WHERE Data = '" + date.strftime("%d.%m.%Y") + "'"
            else:
                date = date.split("-")
                date = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), 0, 0, 0)
                dbname = "Dozav2_" + date.strftime("%Y") + "_" + date.strftime("%m")
                query = "SELECT * FROM " + table + " WHERE Data = '" + date.strftime("%d.%m.%Y") + "'"
            try:
                with closing(pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};'
                             'SERVER=' + self.config['DO']['host'] + ';'
                             'DATABASE=' + dbname + ';'
                             'User=' + self.config['DO']['user']+';'
                             'PASSWORD=' + self.config['DO']['PASSWORD'] + ';',autocommit=True)) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(query)
                        # Формируем список словарей для pgwrite.transaction()
                        field_name = [column[0] for column in cursor.description]
                        csv_tab = []
                        for row in cursor.fetchall():
                            csv_tab.append(dict(zip(field_name, row)))
                        for row in csv_tab:
                            row['Data'] = date.strftime('%Y-%m-%d')
                        return csv_tab
            except pyodbc.DatabaseError:
                logging.exception("Ошибка при подключении к базе данных")
            except Exception:
                logging.exception('Общая ошибка при работе с базой данных')
        except Exception:
            logging.exception("General error")
            print("Общая ошибка модуля. Смотрите журнал.")
