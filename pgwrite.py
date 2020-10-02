# !/usr/bin/python3.6
"""
pgwrite.py - попытка создать универсальный класс, для добавления данных в базу PgSQL
"""

import psycopg2
import logging
from contextlib import closing
import csv


class PgWriter:

    def __init__(self, config):
        self.config = config

    def transaction(self, data, pg_table, exp_file='default.txt', del_field=None):
        try:
            if len(data) == 0 or isinstance(data,type(None)):
                print('Нет данных')
                return
            # print("Подготавливаем CSV файл", end=' ')
            exp_name = exp_file     # На время работы старой БД
            exp_file = self.config['PGSERVER']['PATH'] + exp_file
            try:
                with open(exp_file, 'w') as csv_file:
                    csv_file.close()
            except Exception:
                print("Не возможно создать CSV файл на сервере db.bzf.asu")
            for row in data:
                if del_field in row:
                    row.pop(del_field)
            with open(exp_file, 'a', newline='', encoding='utf-8') as csv_file:
                csv_writer = csv.DictWriter(csv_file, delimiter=';', fieldnames=data[0].keys())
                csv_writer.writeheader()
                csv_writer.writerows(data)
            print("CSV файл создан.", end=' ')

            try:
                # print("Подключение к PostgreSQL", end=' ')
                with closing(psycopg2.connect(host=self.config['PGSERVER']['host'],
                                              user=self.config['PGSERVER']['user'],
                                              password=self.config['PGSERVER']['password'],
                                              dbname=self.config['PGSERVER']['db'])) as connection:
                    with connection.cursor() as cursor:
                        # print("Успешное подключение.", end=' ')
                        field_name = ''
                        flag = True
                        for each in data[0].keys():
                            if flag:
                                field_name = '"' + each + '"'
                                flag = False
                            else:
                                field_name = field_name + ',' + '"' + each + '"'
                        # query = "COPY " + pg_table + " (" + field_name + ") FROM '" + exp_file + "' DELIMITER ';' CSV HEADER ESCAPE '\\'"
                        exp_name = exp_name.replace('\\', '/')  # Для работы старой БД
                        query = "COPY " + pg_table + " (" + field_name + ") FROM '/share/CACHEDEV1_DATA/import/" + exp_name + "' DELIMITER ';' CSV HEADER ESCAPE '\\'" # Для работы старой БД
                        cursor.execute(query)
                        connection.commit()
                        print('Копирование данных выполено.')
            except psycopg2.DatabaseError:
                print("Проблемы при подключении.")
                logging.exception("Problem with connections:")
        except Exception:
            print('Общая ошибка. Смотрите журнал')
            logging.exception("General error")
