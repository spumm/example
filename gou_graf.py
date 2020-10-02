#!/usr/bin/python3.6
"""
Этот модуль собирает данные технологических параметров ГОУ с MySQL серверов, расположенных на пультах оператора ГОУ.
Для работы этого модуля должен быть установлен ODBC драйвер MySQL ODBC 3.51 Driver!
И окружение должно быть х86-32 для работы с этим драйвером
"""
import pyodbc
from contextlib import closing
import datetime
import logging
import itertools


class GOUGraf:
    def __init__(self, config):
        self.config = config

    def read(self, date=None):
        # Список таблиц на db.bzf.asu
        pg_table_list = [
                        # GOT1_1_
                        'gou11_c401','gou11_c402','gou11_c404','gou11_c403',
                        'gou11_cai101','gou11_cai102','gou11_cai103','gou11_cai104','gou11_cai105','gou11_cai107','gou11_cai108','gou11_cai109',
                        'gou12_cai117','gou12_cai118','gou12_cai119','gou12_cai120','gou12_cai122','gou12_cai123','gou12_cai125','gou12_cai126',
                        # GOT3_1_
                        'gou13_c401','gou13_c402','gou13_c403','gou13_c404','gou13_cai113',
                        'gou13_cai101','gou13_cai102','gou13_cai103','gou13_cai104','gou13_cai105','gou13_cai106','gou13_cai107','gou13_cai108','gou13_cai109',
                        'gou1_cai115','gou1_cai116',
                        # GOT1_2_
                        'gou21_c401','gou21_c402','gou21_c404','gou21_c403','gou21_cai113',
                        'gou21_G1_cai105','gou21_cai101','gou21_cai102','gou21_cai103','gou21_cai104','gou21_cai107','gou21_cai108','gou21_cai109',
                        'gou22_cai117','gou22_cai118','gou22_cai119','gou22_cai120','gou22_cai122','gou22_cai123','gou22_cai125','gou22_cai126',
                        # GOT3_2_
                        'gou23_c401' ,'gou23_c402','gou23_c403','gou23_c404','gou23_cai113',
                        'gou23_cai101','gou23_cai102','gou23_cai103','gou23_cai104','gou23_cai105','gou23_cai107','gou23_cai108','gou23_cai109',
                        'gou2_cai115','gou2_cai116',
                        # GOT1_
                        'gou41_c401','gou41_c402','gou41_c404','gou41_c403','gou41_cai113',
                        'gou41_cai105','gou41_cai101','gou41_cai102','gou41_cai103','gou41_cai104','gou41_cai106','gou41_cai107','gou41_cai108','gou41_cai109',
                        'gou42_cai117','gou42_cai118','gou42_cai119','gou42_cai120','gou42_cai122','gou42_cai123','gou42_cai125','gou42_cai126',
                        # GOT3_
                        'gou43_c401' ,'gou43_c402','gou43_c403','gou43_c404','gou43_cai113',
                        'gou43_cai101','gou43_cai102','gou43_cai103','gou43_cai104','gou43_cai105','gou43_cai107','gou43_cai108','gou43_cai109',
                        'gou4_cai115','gou4_cai116',

                        'gou31_cai129','gou31_cai130','gou31_cai131','gou31_cai132','gou31_cai137',
                        'gou33_cai133','gou33_cai134','gou33_cai135','gou33_cai136','gou33_cai138',
                        'gou32_cai117','gou32_cai118','gou32_cai119','gou32_cai120',
                        'gou3_cai115','gou3_cai116']
        # Список таблиц на серверах ГОУ
        gou_table_name = [
                        # gou1-arm.bzf.asu
                        'c401', 'c402', 'c404', 'c403',
                        'cai101', 'cai102', 'cai103', 'cai104', 'cai105', 'cai107', 'cai108', 'cai109',
                        'cai117', 'cai118', 'cai119', 'cai120', 'cai122', 'cai123', 'cai125', 'cai126',

                        'c401', 'c402', 'c403', 'c404', 'cai113',
                        'cai101', 'cai102', 'cai103', 'cai104', 'cai105', 'cai106', 'cai107', 'cai108', 'cai109',
                        'cai115', 'cai116',
                        # gou2-arm.bzf.asu
                        'c401', 'c402', 'c404', 'c403', 'cai113',
                        'G1_cai105', 'cai101', 'cai102', 'cai103', 'cai104', 'cai107', 'cai108', 'cai109',
                        'cai117', 'cai118', 'cai119', 'cai120', 'cai122', 'cai123', 'cai125', 'cai126',

                        'c401', 'c402', 'c403', 'c404', 'cai113',
                        'cai101', 'cai102', 'cai103', 'cai104', 'cai105', 'cai107', 'cai108', 'cai109',
                        'cai115', 'cai116',
                        # gou4-arm.bzf.asu
                        'c401', 'c402', 'c404', 'c403', 'cai113',
                        'cai105', 'cai101', 'cai102', 'cai103', 'cai104', 'cai106', 'cai107', 'cai108', 'cai109',
                        'cai117', 'cai118', 'cai119', 'cai120', 'cai122', 'cai123', 'cai125', 'cai126',

                        'c401', 'c402', 'c403', 'c404', 'cai113',
                        'cai101', 'cai102', 'cai103', 'cai104', 'cai105', 'cai107', 'cai108', 'cai109',
                        'cai115', 'cai116',

                        'cai129', 'cai130', 'cai131', 'cai132', 'cai137',
                        'cai133', 'cai134', 'cai135', 'cai136', 'cai138',
                        'cai117', 'cai118', 'cai119', 'cai120',
                        'cai115', 'cai116']
        try:
            # big_csv_tab - словарь, ключами которого явлюятся элементы table_list.
            # Содержит данные со всех и серверов и отправляет всё в return
            big_csv_tab = {}
            if date is None:
                date = datetime.datetime.now()-datetime.timedelta(days=1)
            else:
                date = date.split("-")
                date = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), 0, 0, 0)
            it = itertools.count()  # счётчик для навгиации по pg_table_list

            """ 
            Для навигации по именам таблиц, используются слайсы со списка gou_table_name
            Происходит итерация по слайсу, и на каждой итерации прибавляется счётчик it.
            Далее в словарь big_csv_tab добавлятся csv_tab, по ключу  pg_table_list[it]
            """
            # Первая газоочистка GOT1_1
            try:
                dbname = 'GOT1_1_' + date.strftime('%Y') + '_' + date.strftime('%m') + '_' + date.strftime('%d')
                with closing(pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};'
                                            'SERVER=' + self.config['GOU']['gou1'] + ';'
                                            'DATABASE=' + dbname + ';'
                                            'User=' + self.config['GOU']['gou_user'] + ';'
                                            'PASSWORD=' + self.config['GOU']['gou_pass'] + ';', autocommit=True)) as connection:
                    with connection.cursor() as cursor:
                        print('Собираем данные с БД ' + dbname + ' газоочистки №1')
                        for each_table in gou_table_name[0:20]:
                            query = 'SELECT time as time_gp, value as value_gp FROM ' + each_table
                            cursor.execute(query)
                            field_name = [column[0] for column in cursor.description]
                            csv_tab = []
                            for row in cursor.fetchall():
                                csv_tab.append(dict(zip(field_name, row)))
                            for row in csv_tab:
                                row['date_gp'] = date.strftime('%Y-%m-%d')
                                row['fild_gp'] = each_table
                            big_csv_tab[pg_table_list[next(it)]] = csv_tab
            except pyodbc.DatabaseError:
                print('Ошибка подключения к базе данных ГОУ-1')
                logging.exception('Database error')
            except Exception:
                print('Общая ошибка при работе с базой данных')
                logging.exception('Database error')

            # Первая газоочистка GOT3_1
            try:
                dbname = 'GOT3_1_' + date.strftime('%Y') + '_' + date.strftime('%m') + '_' + date.strftime('%d')
                with closing(pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};'
                                            'SERVER=' + self.config['GOU']['gou1'] + ';'
                                            'DATABASE=' + dbname + ';'
                                            'User=' + self.config['GOU']['gou_user'] + ';'
                                            'PASSWORD=' + self.config['GOU']['gou_pass'] + ';', autocommit=True)) as connection:
                    with connection.cursor() as cursor:
                        print('Собираем данные с БД ' + dbname + ' газоочистки №1')
                        for each_table in gou_table_name[20:36]:
                            query = 'SELECT time as time_gp, value as value_gp FROM ' + each_table
                            cursor.execute(query)
                            field_name = [column[0] for column in cursor.description]
                            csv_tab = []
                            for row in cursor.fetchall():
                                csv_tab.append(dict(zip(field_name, row)))
                            for row in csv_tab:
                                row['date_gp'] = date.strftime('%Y-%m-%d')
                                row['fild_gp'] = each_table
                            big_csv_tab[pg_table_list[next(it)]] = csv_tab
            except pyodbc.DatabaseError:
                print('Ошибка подключения к базе данных ГОУ-1')
                logging.exception('Database error')
            except Exception:
                print('Общая ошибка при работе с базой данных')
                logging.exception('Database error')

            # Вторая газоочистка GOT1_2
            try:
                dbname = 'GOT1_2_' + date.strftime('%Y') + '_' + date.strftime('%m') + '_' + date.strftime('%d')
                with closing(pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};'
                                            'SERVER=' + self.config['GOU']['gou2'] + ';'
                                            'DATABASE=' + dbname + ';'
                                            'User=' + self.config['GOU']['gou_user'] + ';'
                                            'PASSWORD=' + self.config['GOU']['gou_pass'] + ';', autocommit=True)) as connection:
                    with connection.cursor() as cursor:
                        print('Собираем данные с БД ' + dbname + ' газоочистки №2')
                        for each_table in gou_table_name[36:57]:
                            query = 'SELECT time as time_gp, value as value_gp FROM ' + each_table
                            cursor.execute(query)
                            field_name = [column[0] for column in cursor.description]
                            csv_tab = []
                            for row in cursor.fetchall():
                                csv_tab.append(dict(zip(field_name, row)))
                            for row in csv_tab:
                                row['date_gp'] = date.strftime('%Y-%m-%d')
                                row['fild_gp'] = each_table
                            big_csv_tab[pg_table_list[next(it)]] = csv_tab
            except pyodbc.DatabaseError:
                print('Ошибка подключения к базе данных ГОУ-2')
                logging.exception('Database error')
            except Exception:
                print('Общая ошибка при работе с базой данных')
                logging.exception('Database error')

            # Вторая газоочистка GOT3_2
            try:
                dbname = 'GOT3_2_' + date.strftime('%Y') + '_' + date.strftime('%m') + '_' + date.strftime('%d')
                with closing(pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};'
                                            'SERVER=' + self.config['GOU']['gou2'] + ';'
                                            'DATABASE=' + dbname + ';'
                                            'User=' + self.config['GOU']['gou_user'] + ';'
                                            'PASSWORD=' + self.config['GOU']['gou_pass'] + ';', autocommit=True)) as connection:
                    with connection.cursor() as cursor:
                        print('Собираем данные с БД ' + dbname + ' газоочистки №2')
                        for each_table in gou_table_name[57:72]:
                            query = 'SELECT time as time_gp, value as value_gp FROM ' + each_table
                            cursor.execute(query)
                            field_name = [column[0] for column in cursor.description]
                            csv_tab = []
                            for row in cursor.fetchall():
                                csv_tab.append(dict(zip(field_name, row)))
                            for row in csv_tab:
                                row['date_gp'] = date.strftime('%Y-%m-%d')
                                row['fild_gp'] = each_table
                            big_csv_tab[pg_table_list[next(it)]] = csv_tab
            except pyodbc.DatabaseError:
                print('Ошибка подключения к базе данных ГОУ-2')
                logging.exception('Database error')
            except Exception:
                print('Общая ошибка при работе с базой данных')
                logging.exception('Database error')

            # Четвёртая газоочистка GOT1
            try:
                dbname = 'GOT1_' + date.strftime('%Y') + '_' + date.strftime('%m') + '_' + date.strftime('%d')
                with closing(pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};'
                                            'SERVER=' + self.config['GOU']['gou4'] + ';'
                                            'DATABASE=' + dbname + ';'
                                            'User=' + self.config['GOU']['gou_user'] + ';'
                                            'PASSWORD=' + self.config['GOU']['gou_pass'] + ';', autocommit=True)) as connection:
                    with connection.cursor() as cursor:
                        print('Собираем данные с БД ' + dbname + ' газоочистки №4')
                        for each_table in gou_table_name[72:94]:
                            query = 'SELECT time as time_gp, value as value_gp FROM ' + each_table
                            cursor.execute(query)
                            field_name = [column[0] for column in cursor.description]
                            csv_tab = []
                            for row in cursor.fetchall():
                                csv_tab.append(dict(zip(field_name, row)))
                            for row in csv_tab:
                                row['date_gp'] = date.strftime('%Y-%m-%d')
                                row['fild_gp'] = each_table
                            big_csv_tab[pg_table_list[next(it)]] = csv_tab
            except pyodbc.DatabaseError:
                print('Ошибка подключения к базе данных ГОУ-4')
                logging.exception('Database error')
            except Exception:
                print('Общая ошибка при работе с базой данных')
                logging.exception('Database error')

            # Четвёртая газоочистка GOT3
            try:
                dbname = 'GOT3_' + date.strftime('%Y') + '_' + date.strftime('%m') + '_' + date.strftime('%d')
                with closing(pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};'
                                            'SERVER=' + self.config['GOU']['gou4'] + ';'
                                            'DATABASE=' + dbname + ';'
                                            'User=' + self.config['GOU']['gou_user'] + ';'
                                            'PASSWORD=' + self.config['GOU']['gou_pass'] + ';', autocommit=True)) as connection:
                    with connection.cursor() as cursor:
                        print('Собираем данные с БД ' + dbname + ' газоочистки №4')
                        for each_table in gou_table_name[94:]:
                            query = 'SELECT time as time_gp, value as value_gp FROM ' + each_table
                            cursor.execute(query)
                            field_name = [column[0] for column in cursor.description]
                            csv_tab = []
                            for row in cursor.fetchall():
                                csv_tab.append(dict(zip(field_name, row)))
                            for row in csv_tab:
                                row['date_gp'] = date.strftime('%Y-%m-%d')
                                row['fild_gp'] = each_table
                            big_csv_tab[pg_table_list[next(it)]] = csv_tab
            except pyodbc.DatabaseError:
                print('Ошибка подключения к базе данных ГОУ-4')
                logging.exception('Database error')
            except Exception:
                print('Общая ошибка при работе с базой данных')
                logging.exception('Database error')
            return big_csv_tab
        except Exception:
            print('Общая ошибка модуля. Смотретите журнал')
            logging.exception('General error')


"""Можно вынести вынести код каждого подключения в отдельную функцию и передавать 3 параметра: адрес сервер,
слайс списка gou_table_name и имя базы данных. Но пока что лень,и так отлично работает."""
