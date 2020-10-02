#!/usr/bin/python3.7
"""
Этот модуль собирает данные технологические параметры дозировочного отделения с MySQL сервера,
расположенного на пульте оператора дозировочного отделения.
Для работы этого модуля должен быть установлен ODBC драйвер MySQL ODBC 3.51 Driver!
И окружение должно быть х86-32 для работы с этим драйвером
"""
import pyodbc
import datetime
import logging
from contextlib import closing


class DOGraf:
    def __init__(self, config):
        self.config = config

    def read(self, date=None, table=None):
        if table is None:
            table_list = ['fqn2_638_2', 'fqn1_637_2', 'fqn3_639_2', 'fqn4_640_2', 'fqn5_641_2', 'fqn6_642_2', 'fqn7_643_2',
                          'fqn2_638_3', 'fqn1_637_3', 'fqn3_639_3', 'fqn4_640_3', 'fqn5_641_3', 'fqn6_642_3', 'fqn7_643_3',
                          'fdelta1_671_2', 'fdelta2_672_2', 'fdelta3_673_2', 'fdelta4_674_2', 'fdelta5_675_2',
                          'fdelta6_676_2', 'fdelta7_677_2', 'fUt1_616_2', 'fUt2_617_2', 'fUt3_618_2', 'fUt4_619_2',
                          'fUt5_620_2', 'fUt6_621_2', 'fUt7_622_2', 'fUt1_616_3', 'fUt2_617_3', 'fUt3_618_3', 'fUt4_619_3',
                          'fUt5_620_3', 'fUt6_621_3', 'fUt7_622_3', 'I10_corr0', 'I11_corr0', 'I12_corr0', 'I13_corr0',
                          'I14_corr0', 'I8_corr0', 'I9_corr0', 'I19_corr0', 'I15_corr0', 'I16_corr0', 'I17_corr0',
                          'I18_corr0', 'I20_corr0', 'I21_corr0', 'fKadop3', 'fKadop2', 'Mcur_10', 'Mcur_11', 'Mcur_14',
                          'Mcur_13', 'Mcur_12', 'Mcur_8', 'Mcur_9', 'Mcur_15', 'Mcur_16', 'Mcur_17', 'Mcur_18', 'Mcur_19',
                          'Mcur_20', 'Mcur_21', 'fDcur_11', 'fDcur_12', 'fDcur_13', 'fDcur_14', 'fDcur_15', 'fDcur_16',
                          'fDcur_17', 'coN2_810_1', 'coN1_809_1', 'coN3_811_1', 'coN4_812_1', 'coN5_813_1', 'coN6_814_1',
                          'coN7_815_1', 'dDcur_11_1043', 'dDcur_12_1044', 'dDcur_13_1045', 'dDcur_14_1046', 'dDcur_15_1047',
                          'dDcur_16_1048', 'dDcur_17_1049']
        else:
            table_list = [table]
        try:
            print('Дозировочное отделение. Технологические параметры')
            if date is None:
                date = datetime.datetime.now()-datetime.timedelta(days=1)
            else:
                date = date.split("-")
                date = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), 0, 0, 0)

            dbname = "Dozav2_" + date.strftime("%Y") + "_" + date.strftime("%m") + "_" + date.strftime("%d")
            big_csv_tab = []    # резульитрующая таблица. трехмерный массив
            print('Подключение к базе данных')
            try:
                with closing(pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};'
                                            'SERVER=' + self.config['DO']['host'] + ';'
                                            'DATABASE=' + dbname + ';'
                                            'User=' + self.config['DO']['user'] + ';'
                                            'PASSWORD=' + self.config['DO']['PASSWORD'] + ';', autocommit=True)) as connection:
                    with connection.cursor() as cursor:
                        print('Копирование данных')
                        for each_table in table_list:
                            print('.', flush=True, end='')
                            query = 'SELECT time as time_gp, value as value_gp FROM ' + each_table
                            cursor.execute(query)
                            # Формируем список словарей для pgwrite.transaction()
                            field_name = [column[0] for column in cursor.description]
                            csv_tab = []
                            for row in cursor.fetchall():
                                csv_tab.append(dict(zip(field_name, row)))
                            for row in csv_tab:
                                row['date_gp'] = date.strftime('%Y-%m-%d')
                                row['fild_gp'] = each_table
                            big_csv_tab.append(csv_tab)
                        print('\nДанные скопированы')
                        return big_csv_tab
            except pyodbc.DatabaseError:
                logging.exception('Ошибка при подключении к базе данных')
            except Exception:
                logging.exception('Общая ошибка при работе с базой данных')
        except Exception:
            print('Общая ошибка модуля. Смотрите журнал')
            logging.exception('General error')
