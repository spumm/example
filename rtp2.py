#!/usr/bin/python3.6
"""
Класс возвращяет данные с MySQL сервера РТР-2
В метод read() можно передать дату и время '2019-11-18 15:59:30' начала и конца выборки.
С поледней строки выборки, rows_id записывается в файл в settings.ini в 'lastid'
При вызове read() без аргументов, берется аргумент 'lastid' с settings.ini и делается выборка всех строк, чей rows_id больше lastid
Т.е. для простого добавления данных вызываем метод read() и он будет считывать все последние строки.
"""

import pymysql
import datetime, logging
from contextlib import closing
from pymysql.cursors import DictCursor


class RTP2:

    def __init__(self,config):
        self.config = config

    def read(self,start_time=None,end_time=None):
        try:
            wr_lastid = True
            print('_' * 30)
            print("\nПодключение к серверу РТП-2.")
            with closing(pymysql.connect(host=self.config['RTP-2']['host'],
                                         user=self.config['RTP-2']['user'],
                                         password=self.config['RTP-2']['password'],
                                         db=self.config['RTP-2']['db'],
                                         cursorclass=DictCursor)) as connection:
                with connection.cursor() as cursor:
                    if start_time is None and end_time is None:
                        now_h = datetime.datetime.now()
                        query = "SELECT * FROM rtp2 where id > " + self.config['RTP-2']['lastid'] + " AND datetime_graph < '" + now_h.strftime('%Y-%m-%d %H:00:00') + "' ORDER BY datetime_graph"
                    elif end_time is None:
                        end_time = datetime.datetime.now()
                        query = "SELECT * FROM rtp2 WHERE datetime_graph BETWEEN '" + start_time + "' AND '" + end_time.strftime('%Y-%m-%d %H:00:00') +"' ORDER BY datetime_graph"
                    else:
                        wr_lastid = False
                        query = "SELECT * FROM rtp2 WHERE datetime_graph BETWEEN '" + start_time + "' AND '" + end_time + "' ORDER BY datetime_graph"
                    cursor.execute(query)
                    print("Данные были получены с сервера")
                    try:
                        if cursor.rowcount and wr_lastid:
                            self.config.set('RTP-2','lastid',str(cursor._rows[-1]['id']))
                            with open('settings.ini','w') as fp:
                                self.config.write(fp)
                        else:
                            print("Новых данных нет.")
                            return "NULL"
                    except:
                        print(" В settings.ini отсутсвует параметр lastid")
                        logging.exception("Error")
                    data = cursor.fetchall()
                    cursor.close()
                    return data
        except Exception:
            print("Общая ошибка.")
            logging.exception("Error with database rtp-2")
