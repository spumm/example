#!/usr/bin/python3.6
"""
Этот класс собирает данные с двоичных файлов РТП-1. Файлы располагаются на FTP сервере, на сервере РТП-1.
Логин, пароль и директория с файлами - задаются в settings.ini
Подробнее структура файлов рассмотрена в интрукции.
Вызывать метод read() согласно графику добавления данных в двоичный файл:
С 00:00 до 08:00 - 1 раз, для добавления данных с за прошлый день с 16:00 до 23:59
С 08:00 до 09:00 - 1 раз, добавления данных с 00:00 до 07:59 текущего дня
С 09:00 в середине каждого часа, до добавления данных ежечасно в течении дня, до 17:00.
"""
import datetime, logging
from ftplib import FTP
import struct


class RTP1:
    def __init__(self, config):
        self.config = config

    def read(self, date=None):
        try:
            # Определяем диапазон возвращаемых значений
            try:
                if date is None:
                    date = datetime.datetime.now()
                    run_hour = date.hour
                    if run_hour < 8:
                        date = date - datetime.timedelta(days=1)
                    date = date.replace(hour=0, minute=0, second=0, microsecond=0)
                    file_name = date.strftime('%d-%m-%Y')
                else:
                    run_hour = None
                    date = date.split('-')
                    date = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), 0, 0, 0)
                    file_name = date.strftime('%d-%m-%Y')
                print('_' * 30)
                print('Подключение к FTP-серверу РТП-1.')
                ftp = FTP(self.config['RTP-1']['host'], self.config['RTP-1']['user'], self.config['RTP-1']['password'])
                ftp.cwd(self.config['RTP-1']['dir'])
                with open('temp/rtp1.bin', 'wb') as rtp1bin:
                    print('Копирование файла.')
                    ftp.retrbinary('RETR ' + file_name, rtp1bin.write)
                    ftp.close()
                rtp1bin.close()
                print('Копирование завершено.')
            except OSError:
                print('Ошибка копирования.')
                logging.exception('Copy file failure')
                return 'error'

            try:
                with open('temp/rtp1.bin', 'rb') as rtp1bin:
                    line_number = [15, 21, 31, 32, 33, 34, 35, 36, 50, 51, 52, 253, 254, 255, 78, 79, 80, 84,
                                   85, 86, 74, 75, 76, 120, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201,
                                   202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216,
                                   217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231,
                                   232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246,
                                   247, 248, 249, 250, 251, 252, 117, 118, 119, 325, 164, 344, 369, 377, 378,
                                   379, 385, 386, 387, 125, 126, 128, 425, 426, 427, 87, 88, 89, 347, 348, 349,
                                   49, 428, 429, 430, 431, 432, 433, 484, 485, 468, 469, 470]
                    line_number.reverse()
                    field_name = ['datetime_graph', 'akt', 'reakt', 'ftab', 'ftbc', 'ftca', 'lnab', 'lnbc', 'lnca', 'te1',
                                'te2', 'te3', 'pe1', 'pe2', 'pe3', 'an1', 'an2', 'an3', 'am1', 'am2', 'am3', 'sopr1',
                                'sopr2', 'sopr3', 'soprv', 'tp1', 'tp2', 'tp3', 'tk1', 'tk2', 'tk3', 'tg1', 'tg2',
                                'tg3', 'tg4', 'tv1', 'tv2', 'tv3', 'tch1_1', 'tch1_2', 'tch1_3', 'tch1_4', 'tch2_1',
                                'tch2_2', 'tch2_3', 'tch2_4', 'tch3_1', 'tch3_2', 'tch3_3', 'tch3_4', 'tcht1_1',
                                'tcht1_2', 'tcht1_3', 'tcht1_4', 'tcht1_5', 'tcht1_6', 'tcht1_7', 'tcht1_8', 'tcht2_1',
                                'tcht2_2', 'tcht2_3', 'tcht2_4', 'tcht2_5', 'tcht2_6', 'tcht2_7', 'tcht2_8', 'tcht3_1',
                                'tcht3_2', 'tcht3_3', 'tcht3_4', 'tcht3_5', 'tcht3_6', 'tcht3_7', 'tcht3_8', 'tplk1_1',
                                'tplk1_2', 'tplk2_1', 'tplk2_2', 'tplk3_1', 'tplk3_2', 'tgp1_1', 'tgp1_2', 'tgp2_1',
                                'tgp2_2', 'tgp3_1', 'tgp3_2', 'pel1', 'pel2', 'pel3', 'ww', 'rp', 'q', 'ar', 'ar1',
                                'ar2', 'ar3', 'toe1', 'toe2', 'toe3', 'rv1', 'rv2', 'rv3', 'q1', 'q2', 'q3', 'e1', 'e2',
                                'e3', 'i1', 'i2', 'i3', 'x', 'q1c', 'q2c', 'q3c', 'e1c', 'e2c', 'e3c', 'm1', 'm2',
                                'kiu1', 'kiu2', 'kiu3']

                    # Читанием двоичный файл и формирум список словарей для csv файла
                    try:
                        tab_csv = [{x: 0 for x in field_name} for _ in range(1440)]
                        for fl in field_name:
                            if fl == 'datetime_graph':
                                for i in range(1440):
                                    tmpdt = date + datetime.timedelta(minutes=i)
                                    tab_csv[i][fl] = tmpdt.strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                ln_nm = line_number.pop()
                                rtp1bin.seek(ln_nm * 5760)
                                for i in range(1440):
                                    var = struct.unpack('=f', rtp1bin.read(4))
                                    tab_csv[i][fl] = round(var[0], 2)
                    except Exception:
                        print('Ошибка чтения двоичного файла.')
                        logging.exception('Reading binary file error. Take file from RTP-1')
                        return
                    """
                    В начале суток весь файл забить предельно максимальными значениями.
                    Если АРМ и сервер теряют связь, то в этот период значения не меняются пишутся, и остаются 1*E18
                    Что негативно сказывается на графиках. Добавляем проверку, что бы изменяла эти значения на 0.                    
                    """
                    for row in tab_csv:
                        for each in row:
                            if row[each] == 9.999999680285692e+37:
                                row[each] = 0
                    # Возвращяем нужный временной срез.
                    if run_hour is None:
                        return tab_csv
                    elif run_hour < 8:
                        return tab_csv[960:]
                    elif run_hour == 8:
                        return tab_csv[:480]
                    elif run_hour == 9:
                        return tab_csv[480:540]
                    elif run_hour == 10:
                        return tab_csv[540:600]
                    elif run_hour == 11:
                        return tab_csv[600:660]
                    elif run_hour == 12:
                        return tab_csv[660:720]
                    elif run_hour == 13:
                        return tab_csv[720:780]
                    elif run_hour == 14:
                        return tab_csv[780:840]
                    elif run_hour == 15:
                        return tab_csv[840:900]
                    elif run_hour == 16:
                        return tab_csv[900:960]
            except Exception:
                logging.exception('Ошибка при работе с двоичным файлом.')

        except Exception:
            print("Общая ошибка модуля")
            logging.exception('General error')
