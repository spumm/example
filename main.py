#!/usr/bin/python3.6
"""
Формат даты задается по маске "2019-10-21"
Формат даты и время задается по маске "2019-10-21 00:00:00"
"""
import pgwrite, rtp4, rtp2, rtp1, doz, doz_graf, gou_graf, rtp4graph
import logging
import datetime, time
import os, sys, configparser, itertools
from contextlib import closing
import schedule


config = configparser.ConfigParser()
config.read('settings.ini')
trans = pgwrite.PgWriter(config)
date_now = datetime.datetime.now()
try:
    os.mkdir('temp')
except Exception:
    pass
if not os.path.exists("collector.log"):
    f = open('collector.log', 'w')
    f.close()
logging.basicConfig(filename='collector.log', level=logging.INFO, filemode="a",
                    format='%(asctime)s - %(module)s - %(message)s')


def show_menu():
    menu_list = ['daemon', 'rtp-1', 'rtp-2', 'rtp-3', 'rtp-3_graph', 'rtp-4', 'rtp-4_graph',
                 'do', 'do_graf', 'gou_graf', 'help', 'exit']
    print('Меню:')
    for x in menu_list:
        print(menu_list.index(x), ' -\t', x)
    print('Введите команду:', end='')
    cmd = input()
    return cmd


def daemon():
    print('\t\t\tДемонический режим >:]')
    # Добавляем задания согласно расписанию
    # schedule.every(1).seconds.do(collect_rtp1, True)     # Для отладки
    schedule.every().hour.at(':10').do(collect_rtp2, True)
    schedule.every().hour.at(':17').do(collect_rtp4_graph)
    schedule.every().hour.at(':33').do(collect_rtp3_graph)
    schedule.every().hour.at(':33').do(collect_rtp4)
    schedule.every().day.at('07:00').do(collect_do, True)
    schedule.every().day.at('02:00').do(collect_gou_graf, True)
    schedule.every().day.at('03:00').do(collect_do_graf, True)
    schedule.every().day.at('00:20').do(collect_rtp1, True)
    schedule.every().day.at('08:20').do(collect_rtp1, True)
    schedule.every().day.at('09:20').do(collect_rtp1, True)
    schedule.every().day.at('10:20').do(collect_rtp1, True)
    schedule.every().day.at('11:20').do(collect_rtp1, True)
    schedule.every().day.at('12:20').do(collect_rtp1, True)
    schedule.every().day.at('13:20').do(collect_rtp1, True)
    schedule.every().day.at('14:20').do(collect_rtp1, True)
    schedule.every().day.at('15:20').do(collect_rtp1, True)
    schedule.every().day.at('16:20').do(collect_rtp1, True)

    # Анимация точечек
    while True:
        print(date_now.now().strftime('%Y-%m-%d %H:%M:%S'), '\t wait', end="")
        it = itertools.cycle(['.'] * 3 + ['\b \b'] * 3)
        for x in range(240):
            time.sleep(.25)
            print(next(it), end='', flush=True)
        print('\r', end='', flush=True)
        schedule.run_pending()


def collect_rtp1(dmn=False):
    try:
        rtp = rtp1.RTP1(config)
        if dmn:
            print(date_now.now().strftime('%Y-%m-%d %H:%M:%S'))
            result = rtp.read()
        else:
            print('Введите дату в формате ' + datetime.datetime.now().strftime('%Y-%m-%d'))
            dt = input()
            if dt == '':
                dt = None
            result = rtp.read(dt)
        if result == 'error':
            print('Файл с указанной датой не найден или отсутсвует связь с сервером')
            return
        else:
            trans.transaction(result, config['RTP-1']['dst_table'], 'rtp1graph.csv')
    except Exception:
        print('Ошибка при выполнении')


def collect_rtp2(dmn=False):
    try:
        rtp = rtp2.RTP2(config)
        if dmn:
            print(date_now.now().strftime('%Y-%m-%d %H:%M:%S'))
            result = rtp.read()
            trans.transaction(result, config['RTP-2']['dst_table'], 'rtp2graph.csv', 'id')
        else:
            print('Введите дату начала выборки в формате ' + datetime.datetime.now().strftime('%Y-%m-%d'))
            start_time = input()
            if start_time == '':
                start_time = None
            print('Введите дату окончания выборки в формате ' + datetime.datetime.now().strftime('%Y-%m-%d'))
            print('Пустая дата = сегодня')
            start_end = input()
            if start_end == '':
                start_end = None
            result = rtp.read(start_time, start_end)
            if result == 'NULL':
                print("Нет значений за выбранный период")
            else:
                trans.transaction(result, config['RTP-2']['dst_table'], 'rtp2graph.csv', 'id')
    except Exception:
        print('Ошибка при выполнении')


def collect_rtp3():
    print('Печь на реконструкции')


def collect_rtp3_graph():
    try:
        # Для выгрузки за конкретную дату - смотрите описание модуля.
        rtp = rtp4graph.RTP4Graph(config)   # Используем идентичный модуль
        result = rtp.read(rtp=3)
        for each_folder in result:
            trans.transaction(each_folder, config['RTP-3_Graph']['dst_table'], 'rtp3graph.csv')
    except Exception:
        print('Ошибка при выполнении')


def collect_rtp4():
    try:
        # Для восстановления не снятых данных, внутри модуля есть секция.
        rtp = rtp4.RTP4(config)
        result = rtp.rtp4_log()
        trans.transaction(result, 'asutp."rtp4log"', 'rtp4log.csv')
        result = rtp.rtp4_mode()
        trans.transaction(result, 'asutp."rtp4ust_real"', 'rtp4ust_real.csv')
        result = rtp.rtp4_report()
        trans.transaction(result, 'asutp."rtp4widetable"', 'rtp4widetable.csv')
    except Exception:
        print('Ошибка при выполнении')


def collect_rtp4_graph():
    try:
        # Для выгрузки за конкретную дату - смотрите описание модуля
        rtp = rtp4graph.RTP4Graph(config)
        result = rtp.read()
        for each_folder in result:
            trans.transaction(each_folder, config['RTP-4_Graph']['dst_table'], 'rtp4graph.csv')
    except Exception:
        print('Ошибка при выполнении')


def collect_do(dmn=False):
    try:
        dz = doz.DO(config)
        if dmn:
            print(date_now.now().strftime('%Y-%m-%d %H:%M:%S'))
            result = dz.read()
        else:
            print('Введите дату в формате ' + datetime.datetime.now().strftime('%Y-%m-%d'))
            dt = input()
            if dt == '':
                dt = None
            result = dz.read(dt)
        trans.transaction(result, config['DO']['dst_table'], 'doza.csv')
    except Exception:
        print('Ошибка при выполнении')


def collect_do_graf(dmn=False):
    try:
        dzg = doz_graf.DOGraf(config)
        if dmn:
            # Внимание! Возвращяется трехмерный массив со всеми таблицами. Можно выгрузить только одну таблицу
            # result = dzg.read('2020-01-02', 'coN1_809_1')
            result = dzg.read()
        else:
            print('Введите дату в формате ' + datetime.datetime.now().strftime('%Y-%m-%d'))
            dt = input()
            if dt == '':
                dt = None
            result = dzg.read(dt)
        if len(result) > 0:
            for each_table in result:
                if len(each_table) > 0:
                    print(each_table[0]['fild_gp'])
                    trans.transaction(each_table, 'asutp.doz_' + each_table[0]['fild_gp'], 'dozir\\' + each_table[0]['fild_gp'] + '.csv')
    except Exception:
        print('Ошибка при выполнении')


def collect_gou_graf(dmn=False):
    try:
        ggrf = gou_graf.GOUGraf(config)
        if dmn:
            result = ggrf.read()
        else:
            print('Введите дату в формате ' + datetime.datetime.now().strftime('%Y-%m-%d'))
            dt = input()
            if dt == '':
                dt = None
            result = ggrf.read(dt)
        if len(result) > 0:
            for each_table in result:
                print(each_table)
                trans.transaction(result[each_table], 'asutp.' + each_table, 'gou\\' + each_table + '.csv')
    except Exception:
        print('Ошибка при выполнении')


def good_bye():
    print("Программа завершена")
    logging.info("Good bye. Stop the BZF Collector")
    logging.info('_' * 50)
    exit()


def hlp():
    with closing(open('readme.txt', 'r')) as file:
        for x in file:
            print(x)


def main():
    while True:
        cmd = show_menu()
        # cmd = '4'
        if cmd == '0' or cmd == 'daemon':
            while True:
                daemon()
        elif cmd == '1' or cmd == 'rtp1':
            collect_rtp1()
        elif cmd == '2' or cmd == 'rtp2':
            collect_rtp2()
        elif cmd == '3' or cmd == 'rtp3':
            collect_rtp3()
        elif cmd == '4' or cmd == 'rtp3_graph':
            collect_rtp3_graph()
        elif cmd == '5' or cmd == 'rtp4':
            collect_rtp4()
        elif cmd == '6' or cmd == 'rtp4_graph':
            collect_rtp4_graph()
        elif cmd == '7' or cmd == 'do':
            collect_do()
        elif cmd == '8' or cmd == 'do_graf':
            collect_do_graf()
        elif cmd == '9' or cmd == 'gou_graf':
            collect_gou_graf()
        elif cmd == '10' or cmd == 'help':
            hlp()
        elif cmd == '11' or cmd == 'exit':
            good_bye()
        else:
            print("Введите корреткную команду")


if __name__ == '__main__':
    print('\t\t\tООО "БЗФ" - DATA COLLECTOR\n')
    logging.info("Start BZF Collector")
    try:
        if sys.argv[1] == 'daemon':
            daemon()
    except Exception:
        print('Ручной режим.')
    main()
