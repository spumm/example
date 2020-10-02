#!/usr/bin/python3.6
"""
Класс предоставляет доступ к базе данных MS SQL 2005 расположенной на сервере РТП-4
Использутся системный ODBC драйвер  {ODBC Driver 17 for SQL Server}
Модуль выгружает таблицу из представления [WWALMDB].[dbo].[v_AlarmEventHistory]
[EventStamp],[AlarmState],[TagName],[Description],[Area],[Type],[Value],[CheckValue],[Priority],[Category] FROM [WWALMDB].[dbo].[v_AlarmEventHistory]
Данные уставок из таблицы [RKO_Bratsk].[dbo].[REG_UST_1M]
Данные технологческих параметров из таблица [RKO_BRATSK].[dbo].[WideTable_1H]
"""
import pyodbc
import datetime
import logging
from contextlib import closing


class RTP4:
    def __init__(self,config):
        self.config = config

    def rtp4_log(self):
        try:
            with closing(pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                        'Server=' + self.config['RTP-4']['host'] + ';'
                                        'Database=' + self.config['RTP-4']['db_alarm'] + ';'
                                        'UID=' + self.config['RTP-4']['user'] + ';'
                                        'PWD=' + self.config['RTP-4']['password'])) as connection:
                with connection.cursor() as cursor:
                    time_start = datetime.datetime.now() - datetime.timedelta(hours=1)
                    time_end = datetime.datetime.now()
                    # Для восстановления не снятых данных. Задаем год, месяц, день, час
                    # time_start = datetime.datetime(2020, 3, 2, 15)
                    # time_end = datetime.datetime(2020, 3, 3, 6)
                    print('_' * 30)
                    print('РТП-4 Журнал',  end=' ')
                    cursor.execute("SELECT [EventStamp] AS eventstamp, [AlarmState] AS alarmstate, [TagName] AS tagname,\
                                    [Description] AS description,[Area] AS area,[Type] AS settype, [Value] AS setvalue,\
                                    [CheckValue] AS checkvalue, [Priority] AS priority, [Category] AS category \
                                    FROM [WWALMDB].[dbo].[v_AlarmEventHistory] \
                                    WHERE [EventStamp]>='" + time_start.strftime('%Y-%d-%m %H:00:00') + "' AND  \
                                    [EventStamp]<'" + time_end.strftime('%Y-%d-%m %H:00:00') + "' ORDER BY [EventStamp]")

                    field_name = [column[0] for column in cursor.description]
                    csv_tab = []
                    for row in cursor.fetchall():
                        csv_tab.append(dict(zip(field_name, row)))
                    return csv_tab
        except pyodbc.DatabaseError as msg:
            print(msg)
            logging.exception("")
        except Exception:
            print("Общая ошибка. Читайте журнал")
            logging.exception('General error')

    def rtp4_mode(self):
        try:
            with closing(pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                        'Server=' + self.config['RTP-4']['host'] + ';'
                                        'Database=' + self.config['RTP-4']['db_data'] + ';'
                                        'UID=' + self.config['RTP-4']['user'] + ';'
                                        'PWD=' + self.config['RTP-4']['password'])) as connection:
                with connection.cursor() as cursor:
                    time_start = datetime.datetime.now() - datetime.timedelta(hours=1)
                    time_end = datetime.datetime.now()
                    # Для восстановления не снятых данных. Задаем год, месяц, день, час
                    # time_start = datetime.datetime(2020, 3, 2, 15)
                    # time_end = datetime.datetime(2020, 3, 3, 6)
                    print('РТП-4 Уставки',  end=' ')
                    cursor.execute("SELECT TimePLC as datetime_graph, C1000 as El1_I, C1001 as El1_I_Min, \
                    C1002 as El1_I_Max, C1003 as El1_Par, C1013 as El1_ro, C1014 as El1_ro_Min, C1015 as El1_ro_Max, \
                    C1016 as El1_P, C1017 as El1_P_Min, C1018 as El1_P_Max, C1019 as El1_Q, C1020 as El1_Q_Min, \
                    C1021 as El1_Q_Max, C1022 as El1_Z, C1023 as El1_Z_Min, C1024 as El1_Z_Max, C1025 as El1_Ud, \
                    C1026 as El1_Ud_Min, C1027 as El1_Ud_Max, C1040 as El1_Mode, C1045 as El1_LoLim, C1046 as El1_HiLim,\
                    C1050 as El2_I, C1051 as El2_I_Min, C1052 as El2_I_Max, C1053 as El2_Par, C1063 as El2_ro,\
                    C1064 as El2_ro_Min, C1065 as El2_ro_Max, C1066 as El2_P, C1067 as El2_P_Min, C1068 as El2_P_Max, \
                    C1069 as El2_Q, C1070 as El2_Q_Min, C1071 as El2_Q_Max, C1072 as El2_Z, C1073 as El2_Z_Min, \
                    C1074 as El2_Z_Max, C1075 as El2_Ud, C1076 as El2_Ud_Min, C1077 as El2_Ud_Max, C1090 as El2_Mode, \
                    C1095 as El2_LoLim, C1096 as El2_HiLim, C1100 as El3_I, C1101 as El3_I_Min, C1102 as El3_I_Max, \
                    C1103 as El3_Par, C1113 as El3_ro, C1114 as El3_ro_Min, C1115 as El3_ro_Max, C1116 as El3_P, \
                    C1117 as El3_P_Min, C1118 as El3_P_Max, C1119 as El3_Q, C1120 as El3_Q_Min, C1121 as El3_Q_Max, \
                    C1122 as El3_Z, C1123 as El3_Z_Min, C1124 as El3_Z_Max, C1125 as El3_Ud, C1126 as El3_Ud_Min, \
                    C1127 as El3_Ud_Max, C1140 as El3_Mode, C1145 as El3_LoLim, [C1146] as El3_HiLim \
                    FROM [RKO_Bratsk].[dbo].[REG_UST_1M] \
                    WHERE [TimePLC]>='" + time_start.strftime('%Y-%d-%m %H:00:00') + "' AND  \
                    [TimePLC]<'" + time_end.strftime('%Y-%d-%m %H:00:00') + "'")

                    field_name = [column[0] for column in cursor.description]
                    for each in field_name:
                        field_name[field_name.index(each)] = each.lower()
                    csv_tab = []
                    for row in cursor.fetchall():
                        csv_tab.append(dict(zip(field_name, row)))
                    return csv_tab
        except pyodbc.DatabaseError as msg:
            print(msg)
            logging.exception("")
        except Exception:
            print("Общая ошибка. Читайте журнал")
            logging.exception('General error')

    def rtp4_report(self):
        try:
            with closing(pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                        'Server=' + self.config['RTP-4']['host'] + ';'
                                        'Database=' + self.config['RTP-4']['db_data'] + ';'
                                        'UID=' + self.config['RTP-4']['user'] + ';'
                                        'PWD=' + self.config['RTP-4']['password'])) as connection:
                with connection.cursor() as cursor:
                    time_start = datetime.datetime.now() - datetime.timedelta(hours=1)
                    time_end = datetime.datetime.now()
                    # Для восстановления не снятых данных. Задаем год, месяц, день, час
                    # time_start = datetime.datetime(2020, 3, 2, 15)
                    # time_end = datetime.datetime(2020, 3, 3, 6)
                    print('РТП-4 Технологические параметры.',end=' ')
                    cursor.execute("SELECT [TimePLC] as datetime_graph,[C200],[C201],[C202],[C203],[C204],[C205],[C206],[C207],[C208],\
                    [C209],[C210],[C211],[C212],[C213],[C214],[C215],[C216],[C217],[C218],[C219],[C220],[C221],[C222],\
                    [C223],[C224],[C225],[C226],[C227],[C228],[C229],[C230],[C231],[C232],[C233],[C234],[C235],[C236],\
                    [C237],[C238],[C239],[C240],[C241],[C301],[C302],[C303],[C304],[C305],[C306],[C307],[C308],[C309]\
                     FROM [RKO_BRATSK].[dbo].[WideTable_1H] \
                     WHERE  [TimePLC]>='" + time_start.strftime('%Y-%d-%m %H:00:00') + "' \
                     AND  [TimePLC]<'" + time_end.strftime('%Y-%d-%m %H:00:00') + "' ORDER BY [TimePLC]")

                    field_name = [column[0] for column in cursor.description]
                    for each in field_name:
                        field_name[field_name.index(each)] = each.lower()
                    csv_tab = []
                    for row in cursor.fetchall():
                        csv_tab.append(dict(zip(field_name, row)))
                    return csv_tab
        except pyodbc.DatabaseError as msg:
            print(msg)
            logging.exception("")
        except Exception:
            print("Общая ошибка. Читайте журнал")
            logging.exception('General error')