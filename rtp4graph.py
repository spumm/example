#!/usr/bin/python3.6
"""
Модуль забирает кучу мелких csv файлов с сервера под WindowsXP, формирут широкую таблицу для последующей загрузки в
asutp.rtp4graph.
Мелкие csv файлы формируются программой histdata, входщей в пакет программ InTouch. Программа histdata управляется
посредством DDE запросов, что реализовано на VBA в Excel.
Скрипт перебирает все папки в csv_dir. Из каждой папки формирует список словарей для записи через pgwriter.
И возвращает список, в котором каждый элемент это сформированная таблица.
Для выгрузки за определенную дату, неоходимо на сервере WindowsXP запустить VBA скрипты,
и далее по расписанию всё добавится в БД.
Прочитанные папки скрипт перемещает в csv_del

UPD: Модуль полностью совместим с данными РТП-3, поэтому просто используем его.
Добавил один параметр, для смены директорий под РТП-3.
"""

import logging
import datetime, time
import os, shutil
import csv


class RTP4Graph:
    def __init__(self, config):
        self.config = config

    def read(self,rtp=4):
        if rtp == 3:
            csv_dir = self.config['RTP-3_Graph']['csv_dir']    # папка csv файлами
            csv_del = self.config['RTP-3_Graph']['csv_del']    # папка с отработанными файлами
        else:
            csv_dir = self.config['RTP-4_Graph']['csv_dir']    # папка csv файлами
            csv_del = self.config['RTP-4_Graph']['csv_del']    # папка с отработанными файлами

        csv_tab = []    # Итоговая таблица
        field_names = [ 'datetime_graph',
                        'f111_ps_psn1_state_currentstage',
                        'f112_ps_psn2_state_currentstage',
                        'f113_ps_psn3_state_currentstage',
                        'f121_pr_ph_a__rms__theta',
                        'f122_pr_ph_b__rms__theta',
                        'f123_pr_ph_c__rms__theta',
                        'f124_pr_ph_a__stn__theta_ust',
                        'f125_pr_ph_b__stn__theta_ust',
                        'f126_pr_ph_c__stn__theta_ust',
                        'f131_pr_ph_a__rms__theta',
                        'f132_pr_ph_b__rms__theta',
                        'f133_pr_ph_c__rms__theta',
                        'f134_pr_ph_a__stn__theta_ust',
                        'f135_pr_ph_b__stn__theta_ust',
                        'f136_pr_ph_c__stn__theta_ust',
                        'f141_pr_ph_a__rms__g',
                        'f142_pr_ph_b__rms__g',
                        'f143_pr_ph_c__rms__g',
                        'f144_pr_ph_a__stn__gust',
                        'f145_pr_ph_b__stn__gust',
                        'f146_pr_ph_c__stn__gust',
                        'f151_pr_ph_a__rms__theta',
                        'f152_pr_ph_b__rms__theta',
                        'f153_pr_ph_c__rms__theta',
                        'f154_pr_ph_a__var__ai_elposition',
                        'f155_pr_ph_b__var__ai_elposition',
                        'f156_pr_ph_c__var__ai_elposition',
                        'f161_mc_ao_carb_el1_fanctr__value',
                        'f162_mc_ai_carb_el1_fan__sts__value',
                        'f163_mc_ai_carb_el1_a_t__sts__value',
                        'f164_mc_ai_carb_el1_m_t__sts__value',
                        'f165_carb_el1_state_heater_numset',
                        'f166_carb_el1_settings_tempsetpoint',
                        'f171_mc_ao_carb_el2_fanctr__value',
                        'f172_mc_ai_carb_el2_fan__sts__value',
                        'f173_mc_ai_carb_el2_a_t__sts__value',
                        'f174_mc_ai_carb_el2_m_t__sts__value',
                        'f175_carb_el2_state_heater_numset',
                        'f176_carb_el2_settings_tempsetpoint',
                        'f181_mc_ao_carb_el3_fanctr__value',
                        'f182_mc_ai_carb_el3_fan__sts__value',
                        'f183_mc_ai_carb_el3_a_t__sts__value',
                        'f184_mc_ai_carb_el3_m_t__sts__value',
                        'f185_carb_el3_state_heater_numset',
                        'f186_carb_el3_settings_tempsetpoint',
                        'f191_byp_el2_state_l_shiftcount',
                        'f192_byp_el2_state_l',
                        'f193_byp_el2_state_last_e',
                        'f194_mc_ai_byp_el2_upr_p__sts__value',
                        'f195_mc_ai_byp_el2_dnr_p__sts__value',
                        'f201_byp_el3_state_l_shiftcount',
                        'f202_byp_el3_state_l',
                        'f203_byp_el3_state_last_e',
                        'f204_mc_ai_byp_el3_upr_p__sts__value',
                        'f205_mc_ai_byp_el3_dnr_p__sts__value',
                        'f211_byp_el1_state_l_shiftcount',
                        'f212_byp_el1_state_l',
                        'f213_byp_el1_state_last_e',
                        'f214_mc_ai_byp_el1_upr_p__sts__value',
                        'f215_mc_ai_byp_el1_dnr_p__sts__value',
                        'f221_mc_ao_carb_el1_fanctr__value',
                        'f222_mc_ai_carb_el1_fan__sts__value',
                        'f223_mc_ai_carb_el1_a_t__sts__value',
                        'f224_mc_ai_carb_el1_m_t__sts__value',
                        'f225_carb_el1_state_heater_numset',
                        'f226_carb_el1_settings_tempsetpoint',
                        'f231_mc_ai_hpress_main_p__sts__value',
                        'f241_hpress_p1visu',
                        'f242_mc_ai_hpress_el1_t__sts__value',
                        'f243_hpress_f1visu',
                        'f251_hpress_p2visu',
                        'f252_mc_ai_hpress_el2_t__sts__value',
                        'f253_hpress_f2visu',
                        'f261_hpress_p3visu',
                        'f262_mc_ai_hpress_el3_t__sts__value',
                        'f263_hpress_f3visu',
                        'f271_mc_ai_mns_pump1_p__sts__value',
                        'f272_mc_ai_mns_pump2_p__sts__value',
                        'f273_mc_ai_mns_pump3_p__sts__value',
                        'f274_a1_analog_ai_mns_p',
                        'f275_mc_ai_mns_oil_temp__sts__value',
                        'f281_mc_ao_elctrl_el1_ps__value',
                        'f282_mc_ai_elctrl_el1_ps__sts__value',
                        'f283_mc_ai_elctrl_el1_up__sts__value',
                        'f291_mc_ao_elctrl_el2_ps__value',
                        'f292_mc_ai_elctrl_el2_ps__sts__value',
                        'f293_mc_ai_elctrl_el2_up__sts__value',
                        'f301_mc_ao_elctrl_el3_ps__value',
                        'f302_mc_ai_elctrl_el3_ps__sts__value',
                        'f303_mc_ai_elctrl_el3_up__sts__value',
                        'f311_tc_main_f__value',
                        'f312_tc_res_f__value',
                        'f313_tc_hood_t1__value',
                        'f314_tc_hood_t2__value',
                        'f315_tc_hood_t3__value',
                        'f321_mc_ai_bcool_el1_t__sts__value',
                        'f322_mc_ai_bcool_el2_t__sts__value',
                        'f323_mc_ai_bcool_el3_t__sts__value',
                        'f331_tc_bottom1__value',
                        'f332_tc_bottom2__value',
                        'f333_tc_bottom3__value',
                        'f341_tc_c1_p__value',
                        'f342_tc_c2_p__value',
                        'f343_tc_c3_p__value',
                        'f344_tc_c4_p__value',
                        'f351_tc_c1_t__value',
                        'f352_tc_c2_t__value',
                        'f353_tc_c3_t__value',
                        'f354_tc_c4_t__value',
                        'f361_tc_c1_01_t__value',
                        'f362_tc_c1_02_t__value',
                        'f363_tc_c1_03_t__value',
                        'f364_tc_c1_04_t__value',
                        'f371_tc_c1_05_t__value',
                        'f372_tc_c1_06_t__value',
                        'f373_tc_c1_07_t__value',
                        'f374_tc_c1_08_t__value',
                        'f381_tc_c1_09_t__value',
                        'f382_tc_c1_10_t__value',
                        'f383_tc_c1_11_t__value',
                        'f384_tc_c1_12_t__value',
                        'f391_tc_c1_13_t__value',
                        'f392_tc_c1_14_t__value',
                        'f393_tc_c1_15_t__value',
                        'f394_tc_c1_16_t__value',
                        'f401_tc_c1_17_t__value',
                        'f402_tc_c1_18_t__value',
                        'f403_tc_c1_19_t__value',
                        'f404_tc_c1_20_t__value',
                        'f411_tc_c1_21_t__value',
                        'f412_tc_c1_22_t__value',
                        'f413_tc_c1_23_t__value',
                        'f414_tc_c1_24_t__value',
                        'f421_tc_c1_25_t__value',
                        'f422_tc_c1_26_t__value',
                        'f423_tc_c1_27_t__value',
                        'f424_tc_c1_28_t__value',
                        'f431_tc_c1_29_t__value',
                        'f432_tc_c1_30_t__value',
                        'f433_tc_c1_31_t__value',
                        'f434_tc_c1_32_t__value',
                        'f441_tc_c1_33_t__value',
                        'f442_tc_c1_34_t__value',
                        'f443_tc_c1_35_t__value',
                        'f444_tc_c1_36_t__value',
                        'f451_tc_c1_37_t__value',
                        'f452_tc_c1_38_t__value',
                        'f453_tc_c1_39_t__value',
                        'f454_tc_c1_40_t__value',
                        'f461_tc_c1_41_t__value',
                        'f462_tc_c1_42_t__value',
                        'f463_tc_c2_01_t__value',
                        'f464_tc_c2_02_t__value',
                        'f471_tc_c2_03_t__value',
                        'f472_tc_c2_04_t__value',
                        'f473_tc_c2_05_t__value',
                        'f474_tc_c2_06_t__value',
                        'f481_tc_c2_07_t__value',
                        'f482_tc_c2_08_t__value',
                        'f483_tc_c2_09_t__value',
                        'f484_tc_c2_10_t__value',
                        'f491_tc_c2_11_t__value',
                        'f492_tc_c2_12_t__value',
                        'f493_tc_c2_13_t__value',
                        'f494_tc_c2_14_t__value',
                        'f501_tc_c2_15_t__value',
                        'f502_tc_c2_16_t__value',
                        'f503_tc_c2_17_t__value',
                        'f504_tc_c2_18_t__value',
                        'f511_tc_c2_19_t__value',
                        'f512_tc_c2_20_t__value',
                        'f513_tc_c2_21_t__value',
                        'f514_tc_c2_22_t__value',
                        'f521_tc_c2_23_t__value',
                        'f522_tc_c2_24_t__value',
                        'f523_tc_c2_25_t__value',
                        'f524_tc_c2_26_t__value',
                        'f531_tc_c2_27_t__value',
                        'f532_tc_c2_28_t__value',
                        'f533_tc_c2_29_t__value',
                        'f534_tc_c2_30_t__value',
                        'f541_tc_c2_31_t__value',
                        'f542_tc_c2_32_t__value',
                        'f543_tc_c2_33_t__value',
                        'f544_tc_c2_34_t__value',
                        'f551_tc_c2_35_t__value',
                        'f552_tc_c2_36_t__value',
                        'f553_tc_c2_37_t__value',
                        'f554_tc_c2_38_t__value',
                        'f561_tc_c2_39_t__value',
                        'f562_tc_c2_40_t__value',
                        'f563_tc_c2_41_t__value',
                        'f564_tc_c2_42_t__value',
                        'f571_tc_c3_01_t__value',
                        'f572_tc_c3_02_t__value',
                        'f573_tc_c3_03_t__value',
                        'f574_tc_c3_04_t__value',
                        'f581_tc_c3_05_t__value',
                        'f582_tc_c3_06_t__value',
                        'f583_tc_c3_07_t__value',
                        'f584_tc_c3_08_t__value',
                        'f591_tc_c3_09_t__value',
                        'f592_tc_c3_10_t__value',
                        'f593_tc_c3_11_t__value',
                        'f594_tc_c3_12_t__value',
                        'f601_tc_c3_13_t__value',
                        'f602_tc_c3_14_t__value',
                        'f603_tc_c3_15_t__value',
                        'f604_tc_c3_16_t__value',
                        'f611_tc_c3_17_t__value',
                        'f612_tc_c3_18_t__value',
                        'f613_tc_c3_19_t__value',
                        'f614_tc_c3_20_t__value',
                        'f621_tc_c3_21_t__value',
                        'f622_tc_c3_22_t__value',
                        'f623_tc_c3_23_t__value',
                        'f624_tc_c3_24_t__value',
                        'f631_tc_c3_25_t__value',
                        'f632_tc_c3_26_t__value',
                        'f633_tc_c3_27_t__value',
                        'f634_tc_c3_28_t__value',
                        'f641_tc_c3_29_t__value',
                        'f642_tc_c3_30_t__value',
                        'f643_tc_c3_31_t__value',
                        'f644_tc_c3_32_t__value',
                        'f651_tc_c3_33_t__value',
                        'f652_tc_c3_34_t__value',
                        'f653_tc_c3_35_t__value',
                        'f654_tc_c3_36_t__value',
                        'f661_tc_c3_37_t__value',
                        'f662_tc_c3_38_t__value',
                        'f663_tc_c3_39_t__value',
                        'f664_tc_c3_40_t__value',
                        'f671_tc_c3_41_t__value',
                        'f672_tc_c3_42_t__value',
                        'f673_tc_c4_01_t__value',
                        'f674_tc_c4_02_t__value',
                        'f681_tc_c4_03_t__value',
                        'f682_tc_c4_04_t__value',
                        'f683_tc_c4_05_t__value',
                        'f684_tc_c4_06_t__value',
                        'f691_tc_c4_07_t__value',
                        'f692_tc_c4_08_t__value',
                        'f693_tc_c4_09_t__value',
                        'f694_tc_c4_10_t__value',
                        'f701_tc_c4_11_t__value',
                        'f702_tc_c4_12_t__value',
                        'f703_tc_c4_13_t__value',
                        'f704_tc_c4_14_t__value',
                        'f711_tc_c4_15_t__value',
                        'f712_tc_c4_16_t__value',
                        'f713_tc_c4_17_t__value',
                        'f714_tc_c4_18_t__value',
                        'f721_tc_c4_19_t__value',
                        'f722_tc_c4_20_t__value',
                        'f723_tc_c4_21_t__value',
                        'f724_tc_c4_22_t__value',
                        'f731_tc_c4_23_t__value',
                        'f732_tc_c4_24_t__value',
                        'f733_tc_c4_25_t__value',
                        'f734_tc_c4_26_t__value',
                        'f741_tc_c4_27_t__value',
                        'f742_tc_c4_28_t__value',
                        'f743_tc_c4_29_t__value',
                        'f744_tc_c4_30_t__value',
                        'f751_tc_c4_31_t__value',
                        'f752_tc_c4_32_t__value',
                        'f753_tc_c4_33_t__value',
                        'f754_tc_c4_34_t__value',
                        'f761_tc_c4_35_t__value',
                        'f762_tc_c4_36_t__value',
                        'f763_tc_c4_37_t__value',
                        'f764_tc_c4_38_t__value',
                        'f771_tc_c4_39_t__value',
                        'f772_tc_c4_40_t__value',
                        'f773_tc_c4_41_t__value',
                        'f774_tc_c4_42_t__value',
                        'f781_wd_fd01__val__weightheat',
                        'f782_wd_fd02__val__weightheat',
                        'f783_wd_fd03__val__weightheat',
                        'f784_wd_fd04__val__weightheat',
                        'f791_wd_fd05__val__weightheat',
                        'f792_wd_fd06__val__weightheat',
                        'f793_wd_fd07__val__weightheat',
                        'f794_wd_fd08__val__weightheat',
                        'f801_wd_fd09__val__weightheat',
                        'f802_wd_fd10__val__weightheat',
                        'f803_wd_fd11__val__weightheat',
                        'f804_wd_fd12__val__weightheat',
                        'f811_wd_fd13__val__weightheat',
                        'f812_wd_fd14__val__weightheat',
                        'f813_wd_fd15__val__weightheat',
                        'f814_wd_fd16__val__weightheat',
                        'f821_wd_stat__currt__weight01',
                        'f822_wd_stat__currt__weight02',
                        'f823_wd_stat__currt__weight03',
                        'f824_wd_stat__currt__weight04',
                        'f831_wd_stat__currt__weight05',
                        'f832_wd_bin01__sts__level',
                        'f833_wd_bin02__sts__level',
                        'f834_wd_bin03__sts__level',
                        'f841_wd_bin04__sts__level',
                        'f842_wd_bin05__sts__level',
                        'f843_wd_bin06__sts__level',
                        'f844_wd_bin07__sts__level',
                        'f851_wd_bin08__sts__level',
                        'f852_wd_bin09__sts__level',
                        'f853_wd_bin10__sts__level',
                        'f854_wd_bin11__sts__level',
                        'f861_wd_bin12__sts__level',
                        'f862_wd_bin13__sts__level',
                        'f863_wd_bin14__sts__level',
                        'f864_wd_bin15__sts__level',
                        'f871_wd_bin16__sts__level',
                        'f872_wd_fd01__val__weightheat',
                        'f873_wd_fd02__val__weightheat',
                        'f874_wd_fd03__val__weightheat',
                        'f881_wd_fd04__val__weightheat',
                        'f882_wd_fd05__val__weightheat',
                        'f883_wd_fd06__val__weightheat',
                        'f884_wd_fd07__val__weightheat',
                        'f891_wd_fd08__val__weightheat',
                        'f892_wd_fd09__val__weightheat',
                        'f893_wd_fd10__val__weightheat',
                        'f894_wd_fd11__val__weightheat',
                        'f901_wd_fd12__val__weightheat',
                        'f902_wd_fd13__val__weightheat',
                        'f903_wd_fd14__val__weightheat',
                        'f904_wd_fd15__val__weightheat',
                        'f911_wd_fd16__val__weightheat',
                        'f912_wd_stat__currt__weight01',
                        'f913_wd_stat__currt__weight02',
                        'f914_wd_stat__currt__weight03',
                        'f921_wd_stat__currt__weight04',
                        'f922_wd_stat__currt__weight05',
                        'f923_wd_bin01__sts__level',
                        'f924_wd_bin02__sts__level',
                        'f931_wd_bin03__sts__level',
                        'f932_wd_bin04__sts__level',
                        'f933_wd_bin05__sts__level',
                        'f934_wd_bin06__sts__level',
                        'f941_wd_bin07__sts__level',
                        'f942_wd_bin08__sts__level',
                        'f943_wd_bin09__sts__level',
                        'f944_wd_bin10__sts__level',
                        'f951_wd_bin11__sts__level',
                        'f952_wd_bin12__sts__level',
                        'f953_wd_bin13__sts__level',
                        'f954_wd_bin14__sts__level',
                        'f961_wd_bin15__sts__level',
                        'f962_wd_bin16__sts__level',
                        'f297_byp_el1_state_slip_shiftcount',
                        'f298_byp_el1_state_slip_lastcount',
                        'f299_byp_el2_state_slip_shiftcount',
                        'f300_byp_el2_state_slip_lastcount',
                        'f301_byp_el3_state_slip_shiftcount',
                        'f302_byp_el3_state_slip_lastcount',
                        'fe111_pr_ph_a__rms__i',
                        'fe112_pr_ph_b__rms__i',
                        'fe113_pr_ph_c__rms__i',
                        'fe114_pr_ph_a__stn__iust',
                        'fe115_pr_ph_b__stn__iust',
                        'fe116_pr_ph_c__stn__iust',
                        'fe121_pr_ph_a__rms__u',
                        'fe122_pr_ph_b__rms__u',
                        'fe123_pr_ph_c__rms__u',
                        'fe124_pr_ph_a__var__ai_elposition',
                        'fe125_pr_ph_b__var__ai_elposition',
                        'fe126_pr_ph_c__var__ai_elposition',
                        'fe131_pr_ph_a__rms__theta',
                        'fe132_pr_ph_b__rms__theta',
                        'fe133_pr_ph_c__rms__theta',
                        'fe134_pr_ph_a__stn__theta_ust',
                        'fe135_pr_ph_b__stn__theta_ust',
                        'fe136_pr_ph_c__stn__theta_ust',
                        'fe141_pr_ph_a__rms__g',
                        'fe142_pr_ph_b__rms__g',
                        'fe143_pr_ph_c__rms__g',
                        'fe151_pr_ph_a__rms__ud',
                        'fe152_pr_ph_b__rms__ud',
                        'fe153_pr_ph_c__rms__ud',
                        'fe154_pr_ph_a__stn__udust',
                        'fe155_pr_ph_b__stn__udust',
                        'fe156_pr_ph_c__stn__udust',
                        'fe161_pr_ph_a__rms__um',
                        'fe162_pr_ph_b__rms__um',
                        'fe163_pr_ph_c__rms__um',
                        'fe171_pr_ph_a__rms__i',
                        'fe172_pr_ph_b__rms__i',
                        'fe173_pr_ph_c__rms__i',
                        'fe174_pr_ph_a__var__i_filter',
                        'fe175_pr_ph_b__var__i_filter',
                        'fe176_pr_ph_c__var__i_filter',
                        'fe181_pr_ph_a__var__hpf_frequency',
                        'fe182_pr_ph_b__var__hpf_frequency',
                        'fe183_pr_ph_c__var__hpf_frequency',
                        'fe184_pr_ph_a__var__i_filter',
                        'fe185_pr_ph_b__var__i_filter',
                        'fe186_pr_ph_c__var__i_filter',
                        'fe191_pr_ph_a__stn__iust',
                        'fe192_pr_ph_b__stn__iust',
                        'fe193_pr_ph_c__stn__iust',
                        'fe194_pr_ph_a__rms__p',
                        'fe195_pr_ph_b__rms__p',
                        'fe196_pr_ph_c__rms__p',
                        'fe201_pr_ph_a__stn__pust',
                        'fe202_pr_ph_b__stn__pust',
                        'fe203_pr_ph_c__stn__pust',
                        'fe204_pr_ph_a__rms__z',
                        'fe205_pr_ph_b__rms__z',
                        'fe206_pr_ph_c__rms__z',
                        'fe211_pr_ph_a__stn__zust',
                        'fe212_pr_ph_b__stn__zust',
                        'fe213_pr_ph_c__stn__zust',
                        'fe214_pr_ph_a__rms__ul',
                        'fe215_pr_ph_b__rms__ul',
                        'fe216_pr_ph_c__rms__ul',
                        'fe221_pr_ph_abc__rms__iab',
                        'fe222_pr_ph_abc__rms__ibc',
                        'fe223_pr_ph_abc__rms__ica',
                        'fe224_pr_ph_abc__rms__uab',
                        'fe225_pr_ph_abc__rms__ubc',
                        'fe226_pr_ph_abc__rms__uca',
                        'fe231_pr_ph_abc__rms__utr_ab',
                        'fe232_pr_ph_abc__rms__utr_bc',
                        'fe233_pr_ph_abc__rms__utr_ca',
                        'fe234_pr_ph_abc__rms__usl_ab',
                        'fe235_pr_ph_abc__rms__usl_bc',
                        'fe236_pr_ph_abc__rms__usl_ca',
                        'fe241_pr_ph_a__rms__l',
                        'fe242_pr_ph_b__rms__l',
                        'fe243_pr_ph_c__rms__l',
                        'fe244_pr_ph_a__rms__cos',
                        'fe245_pr_ph_b__rms__cos',
                        'fe246_pr_ph_c__rms__cos',
                        'fe251_byp_el1_state_l_shiftcount',
                        'fe252_byp_el2_state_l_shiftcount',
                        'fe253_byp_el3_state_l_shiftcount',
                        'fe261_pr_hs__simeas1_i_l1',
                        'fe262_pr_hs__simeas1_i_l2',
                        'fe263_pr_hs__simeas1_i_l3',
                        'fe264_pr_hs__sentron1_i_l1',
                        'fe265_pr_hs__sentron1_i_l2',
                        'fe266_pr_hs__sentron1_i_l3',
                        'fe271_pr_hs__sentron1_v_l12',
                        'fe272_pr_hs__sentron1_v_l23',
                        'fe273_pr_hs__sentron1_v_l31',
                        'fe274_pr_hs__sentron1_s_sum',
                        'fe275_pr_hs__sentron1_p_sum',
                        'fe276_pr_hs__sentron1_q_sum',
                        'fe281_byp_el1_state_shift_waste',
                        'fe282_byp_el2_state_shift_waste',
                        'fe283_byp_el3_state_shift_waste',
                        'fe284_ps_psn1_state_currentstage',
                        'fe285_ps_psn2_state_currentstage',
                        'fe286_ps_psn3_state_currentstage',
                        'fe291_mc_ao_elctrl_el1_ps__value',
                        'fe292_mc_ao_elctrl_el2_ps__value',
                        'fe293_mc_ao_elctrl_el3_ps__value',
                        'fe294_mc_ai_elctrl_el1_ps__sts__value',
                        'fe295_mc_ai_elctrl_el2_ps__sts__value',
                        'fe296_mc_ai_elctrl_el3_ps__sts__value']    # Список полей
        print('_' * 30)
        print('Импортирование графиков РТП-' + str(rtp))
        try:
            list_dir = os.listdir(csv_dir)
            for each_dir in list_dir:   # Перебираем папки
                fl_ff = True  # flag first file
                csv_tab_temp = []  # Промежуточная таблица
                csv_tab_dict_temp = []  # Промежуточная таблица для преобразования в словарь
                list_file = os.listdir(csv_dir + '\\' + each_dir)
                for each_file in list_file:     # Перебираем файлы в папке
                    iter_csv_tab_temp = 0
                    csv_file = csv_dir + '\\' + each_dir + '\\' + each_file
                    with open(csv_file, 'r') as csv_file:
                        reader = csv.DictReader(csv_file, delimiter=';')
                        if fl_ff:
                            for each_row in reader:
                                csv_tab_temp.append([])
                                dt = each_row['$Date'].split('/')
                                date = '20' + dt[2] + '-' + dt[0] + '-' + dt[1] + ' ' + each_row['$Time']
                                date = datetime.datetime.strptime(date,'%Y-%m-%d %H:%M:%S')  # Дата для поля datetime_graph
                                for each in each_row:
                                    if each == '$Date':
                                        csv_tab_temp[iter_csv_tab_temp].append(date)
                                    elif each == '$Time':
                                        pass
                                    else:
                                        csv_tab_temp[iter_csv_tab_temp].append(each_row[each])
                                iter_csv_tab_temp += 1
                            fl_ff = False
                        else:
                            for each_row in reader:
                                for each in each_row:
                                    if each == '$Date' or each == '$Time':
                                        pass
                                    else:
                                        csv_tab_temp[iter_csv_tab_temp].append(each_row[each])
                                iter_csv_tab_temp += 1
                        csv_file.close()
                for each_row in csv_tab_temp:
                    csv_tab_dict_temp.append(dict(zip(field_names, each_row)))

                # Возможно понадобится корректировка. Есть баг с часовыми поясами при выгрузке.
                # for each_row in csv_tab_dict_temp:
                #     each_row['datetime_graph'] = each_row['datetime_graph'] - datetime.timedelta(hours=1)
                # Если была произведена выгрузка с запятыми взаместо точек в дробных числах, то добавляем:
                # for each_row in csv_tab_dict_temp:
                #     for each in each_row:
                #         each_row[each] = str(each_row[each]).replace(',', '.')

                csv_tab.append(csv_tab_dict_temp)
                try:
                    time.sleep(5)
                    shutil.move(csv_dir + '\\' + each_dir, csv_del + '\\' + each_dir)
                except OSError as msg:
                    print(msg)
            print('Импортированание завершено. Взято папок ' + str(len(list_dir)) + ' шт.')
            return csv_tab
        except Exception:
            print('Общая ошибка модуля. Смотрите журнал')
            logging.exception('GENERAL ERROR')
