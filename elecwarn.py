#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Electricity Warning Crawler, Mastodon / Python Edition.
# Copyright (C) 2018 Hirochika Yuda, a.k.a. Kuropen.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import requests
import pandas as pd
import io
import datetime
import os
import json
from os.path import join, dirname
from dotenv import load_dotenv


class DemandData:
    """
    The structure of electricity demand data
    """

    def __init__(self, date, time, demand):
        """
        Constructor
        :param date: Date string
        :param time: Time string
        :param demand: Demand amount string
        """
        self.demand = demand
        self.date = date
        self.time = time

    def get_date(self):
        """
        Returning the date string contained
        :return: Date string
        """
        return self.date

    def get_time(self):
        """
        Returning the time string contained
        :return: Time string
        """
        return self.time

    def get_demand(self):
        """
        Returning the demand data contained
        :return: Demand data
        """
        return self.demand

    def get_demand_as_float(self):
        """
        Returning the demand data, as float
        :return: Demand data
        """
        return float(self.demand)


class CsvData:
    """
    CSV Data bundle
    """

    def __init__(self, url, five_min_start=42, hourly_start=7):
        """
        Fetch CSV
        :param url: Target URL
        :param five_min_start: The row where the data per 5 minutes start
        :param hourly_start: The row where the data per 1 hour start
        """
        s = requests.get(url).content
        self.lines = s.decode('shift-jis').splitlines()
        self.five_min_start = five_min_start
        self.hourly_start = hourly_start

    def dump(self):
        """
        Print the data in the CSV
        :return: void
        """
        i = 0
        for l in self.lines:
            print('{0} {1}'.format(i, l))
            i += 1

    def get_peak_supply(self):
        """
        Returning the peak supply
        :return: Peak supply
        """
        peak_supply_list = self.lines[2].split(',')
        peak_supply = peak_supply_list[0]
        return peak_supply

    def get_peak_supply_as_float(self):
        """
        Returning the peak supply, as float
        :return: Peak supply
        """
        return float(self.get_peak_supply())

    def get_five_min_list(self):
        """
        Returning the list of demand per 5 minutes
        :return: Demand list
        """
        five_min_list = self.lines[self.five_min_start:self.five_min_start + 289]
        five_min_csv = pd.read_csv(io.StringIO(back_to_lines_str(five_min_list)))
        asc_col = ['DATE', 'TIME', 'DEMAND']
        five_min_csv.columns = asc_col
        return five_min_csv

    def get_last_five_min_demand(self):
        """
        Returning the demand of last 5 minutes as structure
        :return: Demand data
        """
        five_min_csv = self.get_five_min_list()
        five_min_reverse = five_min_csv.query('DEMAND != "NaN"').iloc[::-1]
        five_min_latest = five_min_reverse.iloc[0]
        return DemandData(five_min_latest.DATE, five_min_latest.TIME, five_min_latest.DEMAND)

    def get_hour_list(self):
        """
        Returning the list of demand per 1 hour
        :return: Demand list
        """
        hour_list = self.lines[self.hourly_start:self.hourly_start + 25]
        hour_csv = pd.read_csv(io.StringIO(back_to_lines_str(hour_list)))
        asc_col = ['DATE', 'TIME', 'DEMAND', 'EXPECTED', 'PERCENTAGE']
        hour_csv.columns = asc_col
        return hour_csv

    def get_last_hour_demand(self):
        """
        Returning the demand of last 1 hour as structure
        :return: Demand data
        """
        hour_csv = self.get_hour_list()
        hour_reverse = hour_csv.query('DEMAND != "NaN"').iloc[::-1]
        hour_latest = hour_reverse.iloc[0]
        return DemandData(hour_latest.DATE, hour_latest.TIME, hour_latest.DEMAND)

    def percentage_as_float(self, demand):
        """
        Calculate usage percentage and return it as float
        :param demand: Demand, both float and structure are accepted
        :return: percentage
        """
        supply = self.get_peak_supply_as_float()
        if demand.__class__.__name__ == 'DemandData':
            dm = demand.get_demand_as_float()
        else:
            dm = demand
        return dm / supply * 100

    def percentage(self, demand):
        """
        Calculate usage percentage and return it as string
        :param demand: Demand, both float and structure are accepted
        :return: percentage
        """
        return "{0:.2f}".format(self.percentage_as_float(demand))


def back_to_lines_str(split_list):
    """
    Writes back a list as multi line string
    :param split_list: list
    :return: string
    """
    return '\n'.join(split_list)


def process_csv_content(company, company_tag, url, five_min_start=42, hourly_start=7):
    """
    Parse CSV file and build data
    :param company: Electric Power Company Name
    :param company_tag: Electric Power Company Name For Tag
    :param url: target CSV URL
    :param five_min_start: The row where the data per 5 minutes start
    :param hourly_start: The row where the data per 1 hour start
    :return: string
    """
    try:
        data = CsvData(url, five_min_start, hourly_start)
        # data.dump()

        # Five minutes list
        latest_demand = data.get_last_five_min_demand()
        percentage = data.percentage_as_float(latest_demand)
        warning_level = ''
        toot_visibility = 'public'
        if percentage > 97:
            warning_level = ' 緊急警報'  # give a space before warning level
        elif percentage > 95:
            warning_level = ' 警報'
        elif percentage > 92:
            warning_level = ' 注意報'
        else:
            toot_visibility = 'unlisted'
        msg = ('【{0}管内 電力使用状況{1}】' +
               '{2} {3}の電力使用量は{4}万kWでした。ピーク時供給力 {5}万kW に対する使用率は {6:.2f}%です。') \
            .format(company,
                    warning_level,
                    latest_demand.get_date(),
                    latest_demand.get_time(),
                    latest_demand.get_demand(),
                    data.get_peak_supply(),
                    percentage)
        token = os.environ.get("TOKEN_{0}".format(company_tag.upper()))

        if not token:
            return msg

        request_payload = {
            'access_token': token,
            'status': msg,
            'visibility': toot_visibility
        }
        status_api_url = os.environ.get("STATUS_API")
        requests.post(status_api_url, data=request_payload)
        return msg
    except Exception as e:
        return str(e)


def _main():
    now = datetime.datetime.now()
    result = {}

    # TEPCO
    result['tokyo'] = \
        process_csv_content('東京電力パワーグリッド',
                            'elecwarn_tokyo',
                            'http://www.tepco.co.jp/forecast/html/images/juyo-j.csv')

    # Tohoku
    result['tohoku'] = \
        process_csv_content('東北電力',
                            'elecwarn_tohoku',
                            'http://setsuden.tohoku-epco.co.jp/common/demand/juyo_02_{0:04d}{1:02d}{2:02d}.csv'
                            .format(
                                now.year,
                                now.month,
                                now.day
                            ))

    # Hokkaido
    result['hokkaido'] = \
        process_csv_content('北海道電力',
                            'elecwarn_hokkaido',
                            'http://denkiyoho.hepco.co.jp/data/juyo_juyo_01_{0:04d}{1:02d}{2:02d}.csv'
                            .format(
                                now.year,
                                now.month,
                                now.day
                            ))

    # Chubu
    result['chubu'] = \
        process_csv_content('中部電力',
                            'elecwarn_chubu',
                            'http://denki-yoho.chuden.jp/denki_yoho_content_data/juyo_cepco003.csv')

    # Hokuriku
    result['hokuriku'] = \
        process_csv_content('北陸電力',
                            'elecwarn_hokuriku',
                            'http://www.rikuden.co.jp/denki-yoho/csv/juyo_05_{0:04d}{1:02d}{2:02d}.csv'
                            .format(
                                now.year,
                                now.month,
                                now.day
                            ))

    # Kansai
    result['kansai'] = \
        process_csv_content('関西電力',
                            'elecwarn_kansai',
                            'http://www.kepco.co.jp/yamasou/juyo1_kansai.csv',
                            46,
                            11)

    # Chugoku
    result['chugoku'] = \
        process_csv_content('中国電力',
                            'elecwarn_chugoku',
                            'http://www.energia.co.jp/jukyuu/sys/juyo_07_{0:04d}{1:02d}{2:02d}.csv'
                            .format(
                                now.year,
                                now.month,
                                now.day
                            ))

    # Shikoku
    result['shikoku'] = \
        process_csv_content('四国電力',
                            'elecwarn_shikoku',
                            'http://www.yonden.co.jp/denkiyoho/juyo_shikoku.csv')

    # Hokuriku
    result['kyushu'] = \
        process_csv_content('九州電力',
                            'elecwarn_kyushu',
                            'http://www.kyuden.co.jp/power_usages/csv/juyo-hourly-{0:04d}{1:02d}{2:02d}.csv'
                            .format(
                                now.year,
                                now.month,
                                now.day
                            ))

    return result


if __name__ == '__main__':
    # this is for debug
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)
    print(json.dumps(_main(), ensure_ascii=False))
