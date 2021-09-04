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
import traceback
import numpy as np
from os.path import join, dirname
from dotenv import load_dotenv
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from enum import Enum

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


class PeakType(Enum):
    AMOUNT = "AMOUNT"
    PERCENTAGE = "PERCENTAGE"


class CsvData:
    """
    CSV Data bundle
    """

    def __init__(self, area_id, url, five_min_start, hourly_start, include_wind=False):
        """
        Fetch CSV
        :param url: Area ID
        :param url: Target URL
        :param five_min_start: The row where the data per 5 minutes start
        :param hourly_start: The row where the data per 1 hour start
        :param include_wind: Whether the electric power company provides wind generation in 5 minutes data
        """
        s = requests.get(url).content
        self.area_id = area_id
        self.lines = s.decode('shift-jis').splitlines()
        self.five_min_start = five_min_start
        self.hourly_start = hourly_start
        self.today = datetime.date.today()
        self.include_wind = include_wind

    def dump(self):
        """
        Print the data in the CSV
        :return: void
        """
        i = 0
        for l in self.lines:
            print('{0} {1}'.format(i, l))
            i += 1

    def get_peak_supply(self, supply_line=2):
        """
        Returning the peak supply
        :return: Peak supply
        """
        peak_supply_list = self.lines[supply_line].split(',')
        peak_supply = peak_supply_list[0]
        return peak_supply

    def get_peak_demand_gql(self, peak_type=PeakType.AMOUNT):
        """
        Returning the peak demand
        :return: Peak demand
        """
        if peak_type == PeakType.AMOUNT:
            supply_line = 2
            demand_line = 5
        else:
            supply_line = 8
            demand_line = 11

        peak_supply_list = self.lines[supply_line].split(',')
        peak_supply = peak_supply_list[0]
        peak_demand_list = self.lines[demand_line].split(',')
        peak_demand = peak_demand_list[0]
        expected_hour = peak_demand_list[1]
        percentage = peak_supply_list[5]
        reserve_pct = peak_supply_list[4]
        return {
            "areaId": self.area_id,
            "date": self.today.isoformat(),
            "expectedHour": expected_hour,
            "type": peak_type.value,
            "percentage": int(percentage),
            "reservePct": int(reserve_pct),
            "isTomorrow": False,
            "amount": int(peak_demand),
            "supply": int(peak_supply),
        }

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
        if self.include_wind:
            asc_col = ['DATE', 'TIME', 'DEMAND', 'SOLAR', 'WIND']
        else:
            asc_col = ['DATE', 'TIME', 'DEMAND', 'SOLAR']
        five_min_csv.columns = asc_col
        return five_min_csv

    def get_last_five_min_demand(self):
        """
        Returning the demand of last 5 minutes as structure
        :return: Demand data
        """
        five_min_csv = self.get_five_min_list()
        five_min_reverse = five_min_csv.query('DEMAND > 0').iloc[::-1]
        five_min_latest = five_min_reverse.iloc[0]
        return DemandData(five_min_latest.DATE, five_min_latest.TIME, five_min_latest.DEMAND)

    def get_last_five_min_demand_gql(self):
        """
        Returning the demand of last 5 minutes as structure
        :return: Demand data
        """
        five_min_csv = self.get_five_min_list()
        five_min_reverse = five_min_csv.query('DEMAND > 0').iloc[::-1]
        five_min_latest = five_min_reverse.iloc[0].fillna(0)
        wind = 0
        if self.include_wind:
            wind = five_min_latest.WIND
        return {
            "areaId": self.area_id,
            "date": self.today.isoformat(),
            "time": five_min_latest.TIME,
            "amount": int(five_min_latest.DEMAND),
            "solar": int(five_min_latest.SOLAR),
            "wind": wind,
        }

    def get_hour_list(self):
        """
        Returning the list of demand per 1 hour
        :return: Demand list
        """
        hour_list = self.lines[self.hourly_start:self.hourly_start + 25]
        hour_csv = pd.read_csv(io.StringIO(back_to_lines_str(hour_list)))
        asc_col = ['DATE', 'TIME', 'DEMAND', 'EXPECTED', 'PERCENTAGE', 'SUPPLY']
        hour_csv.columns = asc_col
        return hour_csv

    def get_last_hour_demand_gql(self):
        """
        Returning the demand of last 1 hour as structure
        :return: Demand data
        """
        hour_csv = self.get_hour_list()
        hour_reverse = hour_csv.query('DEMAND > 0').iloc[::-1]
        hour_latest = hour_reverse.iloc[0].fillna(0)
        return {
            "areaId": self.area_id,
            "date": self.today.isoformat(),
            "hour": int(hour_latest.TIME.split(":")[0]),
            "amount": int(hour_latest.DEMAND),
            "supply": int(hour_latest.SUPPLY),
            "percentage": int(hour_latest.PERCENTAGE),
        }

    def get_last_hour_demand(self):
        """
        Returning the demand of last 1 hour as structure
        :return: Demand data
        """
        hour_csv = self.get_hour_list()
        hour_reverse = hour_csv.query('DEMAND > 0').iloc[::-1]
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


def process_csv_content(id, url, five_min_start=55, hourly_start=14, include_wind=False):
    """
    Parse CSV file and build data
    :param id: Area ID
    :param url: target CSV URL
    :param five_min_start: The row where the data per 5 minutes start
    :param hourly_start: The row where the data per 1 hour start
    :param include_wind: Whether the electric power company provides wind generation in 5 minutes data
    :return: string
    """
    try:
        data = CsvData(id, url, five_min_start, hourly_start, include_wind)
        mutation_argument = {
            "peak": data.get_peak_demand_gql(peak_type=PeakType.AMOUNT),
            "peakPct": data.get_peak_demand_gql(peak_type=PeakType.PERCENTAGE),
            "hourly": data.get_last_hour_demand_gql(),
            "five": data.get_last_five_min_demand_gql(),
        }
        return mutation_argument
    except:
        return traceback.format_exc()


def _main():
    now = datetime.datetime.now()
    if now.hour == 0:
        print("It's before 1 o'clock: no fetch performed.")
        return {}

    api_url = os.environ.get("GRAPHQL_SERVER")
    server_key = os.environ.get("SERVER_KEY")
    transport = AIOHTTPTransport(url=api_url, headers={'Authorization': server_key})
    client = Client(transport=transport, fetch_schema_from_transport=True)

    query = gql(
        """
query AreaQuery {
  authorized
  allArea {
    id
    csvFile
    csvHourlyPos
    csvFiveMinPos
    hasWindData
  }
}
        """
    )

    mutation_query = gql("""
mutation CrawlerMutation(
  $peak:PeakElectricityInput,
  $peakPct:PeakElectricityInput,
  $hourly: HourlyDemandInput,
  $five: FiveMinDemandInput
) {
  postPeak: postPeakElectricity(peakInput: $peak) {
    id
    createdAt
    updatedAt
  }
  postPeakPct: postPeakElectricity(peakInput: $peakPct) {
    id
    createdAt
    updatedAt
  }
  postHourlyDemand(hourlyInput: $hourly) {
    id
    createdAt
    updatedAt
  }
  postFiveMinDemand(fiveInput: $five) {
    id
    createdAt
    updatedAt
  }
}    
    """)

    fetch_result = client.execute(query)

    if fetch_result.get("authorized"):
        for area in fetch_result.get("allArea"):
            mutation_data = process_csv_content(
                area.get("id"),
                area.get("csvFile"),
                hourly_start=area.get("csvHourlyPos"),
                five_min_start=area.get("csvFiveMinPos"),
                include_wind=area.get("hasWindData"),
            )
            print(mutation_data)
            result = client.execute(mutation_query, variable_values=mutation_data)
            print(result)
    else:
        print("Not authorized! Did you set server key properly?")

    return fetch_result


if __name__ == '__main__':
    # this is for debug
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)
    print(json.dumps(_main(), ensure_ascii=False))
