# -*- coding: utf-8 -*-

import os
import re
import datetime

from M02_DataManager import dbDataMgr
from M06_Utility import comEnum
from M01_Simulator import PE_Simulator


class Utility:

    project_dir: str = os.getcwd()
    _simul: PE_Simulator = None
    DayStartTime: str = "00:00:00"
    DayStartDate: datetime.datetime = None
    DueDateUom: str = 'nan'     # nan / mon / day
    runtime: int = 0

    # 날짜 문자열 형식 검사를 위한 정규식
    day_start_time_regex: re.Pattern = re.compile(comEnum.RegexCollection.day_start_time.value)

    # Lot 정보
    MinLotSize: int = 50
    MaxLotSize: int = 400

    ProdWheelCalStd: str = 'hour'

    @staticmethod
    def setup_object(simul: PE_Simulator):
        Utility._simul = simul

    @staticmethod
    def setDayStartDate(year: int, month: int, day: int, hour: int = None, min: int = None, second: int = None):

        if hour is None or min is None or second is None:
            if Utility.chk_day_start_time(Utility.DayStartTime):
                hour = int(Utility.DayStartTime.split(":")[0])
                min = int(Utility.DayStartTime.split(":")[1])
                second = int(Utility.DayStartTime.split(":")[2])
            else:
                hour = 0
                min = 0
                second = 0
        Utility.DayStartDate = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=min, second=second)

    @staticmethod
    def setDayStartTime(value: str):
        if Utility.chk_day_start_time(value=value):
            Utility.DayStartTime = value

    @staticmethod
    def set_runtime(runtime: int):
        Utility.runtime = runtime

    @staticmethod
    def get_data_manager():
        data_manager: dbDataMgr.DataManager = Utility._simul.DataMgr
        return data_manager

    @staticmethod
    def chk_day_start_time(value: str):
        is_matching: bool = type(value) is str and Utility.day_start_time_regex.match(value) is not None
        return is_matching




def test():
    Utility.setDayStartTime("23:53:62")
    print(Utility.DayStartTime)

    Utility.setDayStartTime("00:00:00")
    print(Utility.DayStartTime)

    Utility.setDayStartDate(year=2020, month=3, day=8)
    print(Utility.DayStartDate)


if __name__ == '__main__':
    test()
