# -*- coding: utf-8 -*-

import os
import re

from M02_DataManager import dbDataMgr
from M06_Utility import comEnum
from M01_Simulator import PE_Simulator


class Utility:

    project_dir: str = os.getcwd()
    _simul: PE_Simulator = None
    DayStartTime: str = "00:00:00"
    runtime: int = 0

    # Lot 정보
    MinLotSize: int = 50
    MaxLotSize: int = 400

    @staticmethod
    def setup_object(simul: PE_Simulator):
        Utility._simul = simul

    @staticmethod
    def set_day_start_time(value: str):
        day_start_time_regex: re.Pattern = re.compile(comEnum.RegexCollection.day_start_time.value)
        if type(value) is str and day_start_time_regex.match(value) is not None:
            Utility.DayStartTime = value

    @staticmethod
    def set_runtime(runtime: int):
        Utility.runtime = runtime

    @staticmethod
    def get_data_manager():
        data_manager: dbDataMgr.DataManager = Utility._simul.DataMgr
        return data_manager


def test():
    Utility.set_day_start_time("23:53:62")
    print(Utility.DayStartTime)

    Utility.set_day_start_time("04:54:22")
    print(Utility.DayStartTime)


if __name__ == '__main__':
    test()
