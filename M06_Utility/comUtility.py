# -*- coding: utf-8 -*-

import os
import re
import datetime
import calendar
import pandas as pd

from M02_DataManager import dbDataMgr
from M06_Utility import comEnum
from M01_Simulator import PE_Simulator

class Utility:

    project_dir: str = os.getcwd()
    _simul: PE_Simulator = None
    DayStartTime: str = "00:00:00"
    DayStartDate: datetime.datetime = None
    DayEndDate: datetime.datetime = None
    DayHorizon: datetime.timedelta = datetime.timedelta(days=60)
    MonthMaxDays: dict = {}
    runtime: datetime.datetime = None
    DueDateUom: str = 'nan'     # 고정생산주기(납기기준): nan / mon / day

    # Silo -> Bagging 전송 유예 옵션
    SiloWait: datetime.timedelta = None

    # 날짜 문자열 형식 검사를 위한 정규식
    day_start_time_regex = re.compile(comEnum.RegexCollection.day_start_time.value)

    # =========================================== #
    # Cofiguration 정보
    # =========================================== #
    EngConfDict: dict = {}
    # Simulation Version
    FsVerId: str = ''
    DsVerId: str = ''

    # Scheduling Period 장보
    ProdCycle: str = ""
    PlanStartTime: str = ""
    PlanEndTime: str = ""

    # Machine Lot Size 정보
    MinLotSize: int = 50    # Lot Minimum Capacity
    MaxLotSize: int = 400   # Lot Maximum Capacity

    # Constraint 정보
    AfterSdGrade: str = ""

    # Production Wheel 관련
    ProdWheelDf = None
    ProdWheelCalStd: str = 'hour'
    ProdWheelHour: dict = {}

    # Lot Sequencing Optimization (SCOP algorithm)
    OptTimeLimit = 1

    # Time Constraint Cofiguration
    GradeChangeFinishConst: bool = False    # Configuration DB화 필요
    GradeGroupChangeConst: bool = False     # Configuration DB화 필요
    BaggingOperTimeConst: bool = False      # Configuration DB화 필요

    @staticmethod
    def setupObject(simul: PE_Simulator):
    #def setupObject(simul: PE_Simulator, engConfig: pd.DataFrame):
        Utility._simul = simul

        # engConfDict = {}
        # for idx, row in engConfig.iterrows():
        #     engConfig[row['paramCode']] = row['paramVal']
        # Utility.EngConfDict = engConfDict
        #
        # # Configuration 정보 등록
        # Utility.FsVerId = engConfDict['FS_VRSN_ID']
        # Utility.DsVerId = engConfDict['DS_VRSN_ID']
        #
        # # 계획 기간정보
        # Utility.ProdCycle = engConfDict['PROD_PERIOD']
        # Utility.PlanStartTime = engConfDict['PROD_START_DATE']
        # Utility.PlanEndTime = engConfDict['PROD_END_DATE']
        #
        # # Machine Lot Size 정보
        # Utility.MinLotSize = engConfDict['REACTOR_LOT_MIN']
        # Utility.MaxLotSize = engConfDict['REACTOR_LOT_MAX']
        #
        # # Shutdown 처리
        # Utility.AfterSdGrade = engConfDict['PROD_ITEM_AFTER_SHUTDOWN']
        #
        # # Time Constraint

    @staticmethod
    def setSiloWaitTime(hours: float):
        Utility.SiloWait = datetime.timedelta(hours=hours)

    @staticmethod
    def chop_microsecond(date_value: datetime.datetime):
        date_value = date_value - datetime.timedelta(microseconds=date_value.microseconds)
        date_value = date_value + datetime.timedelta(seconds=1)
        return date_value

    @staticmethod
    def setDayStartDate(year: int, month: int, day: int, hour: int = None, min: int = None, second: int = None):

        if hour is None or min is None or second is None:
            if Utility.chk_day_start_time(Utility.DayStartTime):
                hour = int(Utility.DayStartTime.split(":")[0]) if hour is None else hour
                min = int(Utility.DayStartTime.split(":")[1]) if min is None else min
                second = int(Utility.DayStartTime.split(":")[2]) if second is None else second
            else:
                hour = 0
                min = 0
                second = 0
            Utility.DayStartDate = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=min, second=second)
        else:
            Utility.DayStartDate = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=min, second=second)
        Utility.runtime = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=min, second=second)

    @staticmethod
    def GetMonthMaxDayDict(year_month_pairs: list):
        monthMaxDayDict: dict = {}
        for year_month_pair in year_month_pairs:
            monthMaxDayDict[year_month_pair] = Utility.GetMonthMaxDay(year=year_month_pair[0],
                                                                      month=year_month_pair[1])[-1]
        return monthMaxDayDict

    @staticmethod
    def GetMonthMaxDay(year: int, month: int):
        return calendar.monthrange(year=year, month=month)

    @staticmethod
    def calcDayEndDate():
        Utility.DayEndDate = Utility.DayStartDate + Utility.DayHorizon

    @staticmethod
    def setDayStartTime(value: str):
        if Utility.chk_day_start_time(value=value):
            Utility.DayStartTime = value

    @staticmethod
    def setDayHorizon(days: int):
        Utility.DayHorizon = datetime.timedelta(days=days)

    @staticmethod
    def set_runtime(runtime: datetime.datetime):
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

    Utility.setDayStartTime("00:12:12")
    print(Utility.DayStartTime)

    Utility.setDayStartDate(year=2020, month=3, day=8)
    print(Utility.DayStartDate)

    print(Utility.GetMonthMaxDayDict([(2020, 3), (2020, 4), (2020, 5)]))


if __name__ == '__main__':
    test()
