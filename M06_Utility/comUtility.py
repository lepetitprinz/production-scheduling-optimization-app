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

    _simul: PE_Simulator = None
    project_dir: str = os.path.dirname(os.getcwd())

    # Simulation Version
    FsVerId: str = ''
    MPVerId: str = ''

    # Time 정보
    Runtime: datetime.datetime = None
    DayStartTime: str = "00:00:00"
    DayStartDate: datetime.datetime = None
    DayEndDate: datetime.datetime = None
    DueDateUom: str = 'nan'     # 고정생산주기(납기기준): nan / mon / day
    DayHorizon: datetime.timedelta = datetime.timedelta(days=92)
    MonthMaxDays: dict = {}

    # Silo -> Bagging 전송 유예 옵션
    SiloWait: datetime.timedelta = None

    # 날짜 문자열 형식 검사를 위한 정규식
    day_start_time_regex = re.compile(comEnum.RegexCollection.day_start_time.value)

    # Dictionary 형태 Data
    ProdMstDict: dict = {}
    DmdQtyDict: dict = {}
    EngConfDict: dict = {}
    GradeChangeOgDict: dict = {}

    # Production Wheel
    ProdWheelDf = None
    ProdWheelCalStd: str = 'hour'
    ProdWheelHour: dict = {}

    # =================================================== #
    # Cofiguration 정보
    # =================================================== #

    # Scheduling Period 장보
    ProdCycle: str = ""
    PlanStartTime: str = ""
    PlanStartDay: str = ""
    PlanEndTime: str = ""
    PlanEndDay: str = ""

    # Machine Lot Size 정보
    MinLotSize: int = 50    # Lot Minimum Capacity
    MaxLotSize: int = 400   # Lot Maximum Capacity

    # Silo Capa 정보
    SiloCapa: int = 4000
    SiloQty:int = 10

    # Lot Sequencing Optimization (SCOP algorithm)
    OptTimeLimit = 1
    # =================================================== #
    # Constraint 정보 #
    # =================================================== #
    GradeChangeFinishConst: bool = False
    GradeGroupChangeConst: bool = False
    BaggingOperTimeConst: bool = False

    BaggingLeadTimeConst: bool = False
    BaggingLeadTime: int = 0

    BaggingWorkCalendarUse: bool = False
    BaggingWorkStartHour: int = 0
    BaggingWorkEndHour: int = 24

    # Reactor Shutdown 일정 변수
    ReactorShutdownYn: str = ""
    AfterSdGrade: str = ""
    ReactorShutdownStartDate: datetime.datetime = None
    ReactorShutdownEndDate: datetime.datetime = None

    @staticmethod
    def SetupObject(simul: PE_Simulator, engConfig: pd.DataFrame):
        Utility._simul = simul

        engConfDict = {}
        for idx, row in engConfig.iterrows():
            engConfDict[row['paramName']] = row['paramVal']

        Utility.EngConfDict = engConfDict

        # Configuration 정보 등록
        Utility.FsVerId = engConfDict['FS_VRSN_ID']
        Utility.MPVerId = engConfDict['MP_VRSN_ID']

        # 계획 기간정보
        Utility.ProdCycle = engConfDict['PROD_PERIOD']
        Utility.PlanStartTime = engConfDict['PROD_START_DATE']
        Utility.PlanStartDay = Utility.PlanStartTime[:6]
        Utility.PlanEndTime = engConfDict['PROD_END_DATE']
        Utility.PlanEndDay = Utility.PlanEndTime[:6]

        # Machine Lot Size 정보
        Utility.MinLotSize = int(engConfDict['REACTOR_LOT_MIN'])
        Utility.MaxLotSize = int(engConfDict['REACTOR_LOT_MAX'])

        # Time Constraint
        baggingLeadTimeYn = engConfDict['BAGGING_LOT_CHANGE_TIME_LT_YN']
        if baggingLeadTimeYn == "Y":
            baggingLeadTimeConst = True
        else:
            baggingLeadTimeConst = False
        Utility.BaggingLeadTimeConst = baggingLeadTimeConst
        Utility.BaggingLeadTime = engConfDict['BAGGING_LOT_CHANGE_TIME_LT']

        # 일별 가동 시간 정보 등록
        Utility.BaggingWorkCalendarUse = engConfDict['BAGGING_LOT_CHANGE_TIME_YN'] == 'Y'
        Utility.BaggingWorkStartHour = int(engConfDict['BAGGING_LOT_CHANGE_TIME_START'])
        Utility.BaggingWorkEndHour = int(engConfDict['BAGGING_LOT_CHANGE_TIME_END'])

        # Reactor Shutdown Time 정보 세팅
        Utility.ReactorShutdownYn = engConfDict['SHUTDOWN_PERIOD_YN']
        if Utility.ReactorShutdownYn == 'Y' or Utility.ReactorShutdownYn == 'y':
            Utility.AfterSdGrade = engConfDict['SHUTDOWN_PROD_ITEM']
            Utility.ReactorShutdownStartDate = datetime.datetime.strptime(engConfDict['SHUTDOWN_START_DATE'], "%Y%m%d")
            Utility.ReactorShutdownStartDate = Utility.ReactorShutdownStartDate.replace(hour=0, minute=0, second=0, microsecond=0)
            Utility.ReactorShutdownEndDate = datetime.datetime.strptime(engConfDict['SHUTDOWN_END_DATE'], "%Y%m%d")
            Utility.ReactorShutdownEndDate = Utility.ReactorShutdownEndDate.replace(hour=23, minute=59, second=59, microsecond=0)

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
            if Utility.ChkDayStartTime(Utility.DayStartTime):
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
        Utility.Runtime = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=min, second=second)

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
    def CalcDayEndDate():
        Utility.DayEndDate = Utility.DayStartDate + Utility.DayHorizon

    @staticmethod
    def SetDayStartTime(value: str):
        if Utility.ChkDayStartTime(value=value):
            Utility.DayStartTime = value

    @staticmethod
    def SetDayHorizon(days: int):
        pass
        # Utility.DayHorizon = datetime.timedelta(days=days)

    @staticmethod
    def SetRuntime(runtime: datetime.datetime):
        Utility.Runtime = runtime

    @staticmethod
    def GetDataManager():
        data_manager: dbDataMgr.DataManager = Utility._simul.DataMgr
        return data_manager

    @staticmethod
    def ChkDayStartTime(value: str):
        is_matching: bool = type(value) is str and Utility.day_start_time_regex.match(value) is not None
        return is_matching