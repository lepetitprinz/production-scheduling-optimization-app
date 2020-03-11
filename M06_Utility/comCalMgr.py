# -*- coding: utf-8 -*-

import datetime
import pandas as pd

from M03_Site import simFactoryMgr
from M04_PhyProductionMgr import objMachine
from M06_Utility import comUtility


class CalendarManager(object):

    # def __init__(self, siteID: str, defWorkFlag: bool, dayStartTime: str):
    #     self.SiteID : str = siteID                                      #
    #     # self.WorkCalendar:comWorkCal.WorkCalendar                      #
    #     self.FacCalTimeLine:list = []                                   # Factory Calendar Time Line
    #     self.MacCalTimeLineList:list = []                               # Calendar Time Info Obj 리스트.
    #     self._macCalIdList:list = []                                    # Machine Calendar Obj 조회용 Cal ID 리스트
    #     self._defWorkFlag:bool = defWorkFlag                            # Ture: / False
    #     self.DayStartTime:str = dayStartTime

    def __init__(self):

        self._factory: simFactoryMgr.Factory = None

        self._machine: objMachine.Machine = None

        self.macStopSeq: list = None

    def SetupObject(self, factory: simFactoryMgr, machine: objMachine,
                    start_date: datetime.datetime, end_date: datetime.datetime,
                    start_hour: int = None, end_hour: int = None):

        self._factory = factory
        self._machine = machine

        if sum([hour is not None for hour in [start_hour, end_hour]]) == 2:
            self._build_daily_break_sequence(start_hour=start_hour, end_hour=end_hour,
                                             start_date=start_date, end_date=end_date)

    def append_downtime(self, from_date: datetime.datetime, to_date: datetime.datetime):
        appending_tuple: tuple = (from_date, to_date)
        self.macStopSeq.append(appending_tuple)
        self.macStopSeq.sort()

    def rebuild_break_sequence(self):
        pass

    # 작성 중
    def _build_daily_break_sequence(self,
            start_hour: int, end_hour: int,
            start_date: datetime.datetime, end_date: datetime.datetime, return_flag: bool = False):

        breaktime_daily_hours: int = (24 - end_hour) + start_hour

        first_from_date: datetime.datetime = self._get_first_time(start_hour=start_hour, end_hour=end_hour, target_date=start_date)

        end_from_date: datetime.datetime = self._get_first_time(start_hour=start_hour, end_hour=end_hour, target_date=end_date)

        # first_seq: tuple = tuple([first_from_date, first_to_date])
        # end_seq: tuple = tuple([end_from_date, end_to_date])

        breakdown_seq: list = []
        tmp_from_date: datetime.datetime = first_from_date
        while tmp_from_date <= end_date:
            tmp_to_date: datetime.datetime = tmp_from_date + datetime.timedelta(hours=breaktime_daily_hours)
            breaktime: tuple = (tmp_from_date, tmp_to_date)
            breakdown_seq.append(breaktime)
            tmp_from_date = tmp_from_date + datetime.timedelta(days=1)

        self.macStopSeq = breakdown_seq

        if return_flag:
            return breakdown_seq

    def _get_first_time(self, start_hour: int, end_hour: int, target_date: datetime.datetime):
        from_date: datetime.datetime = None
        to_date: datetime.datetime = None

        from_date = target_date - datetime.timedelta(days=1)
        from_date = from_date.replace(hour=end_hour)

        # start_date = min(from_date, start_date)
        return from_date


def test():

    from PE_Simulator import Simulator
    from simOperMgr import Operation

    simulator: Simulator = Simulator()
    simulator.SetupDbObject(
        source="db", year=2020, month=3, day=1, day_start_time="08:00:00", horizon_days=92,
        silo_qty=4000, nof_silo=10, silo_wait_hours=0
    )

    factory: simFactoryMgr.Factory = simFactoryMgr.Factory(simul=simulator, facID="IM_FACTORY")

    operation: Operation = Operation(factory=factory, oper_id="BAG", kind="BAGGING")

    machine: objMachine.Machine = objMachine.Machine(
        factory=factory, operation=operation, mac_id="BEGGAR"
    )

    comUtility.Utility.setDayStartDate(year=2020, month=3, day=9, hour=9)
    comUtility.Utility.setDayHorizon(days=10)
    comUtility.Utility.calcDayEndDate()
    print(f"DayStartDate = {comUtility.Utility.DayStartDate}")
    print(f"DayEndDate = {comUtility.Utility.DayEndDate}")

    calendar: CalendarManager = CalendarManager()
    calendar.SetupObject(
        factory=factory,
        machine=machine,
        start_date=datetime.datetime(2020, 3, 1, 8, 0, 0),
        end_date=datetime.datetime(2020, 6, 1, 20, 0, 0),
        start_hour=8,
        end_hour=20
    )
    from_date: datetime.datetime = calendar._get_first_time(start_hour=8, end_hour=20, target_date=comUtility.Utility.DayStartDate)
    print(f"from_date = {from_date}")

    breaks_seq: list = calendar._build_daily_break_sequence(start_hour=8, end_hour=20, start_date=comUtility.Utility.DayStartDate, end_date=comUtility.Utility.DayEndDate)

    calendar.append_downtime(from_date=datetime.datetime(2020, 5, 11, 0, 0, 0),
                             to_date=datetime.datetime(2020, 5, 12, 0, 0, 0))
    breaks_seq = calendar.macStopSeq

    print("\nBEFORE")
    for breaktime in breaks_seq:
        print(breaktime)

    calendar.rebuild_break_sequence()

    print("\nAFTER")
    for breaktime in breaks_seq:
        print(breaktime)

if __name__ == '__main__':
    test()
