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

        self.seq_daily: list = []
        self.seq_shutdown: list = []
        self.seq_breakdown: list = []

        self.seq_full: list = []

    def SetupObject(self, factory: simFactoryMgr, machine: objMachine,
                    start_date: datetime.datetime, end_date: datetime.datetime,
                    start_hour: int = None, end_hour: int = None):

        self._factory = factory
        self._machine = machine

        if sum([hour is not None for hour in [start_hour, end_hour]]) == 2:
            self._build_daily_break_sequence(start_hour=start_hour, end_hour=end_hour,
                                             start_date=start_date, end_date=end_date)

    def append_downtime(self, from_date: datetime.datetime, to_date: datetime.datetime,
                        to_which: str = "shutdown"):
        # to_which: str = "shutdown"
        appending_tuple: tuple = (from_date, to_date)

        if to_which == "shutdown":
            self.seq_shutdown.append(appending_tuple)
            self.seq_shutdown.sort()
        elif to_which == "breakdown":
            self.seq_breakdown.append(appending_tuple)
            self.seq_breakdown.sort()
        elif to_which == "daily":
            self.seq_daily.append(appending_tuple)
            self.seq_daily.sort()

        self._machine.hasCalendar = True

    def rebuild_break_sequence(self, which_seq: str):
        target_seq_dict: dict = {
            "full": self.seq_full,
            "daily": self.seq_daily,
            "shutdown": self.seq_shutdown,
            "breakdown": self.seq_breakdown
        }

        intersecting_cnt: int = 0

        before_seq: list = target_seq_dict[which_seq]

        if len(before_seq) == 0:
            return None

        merged_to_prior_one: bool = False
        new_seq: list = []
        for i in range(len(before_seq) - 1):
            dissolved_down: tuple = None
            prior_down: tuple = before_seq[i]
            succd_down: tuple = before_seq[i + 1]

            if merged_to_prior_one:
                merged_to_prior_one = False
                continue

            flag: bool = self._machine._chkOverlapToMacStopPeriod(
                from_to_tuple=prior_down,
                start_time=succd_down[0], end_time=succd_down[1]
            )

            if flag:
                intersecting_cnt += 1
                dissolved_down = (
                    min([prior_down[0], succd_down[0]]),
                    max([prior_down[1], succd_down[1]])
                )
                new_seq.append(dissolved_down)
                merged_to_prior_one = True
            else:
                new_seq.append(prior_down)

        if intersecting_cnt == 0:
            return new_seq
        else:
            if which_seq == "full":
                self.seq_full = new_seq
            elif which_seq == "daily":
                self.seq_daily = new_seq
            elif which_seq == "shutdown":
                self.seq_shutdown = new_seq
            elif which_seq == "breakdown":
                self.seq_breakdown = new_seq
            else:
                raise KeyError(
                    f"FIX ME !! from {self.__class__.__name__}.rebuild_break_sequence()"
                )
            self.rebuild_break_sequence(which_seq=which_seq)

        self._pruning_len_zero_intervals(which_seq=which_seq)

    def build_full_sequence(self):

        self.seq_full = self.seq_daily + self.seq_shutdown + self.seq_breakdown

        # for seq in self.seq_full:
        #     print(seq)

    def sort_seq(self, which_seq: str):
        if which_seq == "full":
            self.seq_full.sort()
        elif which_seq == "daily":
            self.seq_daily.sort()
        elif which_seq == "shutdown":
            self.seq_shutdown.sort()
        elif which_seq == "breakdown":
            self.seq_breakdown.sort()
        else:
            raise KeyError(
                f"FIX ME !! from {self.__class__.__name__}.rebuild_break_sequence()"
            )

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

        self.seq_daily = breakdown_seq

        if return_flag:
            return breakdown_seq

    def _get_first_time(self, start_hour: int, end_hour: int, target_date: datetime.datetime):
        from_date: datetime.datetime = None
        to_date: datetime.datetime = None

        from_date = target_date - datetime.timedelta(days=1)
        from_date = from_date.replace(hour=end_hour)

        # start_date = min(from_date, start_date)
        return from_date

    def _pruning_len_zero_intervals(self, which_seq: str):
        # self.macStopSeq = [
        #     tup for tup in self.macStopSeq
        #     if tup[0] == tup[1]
        # ]

        if which_seq == "full":
            self.seq_full = [
                tup for tup in self.seq_full
                if tup[0] != tup[1]
            ]
        elif which_seq == "daily":
            self.seq_daily = [
                tup for tup in self.seq_daily
                if tup[0] != tup[1]
            ]
        elif which_seq == "shutdown":
            self.seq_shutdown = [
                tup for tup in self.seq_shutdown
                if tup[0] != tup[1]
            ]
        elif which_seq == "breakdown":
            self.seq_breakdown = [
                tup for tup in self.seq_breakdown
                if tup[0] != tup[1]
            ]
        else:
            raise KeyError(
                f"FIX ME !! from {self.__class__.__name__}.rebuild_break_sequence()"
            )


def test():

    from PE_Simulator import Simulator
    from simOperMgr import Operation

    simulator: Simulator = Simulator()
    simulator.SetupDbObject(
        source="db", day_start_time=comUtility.Utility.DayStartTime
    )

    factory: simFactoryMgr.Factory = simFactoryMgr.Factory(simul=simulator, facID="IM_FACTORY")

    operation: Operation = Operation(factory=factory, oper_id="BAG", kind="BAGGING")

    machine: objMachine.Machine = objMachine.Machine(
        factory=factory, operation=operation, mac_id="BEGGAR"
    )

    comUtility.Utility.setDayStartDate(year=2020, month=3, day=9, hour=9)
    comUtility.Utility.SetDayHorizon(days=10)
    comUtility.Utility.CalcDayEndDate()
    print(f"DayStartDate = {comUtility.Utility.DayStartDate}")
    print(f"DayEndDate = {comUtility.Utility.DayEndDate}")

    calendar: CalendarManager = CalendarManager()



    from_yyyy: str = comUtility.Utility.PlanStartDay[:4]
    from_mm: str = "%02d" % factory._dataMgr.dmdMonth
    from_yyyymm: str = from_yyyy + from_mm
    to_dd: int = comUtility.Utility.GetMonthMaxDay(year=int(from_yyyy), month=factory._dataMgr.dmdMonth)[-1]
    demand = factory._dataMgr._conMgr.GetDbData(factory._dataMgr._conMgr.GetDpQtyDataSql_Custom(from_yyyymm, from_yyyymm))

    calendar.SetupObject(
        factory=factory, machine=machine,
        start_date=comUtility.Utility.DayStartDate,
        end_date=comUtility.Utility.DayEndDate,
        start_hour=8, end_hour=20
    )

    from_date: datetime.datetime = calendar._get_first_time(start_hour=8, end_hour=20, target_date=comUtility.Utility.DayStartDate)
    print(f"from_date = {from_date}")

    breaks_seq: list = calendar._build_daily_break_sequence(start_hour=8, end_hour=20, start_date=comUtility.Utility.DayStartDate, end_date=comUtility.Utility.DayEndDate)

    calendar.append_downtime(from_date=datetime.datetime(2020, 5, 11, 0, 0, 0),
                             to_date=datetime.datetime(2020, 5, 12, 0, 0, 0))
    breaks_seq = calendar.seq_daily

    print("\nBEFORE")
    for breaktime in breaks_seq:
        print(breaktime)

    calendar.build_full_sequence()

    print("\nAFTER")
    for breaktime in breaks_seq:
        print(breaktime)


if __name__ == '__main__':
    test()
