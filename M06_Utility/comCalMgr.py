# -*- coding: utf-8 -*-

import pandas as pd

from M03_Site import simFactoryMgr
from M04_PhyProductionMgr import objMachine

class CalendarManager:

    # def __init__(self, siteID: str, defWorkFlag: bool, dayStartTime: str):
    #     self.SiteID : str = siteID                                      #
    #     # self.WorkCalendar:comWorkCal.WorkCalendar                      #
    #     self.FacCalTimeLine:list = []                                   # Factory Calendar Time Line
    #     self.MacCalTimeLineList:list = []                               # Calendar Time Info Obj 리스트.
    #     self._macCalIdList:list = []                                    # Machine Calendar Obj 조회용 Cal ID 리스트
    #     self._defWorkFlag:bool = defWorkFlag                            # Ture: / False
    #     self.DayStartTime:str = dayStartTime

    def __init__(self, site: object):

        self._factory: simFactoryMgr.Factory = None

        self.df_breakdown: pd.DataFrame = pd.DataFrame(
            {
                "MacID": [],
                "FromDate": [],
                "ToDate": []
            }
        )

    def SetupObject(self, factory: simFactoryMgr, daily_hour: tuple = None, weekly_day: tuple = None):

        self._factory = factory

        if daily_hour is not None:
            for obj in self._factory.MachineList:
                macObj: objMachine.Machine = obj
                self._append_daily_breakdown(macId=macObj.Id, from_hour=daily_hour[0], to_hour=daily_hour[1])

    def _append_daily_breakdown(self, macId: str, from_hour: int, to_hour: int):
        arr: pd.DataFrame = pd.DataFrame(
            {
                "MacID": [],
                "FromDate": [],
                "ToDate": []
            }
        )

    def _get_machine_location(self, machine: objMachine):
        pass
