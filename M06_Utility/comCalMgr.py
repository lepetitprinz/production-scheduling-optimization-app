# -*- coding: utf-8 -*-

class CalendarManager:

    def __init__(self, siteID: str, defWorkFlag: bool, dayStartTime: str):
        self.SiteID : str = siteID                                      #
        # self.WorkCalendar:comWorkCal.WorkCalendar                      #
        self.FacCalTimeLine:list = []                                   # Factory Calendar Time Line
        self.MacCalTimeLineList:list = []                               # Calendar Time Info Obj 리스트.
        self._macCalIdList:list = []                                    # Machine Calendar Obj 조회용 Cal ID 리스트
        self._defWorkFlag:bool = defWorkFlag                            # Ture: / False
        self.DayStartTime:str = dayStartTime