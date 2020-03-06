# -*- coding: utf-8 -*-

from M02_DataManager import dbDataMgr   # DataManager
from M03_Site import simFactoryMgr  # Factory
from M06_Utility import comUtility  # Utility
from M06_Utility import comCalMgr   # CalendarManager


class FacUtility:
    def __init__(self, facObj: simFactoryMgr, dataMgr: dbDataMgr):
        # ===============================
        self.FactoryObj: simFactoryMgr = facObj
        self._dataMgr: dbDataMgr = dataMgr
        self._comUtility: comUtility = comUtility.Utility                           # 공통 M06_Utility 를 참조
        self.CalendarMgr: comCalMgr = None               # Calendar Manager
        self._facFirstFlowID: str = ""
        self._facLastFlowID: str = ""

        # 관련 DB 정보 Array 변수들을 선언
        self._lotArr: list = []     # ()

    def GetInvLotObjList_byFac(self, wh_id: str):
        pass
