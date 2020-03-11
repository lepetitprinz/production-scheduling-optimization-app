# -*- coding: utf-8 -*-

import datetime

from M03_Site import simFactoryMgr
from M02_DataManager import dbDataMgr
from M03_Site import simFactoryMgr, simOperMgr
from M04_PhyProductionMgr import objWarehouse
from M05_ProductManager import objLot
from M06_Utility import comUtility

import numpy as np
from itertools import permutations

class Simulator:
    def __init__(self):
        self._util = comUtility.Utility
        self.DataMgr: dbDataMgr.DataManager = None
        self._whNgr: objWarehouse.Warehouse = None
        self._facObjList: list = []

    def SetupDbObject(self, year: int, month: int, day: int, day_start_time: str, horizon_days: int, silo_qty: int, nof_silo: int = 1):
        self.DataMgr = dbDataMgr.DataManager(source="db")
        self.DataMgr.SetupObject()
        self.DataMgr.build_demand_max_days_by_month()
        self._util.setup_object(simul=self)
        # self._util.set_runtime(runtime=0)

        print("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*= Configuration Information =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=")
        print("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=")

        # Factory 인스턴스 세팅
        self._create_new_factory(factory_id="GS_CALTEX", day_start_time=day_start_time, year=year, month=month, day=day, horizon_days=horizon_days, silo_qty=silo_qty, nof_silo=nof_silo)

        flag = self.SetupObject()

    def SetupObject(self):

        operObj: simOperMgr = None
        facID: str = ""

        for obj in self._facObjList:
            facObj: simFactoryMgr = obj
            facObj.SetupResumeData()    # 현재 RM warehouse만 setting
            facObj.sendInitEvent()      # 공장 객체 초기화 정보를 DB에 전달(미구현)

    def run_simulator(self):
        if len(self._facObjList) < 1:
            # Factory가 없는 경우?
            print("Factory 객체 없음")
            raise AssertionError()
        elif len(self._facObjList) == 1:
            self._run_single_factory()
        else:
            self._run_multi_factory()

    def _run_single_factory(self):
        facObj: simFactoryMgr.Factory = self._facObjList[0]
        # 머신 깨우기
        facObj.wake_up_machine()
        # Lot 할당
        # facObj.AssignLot()

        # Factory 초기 시작시간 셋팅
        self._util.set_runtime(runtime=self._util.DayStartDate)

        # Factory 가동 시작
        facObj.run_factory()

    def _run_multi_factory(self):
        pass

    def _create_new_factory(self, factory_id: str, day_start_time: str, year: int, month: int, day: int, horizon_days: int, silo_qty: int, nof_silo: int):
        facObj: simFactoryMgr = simFactoryMgr.Factory(simul=self, facID=factory_id)
        facObj.SetupObject(
            dayStartTime=day_start_time,
            year=year,
            month=month,
            day=day,
            horizon_days=horizon_days,
            silo_qty=silo_qty,
            nof_silo=nof_silo
        )
        self._facObjList.append(facObj)

    def SaveSimulData(self):
        # self.DataMgr.SaveEngConfig()
        prodScheduleRslt = []

        if len(self._facObjList) == 1:
            facObj: simFactoryMgr.Factory = self._facObjList[0]
            for wh in facObj.WhouseObjList:
                whObj:objWarehouse.Warehouse = wh
                if whObj.Kind == 'FGI':
                    prodScheduleRslt = whObj.ProdScheduleRsltArr

            self.DataMgr.SaveProdScheduleRslt(prodScheduleRslt=prodScheduleRslt)
