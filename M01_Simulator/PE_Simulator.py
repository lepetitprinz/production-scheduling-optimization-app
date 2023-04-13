# -*- coding: utf-8 -*-

import datetime

from M02_DataManager import dbDataMgr
from M03_Site import simFactoryMgr, simOperMgr
from M04_PhyProductionMgr import objWarehouse, objMachine
from M06_Utility import comUtility

class Simulator:
    FACTORY_ID = 'TEST'

    def __init__(self):
        self._util = comUtility.Utility
        self.DataMgr: dbDataMgr.DataManager = None
        self._whNgr: objWarehouse.Warehouse = None
        self._facObjList: list = []

    def SetupDbObject(self, source: str, day_start_time: str, dmdMonth: int = None):
        self.DataMgr = dbDataMgr.DataManager(source=source, dmdMonth=dmdMonth)
        engConfData = self.DataMgr.SetupEngConfData()
        self._util.SetupObject(simul=self, engConfig=engConfData)

        year = int(self._util.PlanStartTime[:4])
        month = int(self._util.PlanStartTime[4:6])
        day = int(self._util.PlanStartTime[6:])

        schedStartTime = self._util.PlanStartTime
        schedEndTime = self._util.PlanEndTime
        schedStartDateTime = datetime.datetime.strptime(schedStartTime, '%Y%m%d')
        schedEndDateTime = datetime.datetime.strptime(schedEndTime, '%Y%m%d')
        schedPeriod = str(schedEndDateTime - schedStartDateTime)
        schedPeriodDays = int(schedPeriod.split()[0]) + 1
        # horizon_days = schedPeriodDays  # 고정값 처리 가능

        siloCapa = self._util.SiloCapa
        SiloQty = self._util.SiloQty

        # Bagging Lead Time 제약
        if self._util.BaggingLeadTimeConst == True:
            silo_wait_hours = self._util.BaggingLeadTime
        else:
            silo_wait_hours = 0

        # DB에 있는 Data 정보 받아오는 처리
        self.DataMgr.SetupObject()
        self.DataMgr.build_demand_max_days_by_month()
        # engConfig = self.DataMgr.dbEngConf
        #self._util.setupObject(simul=self, engConfig=engConfig)
        # self._util.set_runtime(runtime=0)

        print("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*= Configuration Information =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=")
        print("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=")

        # Factory 인스턴스 세팅
        if self.DataMgr.dmdMonth is not None:
            self._createNewFactory(factory_id=FACTORY_ID, day_start_time=day_start_time,
                                   year=comUtility.Utility.DayStartDate.year,
                                   month=comUtility.Utility.DayStartDate.month,
                                   day=comUtility.Utility.DayStartDate.day,
                                   horizon_days=comUtility.Utility.DayHorizon.days,
                                   silo_qty=comUtility.Utility.SiloCapa,
                                   nof_silo=SiloQty, silo_wait_hours=int(silo_wait_hours))
        else:
            self._createNewFactory(factory_id=FACTORY_ID, day_start_time=day_start_time, year=year, month=month,
                                   day=day,
                                   horizon_days=comUtility.Utility.DayHorizon.days,
                                   silo_qty=comUtility.Utility.SiloCapa,
                                   nof_silo=SiloQty, silo_wait_hours=int(silo_wait_hours))

        self.SetupObject(use_mac_down_cal_db=True)

    def SetupObject(self, use_mac_down_cal_db: bool = False):

        operObj: simOperMgr = None
        facID: str = ""

        for obj in self._facObjList:
            facObj: simFactoryMgr = obj
            facObj.SetupResumeData(use_mac_down_cal_db=use_mac_down_cal_db)    # 현재 RM warehouse만 setting
            facObj.sendInitEvent()      # 공장 객체 초기화 정보를 DB에 전달(미구현)

    def runSimulator(self):
        if len(self._facObjList) < 1:
            print("Factory Object does not exist")
            raise AssertionError()
        elif len(self._facObjList) == 1:
            self._runSingleFactory()
        else:
            self._runMultiFactory()

    def _runSingleFactory(self):
        facObj: simFactoryMgr.Factory = self._facObjList[0]
        # Initialize Machine
        facObj.wake_up_machine()
        # Allocate Lot
        facObj.AssignLot()

        # Factory 초기 시작시간 셋팅
        self._util.SetRuntime(runtime=self._util.DayStartDate)

        # Factory 가동 시작
        facObj.RunFactory()

    def _runMultiFactory(self):
        pass

    def _createNewFactory(
            self,
            factory_id: str,
            day_start_time: str,
            year: int,
            month: int,
            day: int,
            horizon_days: int,
            silo_qty: int,
            nof_silo: int,
            silo_wait_hours: int = 0
    ):
        facObj: simFactoryMgr = simFactoryMgr.Factory(simul=self, facID=factory_id)
        facObj.SetupObject(
            dayStartTime=day_start_time,
            year=year,
            month=month,
            day=day,
            horizon_days=horizon_days,
            silo_qty=silo_qty,
            nof_silo=nof_silo,
            silo_wait_hours=silo_wait_hours
        )
        self._facObjList.append(facObj)

    def SaveSimulData(self):
        # self.DataMgr.SaveEngConfig()
        shortageLotObjList = []

        if len(self._facObjList) == 1:
            facObj: simFactoryMgr.Factory = self._facObjList[0]
            for wh in facObj.WhouseObjList:
                whObj:objWarehouse.Warehouse = wh

                # Final Production Data save
                if whObj.Kind == 'FGI':
                    prodScheduleRslt = whObj.ProdScheduleRsltArr
                    self.DataMgr.SaveProdScheduleRslt(prodScheduleRslt=prodScheduleRslt)

                if whObj.Kind == 'RM' or whObj.Kind == 'silo':
                    shortageLotObjList.extend(whObj.LotObjList)

            # Shortage Data Save
            for oper in facObj.OperList:
                operObj:simOperMgr.Operation = oper
                if operObj.Kind == 'REACTOR':
                    macObj: objMachine.Machine = operObj.MacObjList[0]
                    if macObj.Lot != None:
                        shortageLotObjList.append(macObj.Lot)
                else:
                    for mac in operObj.MacObjList:
                        macObj:objMachine.Machine = mac
                        if macObj.Lot != None:
                            shortageLotObjList.append(macObj.Lot)

            self.DataMgr.SaveShortageRslt(shortageLotList=shortageLotObjList)

            # Grade Change Cost Data Save
            for oper in facObj.OperList:
                operObj: simOperMgr.Operation = oper
                if operObj.Kind == 'REACTOR':
                    macObj: objMachine.Machine = operObj.MacObjList[0]
                    gradeChangeCostList = macObj.GradeChangeCostList
                    self.DataMgr.SaveGradeChangeCostRslt(gradeChangeCostList=gradeChangeCostList)

            # Shutdown Data Save
            if comUtility.Utility.ReactorShutdownYn == 'Y':
                self.DataMgr.SaveShutDownRslt()