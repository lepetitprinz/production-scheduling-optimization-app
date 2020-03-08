# -*- coding: utf-8 -*-

import datetime

from M03_Site import simFactoryMgr
from M02_DataManager import dbDataMgr
from M03_Site import simFactoryMgr, simOperMgr
from M05_ProductManager import objLot
from M06_Utility import comUtility

import numpy as np
from itertools import permutations

class Simulator:
    def __init__(self):
        self._util = comUtility.Utility
        self.DataMgr: dbDataMgr.DataManager = None
        self._facObjList: list = []

    def SetupDbObject(self, year: int, month: int, day: int, day_start_time: str, horizon_days: int):
        self.DataMgr = dbDataMgr.DataManager(source="file")
        self.DataMgr.SetupObject()
        self._util.setup_object(simul=self)
        self._util.set_runtime(runtime=0)

        print("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*= Configuration Information =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=")
        print("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=")

        # Factory 인스턴스 세팅
        self._create_new_factory(factory_id="HARDCODED_FAC", day_start_time=day_start_time, year=year, month=month, day=day, horizon_days=horizon_days)

        flag = self.SetupObject()

    def SetupObject(self):

        operObj: simOperMgr = None
        facID: str = ""

        for obj in self._facObjList:
            facObj: simFactoryMgr = obj
            facObj.SetupResumeData()
            facObj.sendInitEvent()

    def run_simulator(self):
        if len(self._facObjList) < 1:
            # Factory가 없는 경우?
            return
        elif len(self._facObjList) == 1:
            self._run_single_factory()
        else:
            self._run_multi_factory()

    def _run_single_factory(self):
        facObj: simFactoryMgr.Factory = self._facObjList[0]
        # 머신 깨우기
        facObj.wake_up_machine()
        # Lot 할당
        facObj.AssignLot()

        self._util.set_runtime(runtime=self._util.DayStartDate)

        facObj.run_factory()

    def _run_multi_factory(self):
        pass

    def _create_new_factory(self, factory_id: str, day_start_time: str, year: int, month: int, day: int, horizon_days: int):
        facObj: simFactoryMgr = simFactoryMgr.Factory(simul=self, facID=factory_id)
        facObj.SetupObject(
            dayStartTime=day_start_time,
            year=year,
            month=month,
            day=day,
            horizon_days=horizon_days
        )
        self._facObjList.append(facObj)




    ########################################
    # 제약
    ########################################

    # Time Constraint
    # 1.Reactor Constraint

    # 2.Packaging Constraint



    # def getMinProdWheelCost(self, dmdReactorProdDict:dict, gradeCostDict:dict, costCalStandard:str = 'hour'):
    #
    #     '''
    #     dmdReactorProdDict : {gradeGroup1: [GRADE_A, ...],
    #                           gradeGroup2 : [GRADE_D, ...]}
    #     gradeCostDict : {(GRADE_A, GRADE_B) : [Hour, OG_Qty],
    #                      (GRADE_A, GRADE_C) : [Hour, OG_Qty],
    #                      ...}
    #     costCalStandard : hour or ogQty
    #     '''
    #
    #     appliedGradeCost = {}
    #
    #     if costCalStandard == 'hour':
    #         for key, val in gradeCostDict.items():
    #             appliedGradeCost.update({key, val[0]})
    #     else:
    #         for key, val in gradeCostDict.items():
    #             appliedGradeCost.update({key, val[1]})
    #
    #     gradeGroupSeq = []
    #     gradeSeq = []
    #
    #     for val in dmdReactorProdDict.values():
    #         gradeGroupSeq.append(list(permutations(val, len(val))))
    #
    #     # Make production schedule sequence
    #     for group1 in gradeGroupSeq[0]:
    #         group1 = list(group1)
    #         for group2 in gradeGroupSeq[1]:
    #             group2 = list(group2)
    #             seq = np.append(group1, group2)
    #             gradeSeq.append(seq)