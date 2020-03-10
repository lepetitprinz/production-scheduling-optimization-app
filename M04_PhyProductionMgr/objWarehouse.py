# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import datetime

from M03_Site import simFactoryMgr, simOperMgr
from M05_ProductManager import objLot
from M06_Utility.comUtility import Utility


class Warehouse:
    def __init__(self, factory: simFactoryMgr, whId: str, kind: str):
        self._factory: simFactoryMgr = factory
        self.Id: str = whId
        self.Kind: str = kind                            # RM / WareHouse / silo / hopper
        self.LotObjList: list = []
        self.LpstLotDict: dict = {}
        self.Capacity: int = 0          # warehouse 고유 capa
        self.CurCapa: int = 0           # 현재 할당된 재고를 고려한 capa

        self.ToLoc: object = None

        self.FirstEventTime: datetime.datetime = None

        # Time Constraint
        self.GradeChangeFinishConst: bool = False
        self.GradeGroupChangeConst: bool = False
        self.BaggingOperTimeConst: bool = False

        self.BeforeLotList: list = []
        self._prodWheelHour:dict = {}

    def setup_object(self, capacity: float = None):
        self._setCapacity(capacity=capacity)

        # Time Constraint Configuration Setting
        self.GradeChangeFinishConst = Utility.GradeChangeFinishConst
        self.GradeGroupChangeConst = Utility.GradeGroupChangeConst
        self.BaggingOperTimeConst = Utility.BaggingOperTimeConst

        self._prodWheelHour = Utility.ProdWheelHour

    def setup_resume_data(self, lotObjArr: pd.DataFrame):
        for idx, row in lotObjArr.iterrows():
            print(row)
            prodId: str = row['product'][:row['product'].find("_", row['product'].find("_") + 1)]
            lotObj: objLot.Lot = objLot.Lot(id=row['product'], prodId=prodId, loc=self)
            lotObj.setup_object(
                due_date = row['yyyymm'],
                qty = row['qty'],
                region = row['region']
            )
            self._registerLotObj(lotObj=lotObj)
            # lotObj: objLot = obj
            # self._register_lot_obj(lot_obj=lotObj)

    def set_to_location(self, to_loc: object):
        self.ToLoc = to_loc

    def SyncRunningTime(self):
        lotObj: objLot.Lot = self._pickAvailableLot()

        # 최종 생산완료 된 경우 출하 처리
        if self.ToLoc == "Sales":
            self.shipping()

        else:
            # 선택한 Lot에 대해 할당 가능한 Operation - Machine을 찾는 처리
            to_oper, available_machines = self._findAvailableOper(lot=lotObj)
            if len(available_machines) > 0:
                self.lot_leave(to_loc=to_oper, lot=lotObj)
                self.setFstEventTime()
            if len(available_machines) == 0:
                # is_in_break, break_end_time = to_oper.are_machines_in_break(lot=least_lpst_lot)
                # if len(is_in_break)
                print(f"\t\t{to_oper.__class__.__name__}({to_oper.Id}) No Machines Avaiable. Waiting for Processing...")
                to_oper.ResetFstEventTime()
                to_oper.inform_to(from_obj=self, runTime=to_oper.FirstEventTime, downFlag=True)
                # to_oper.set_first_event_time()
                # self.set_first_event_time(break_end_time)

    def _shipping(self, lot: objLot):
        # least_lpst_lot: objLot.Lot = self._get_least_lpst_lot()

        self._removeLot(lot=lot, shipping_flag=True)
        self._updateCurrCapa(lot=lot, in_flag=False)
        # self._rebuild_lpst_lot_dict()
        self.setFstEventTime()

        print(f"\t\t{self.__class__.__name__}({self.Id}).shipping() >> {lot}")

    def shipping(self):
        for lot in self.LotObjList:
            self._shipping(lot=lot)

    def lot_leave(self, to_loc: simOperMgr, lot: objLot):
        # least_lpst_lot: objLot.Lot = self._get_least_lpst_lot()
        current_time: datetime.datetime = Utility.DayStartDate

        print(f"\t\t{self.__class__.__name__}({self.Id}).lot_leave() >> {(lot.Id, lot.Lpst, lot.ReactDuration, lot.PackDuration)}")

        to_loc.lotArrive(lot)
        self._removeLot(lot=lot)
        self._updateCurrCapa(lot=lot, in_flag=False)
        # self._rebuild_lpst_lot_dict()
        self.setFstEventTime()

    def lotArrive(self, from_loc: object, lot: objLot):
        lotObj: objLot.Lot = lot
        lotObj.ToLoc = self.ToLoc
        print(f"\t\t{self.__class__.__name__}({self.Id}).lot_arrive() >> {lotObj}")
        self._registerLotObj(lotObj=lotObj)
        self._updateCurrCapa(lot=lotObj, in_flag=True)
        # self._rebuild_lpst_lot_dict()
        self.setFstEventTime(Utility.runtime)

    def getAssignableFlag(self, lot: objLot):
        isAssignable = False
        lotObj: objLot.Lot = lot
        if self.CurCapa >= lotObj.Qty:
            isAssignable = True
        return isAssignable

    def resetCurCapa(self):
        self.CurCapa = int(self.Capacity)

    def _pickAvailableLot(self, rule: str = "FIRST"):
        if rule == "FIRST":
            first_lot: objLot.Lot = self.LotObjList[0]
            return first_lot

    def _updateCurrCapa(self, lot: objLot, in_flag: bool):
        rslt: float = 0.0
        lotObj: objLot.Lot = lot
        if in_flag:
            self.CurCapa -= lotObj.Qty
        else:
            self.CurCapa += lotObj.Qty

    def _findAvailableOper(self, lot: objLot):
        # rsltOper: simOperMgr.Operation = None
        # rsltMachines: list = []

        targetOperList: list = \
            [oper for oper in self._factory.OperList if oper.Kind == self.ToLoc]
        targetOper: simOperMgr.Operation = targetOperList[0]    # targetOper : reactor / bagging

        # Target으로 하는 공정에 대해애서 가능한 machine들을 찾는 처리
        is_oper_assignable, available_machines = targetOper.GetAssignableFlag(lot=lot)
        return targetOper, available_machines

        # if self.Id == "RM":
        #     for obj in self._factory.OperList:
        #         operObj: simOperMgr.Operation = obj
        #         is_oper_assignable, available_machines = operObj.get_assignable_flag()
        #         if operObj.Id == "REACTOR" and is_oper_assignable:
        #             rsltOper = operObj
        #             rsltMachines = available_machines

    def truncate_lot_list(self):
        self.LotObjList.clear()
        self._factory._lot_obj_list.clear()

    def _removeLot(self, lot: objLot, shipping_flag: bool = False):
        try:
            self.BeforeLotList = self.LotObjList
            self.LotObjList.remove(lot)
            if shipping_flag:
                self._factory._remove_lot(lot=lot)
        except ValueError as e:
            raise e

    def _get_least_lpst_lot(self):
        # self.assign_random_lpst()
        self._rebuild_lpst_lot_dict()
        Warning(f"Fix Me !! from {self.__class__}._get_least_lpst_lot !!")
        least_lpst_lot: objLot.Lot = self.LpstLotDict[min(self.LpstLotDict.keys())][0]
        return least_lpst_lot

    def _rebuild_lpst_lot_dict(self):
        self.LpstLotDict = dict()
        for obj in self.LotObjList:
            lotObj: objLot.Lot = obj
            lpst: int = lotObj.Lpst
            if lpst not in self.LpstLotDict.keys():
                self.LpstLotDict[lpst] = [lotObj]
            else:
                raise KeyError(
                    f"WareHouse {self.Id} 에 Lpst 값 (Lpst={lpst}) 이 중복되는 Lot 이 있습니다 ! "
                    f">> {self.LpstLotDict[lpst][0].Lpst} / {lotObj.Lpst}"
                )
                # self.LpstLotDict[lpst].append(lotObj)

    def _setCapacity(self, capacity: float):
        if self.Kind == "RM":
            self.Capacity = np.Inf
            self.CurCapa = np.Inf
        else:
            self.Capacity = capacity
            self.CurCapa = capacity
            # raise Exception(
            #     f"Make Me ! from {self.__class__}.setup_object !!"
            # )

    def _registerLotObj(self, lotObj: objLot):
        if type(lotObj) is not objLot.Lot:
            raise TypeError(
                "Lot 객체가 아닌것을 Lot 객체 리스트에 Append 하려 하고 있습니다."
            )
        lotObj: objLot.Lot = lotObj
        lotObj.set_location(self)
        self.LotObjList.append(lotObj)
        self._factory._register_lot_to(lot_obj=lotObj, to="self")

    def setFstEventTime(self, runTime: datetime.datetime = None):
        self.FirstEventTime = runTime

    # def assign_random_lpst(self):
    #     for obj in self.LotObjList:
    #         lotObj: objLot.Lot = obj
    #         lotObj.Lpst = self.LotObjList.index(lotObj)
    #     self._rebuild_lpst_lot_dict()

    # ------------------------------------------------------------------------------------------------------ #
    # Time Constraint
    # - Reactor (중합)
    # - Bagging (포장)
    # ------------------------------------------------------------------------------------------------------ #

    # Reactor Time Constraint
    def ChkAssignableToReactor(self, lot: objLot):

        if self.GradeChangeFinishConst == True:
            self._chkGradeChangeFinish(lot=lot)

        if self.GradeGroupChangeConst == True:
            self._chkGradeGroupChange(lot=lot)

        return True

    def _chkGradeChangeFinish(self, lot: objLot):
        pass

    def _chkGradeGroupChange(self, lot: objLot):
        pass

    # Bagging Time Constraint
    def ChkAssignableToBagging(self, lot: objLot):

        if self.BaggingOperTimeConst == True:
            self._chkBaggingOperTime(lot=lot)

        return True

    def _chkBaggingOperTime(self, lot: objLot):
        pass


def test():
    pass


if __name__ == '__main__':
    test()
