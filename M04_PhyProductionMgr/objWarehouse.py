# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import datetime

from M03_Site import simFactoryMgr
from M05_ProductManager import objLot
from M06_Utility import comUtility


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

    def setup_object(self, capacity: float = None):
        self._setCapacity(capacity=capacity)

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
        # to_loc: object =
        self.lot_leave(to_loc=to_loc)

    def lot_leave(self, to_loc: object):
        least_lpst_lot: objLot.Lot = self._get_least_lpst_lot()
        current_time: datetime.datetime = comUtility.Utility.DayStartDate

        to_loc.lot_arrive(least_lpst_lot)
        self._remove_lot(lot=least_lpst_lot)
        self._rebuild_lpst_lot_dict()

    def lot_arrive(self, from_loc: object, lot: objLot):
        lotObj: objLot.Lot = lot
        self._registerLotObj(lotObj=lotObj)
        self._rebuild_lpst_lot_dict()

    def _remove_lot(self, lot: objLot):
        try:
            self.LotObjList.remove(lot)
        except ValueError:
            pass

    def _get_least_lpst_lot(self):
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
        self.LotObjList.append(lotObj)
        self._factory._register_lot_to(lot_obj=lotObj, to="self")

    def set_first_event_time(self, runTime: datetime.datetime = None):
        self.FirstEventTime = runTime

def test():
    pass


if __name__ == '__main__':
    test()
