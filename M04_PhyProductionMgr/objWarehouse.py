# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from M03_Site import simFactoryMgr
from M05_ProductManager import objLot


class Warehouse:
    def __init__(self, factory: simFactoryMgr, whId: str, kind: str):
        self._factory: simFactoryMgr = factory
        self.Id: str = whId
        self.Kind: str = kind                            # RM / WareHouse / silo / hopper
        self.LotObjList: list = []
        self.Capacity: int = 0          # warehouse 고유 capa
        self.CurCapa: int = 0           # 현재 할당된 재고를 고려한 capa

        self.ToLoc: object = None

    def setup_object(self, capacity: float = None):
        self._setCapacity(capacity=capacity)

    def setup_resume_data(self, lotObjArr: pd.DataFrame):
        for idx, row in lotObjArr.iterrows():
            print(row)
            lotObj: objLot.Lot = objLot.Lot(id=row['product'], prodId=row['product'], loc=self)
            lotObj.setup_object(
                due_date=row['yyyymm'],
                qty=row['qty'],
                region=row['region']
            )
            self._registerLotObj(lotObj=lotObj)
            # lotObj: objLot = obj
            # self._register_lot_obj(lot_obj=lotObj)

    def set_to_location(self, to_loc: object):
        self.ToLoc = to_loc

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


def test():
    pass


if __name__ == '__main__':
    test()
