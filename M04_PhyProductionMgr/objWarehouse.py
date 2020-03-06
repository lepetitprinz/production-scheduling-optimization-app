# -*- coding: utf-8 -*-

import pandas as pd
import numpy
from M03_Site import simFactoryMgr
from M05_ProductManager import objLot


class Warehouse:
    def __init__(self, factory: simFactoryMgr, wh_id: str):
        self._factory: simFactoryMgr = factory
        self.id: str = wh_id
        self.lot_obj_list: list = []
        self.capacity:int = 0


    def setup_object(self):
        pass

    def setup_resume_data(self, lot_obj_array: pd.DataFrame):
        for idx, row in lot_obj_array.iterrows():
            print(row)
            lotObj: objLot.Lot = objLot.Lot(id=row['product'], loc=self)
            lotObj.setup_object(
                due_date=row['yyyymm'],
                qty=row['qty'],
                region=row['region']
            )
            self._register_lot_obj(lot_obj=lotObj)
            # lotObj: objLot = obj
            # self._register_lot_obj(lot_obj=lotObj)

    def _register_lot_obj(self, lot_obj: objLot):
        if type(lot_obj) is not objLot.Lot:
            raise TypeError(
                "Lot 객체가 아닌것을 Lot 객체 리스트에 Append 하려 하고 있습니다."
            )
        self.lot_obj_list.append(lot_obj)
        self._factory._register_lot_to(lot_obj=lot_obj, to="self")


def test():
    pass


if __name__ == '__main__':
    test()
