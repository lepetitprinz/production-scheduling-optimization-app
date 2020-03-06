# -*- coding: utf-8 -*-

from M03_Site import simFactoryMgr
from M05_ProductManager import objLot


class Warehouse:
    def __init__(self, factory: simFactoryMgr, wh_id: str):
        self._factory: simFactoryMgr = factory
        self.id: str = wh_id
        self.lot_obj_list: list = []

    def setup_object(self):
        pass

    def setup_resume_data(self, lot_obj_list: list):
        for obj in lot_obj_list:
            lotObj: objLot = obj
            self._register_lot_obj(lot_obj=lotObj)

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
