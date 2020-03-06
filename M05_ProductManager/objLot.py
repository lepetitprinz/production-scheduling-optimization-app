# -*- coding: utf-8 -*-

import re

from M02_DataManager import dbDataMgr
from M03_Site import simOperMgr
from M04_PhyProductionMgr import objMachine, objWarehouse
from M06_Utility import comEnum, comUtility


class Lot(object):

    lot_id_regex: re.Pattern = re.compile(comEnum.RegexCollection.lot_id.value)
    lot_id_format: dict = {
                            "Grade": 0,
                            "PackSize": 1,
                            "PackType": 2
                          }

    def __init__(self, id: str, loc: object):
        self.Id: str = id
        self.Grade: str = ""
        self.FinalProd:str = ""
        self.PackSize: str = ""
        self.PackType: str = ""

        self.DueDate: str = ""
        self.Duration: float = 0.0
        self.PackDuration: float = 0.0
        self.ReactDuration: float = 0.0

        self.StartTimeMin: str = ""
        self.StartTimeMac: str = ""

        self.Region: str = ""
        self.Qty: float = 0.0
        self.Priority: int = None

        self.FromLoc: object = loc
        self.Location: object = loc
        self.ToLoc: object = None

        # self.Oper = None
        self.Machine: objMachine.Machine = None
        self.WareHouse: objWarehouse.Warehouse = None
        if type(self.Location) is objMachine.Machine:
            self.Machine = self.Location
        elif type(self.Location) is objWarehouse.Warehouse:
            self.WareHouse = self.Location
        else:
            pass

    def setup_object(self, due_date: str, qty: float, region: str, duration: float):

        self.Grade = self._get_attr_from_id(id=self.Id, attr="Grade")
        self.PackSize = self._get_attr_from_id(id=self.Id, attr="PackSize")
        self.PackType = self._get_attr_from_id(id=self.Id, attr="PackType")

        self.Region = region

        self.Duration

        self.DueDate = due_date

        self.Qty = qty


    def _get_pack_duration(self, grade: str):

        dataMgr: dbDataMgr.DataManager = comUtility.Utility.get_data_manager()
        dict_prod_yield: dict = dataMgr._get_dict_prod_yield()
        dict_prod_yield: dict = dict_prod_yield['package']
        if grade not in dict_prod_yield.keys():
            raise Exception(
                ""
            )
        rslt: float = dict_prod_yield[grade]
        return rslt

    def _get_react_duration(self, grade: str):

        dataMgr: dbDataMgr.DataManager = comUtility.Utility.get_data_manager()
        dict_prod_yield: dict = dataMgr._get_dict_prod_yield()
        dict_prod_yield: dict = dict_prod_yield['reactor']

    def _get_attr_from_id(self, id: str, attr: str):
        # if not self._chk_id_format(id):
        #     raise AssertionError(
        #         f"Lot 에 지정한 ID : {id} 는 형식에 맞지 않습니다."
        #     )
        if attr not in Lot.lot_id_format.keys():
            raise KeyError(
                f"Lot ID {id} 에는 지정한 속성 {attr} 정보가 없습니다."
            )

        attr_value: str = id.split('/')[Lot.lot_id_format[attr]]
        return attr_value

    def _chk_id_format(self, id: str):
        matches: bool = False
        if Lot.lot_id_regex.match(id) is not None:
            matches = True
        return matches


def test():
    pass


if __name__ == '__main__':
    test()
