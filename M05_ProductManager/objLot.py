# -*- coding: utf-8 -*-

import re
import datetime
import calendar
import math

from M02_DataManager import dbDataMgr
from M03_Site import simOperMgr
from M04_PhyProductionMgr import objMachine, objWarehouse
from M06_Utility import comEnum, comUtility


class Lot(object):

    lot_id_regex = re.compile(comEnum.RegexCollection.lot_id.value)
    lot_id_format: dict = {
        "Grade": 0,
        "PackSize": 1,
        "PackType": 2
    }

    def __init__(self, id: str, prodId: str, loc: object):
        self.Id: str = id
        self.ProdId: str = prodId
        self.Grade: str = ""        # GRADE_A / GRADE_B / GRADE_C / GRADE_D / GRADE_E/ GRADE_F / GRADE_G / GRADE_H
        self.PackSize: str = ""     # P2 / P7 / P9
        self.PackType: str = ""     # WV / FS / BK / SB

        self.Lpst: int = 0

        self.Region: str = ""

        self.Duration: int = 0.0
        self.PackDuration: float = 0.0
        self.ReactDuration: float = 0.0

        self.DueDate: datetime.datetime = None
        self.StartTimeMin: datetime.datetime = None
        self.StartTimeMax: datetime.datetime = None

        self.Qty: float = 0.0

        self.Location: object = loc
        self.ToLoc: str = None

        # Silo 관련 속성
        self.Silo = ""

        self.Oper = simOperMgr.Operation = None
        self.Machine: objMachine.Machine = None
        self.WareHouse: objWarehouse.Warehouse = None


    def setup_object(self, due_date: str, qty: float, region: str):

        self.Grade = self._get_attr_from_id(id=self.Id, attr="Grade")
        self.PackSize = self._get_attr_from_id(id=self.Id, attr="PackSize")
        self.PackType = self._get_attr_from_id(id=self.Id, attr="PackType").split("_")[0]

        self.Region = region

        self.DueDate = self._get_last_day_of_month(due_date=due_date)

        self.Qty = qty

        self.PackDuration = self._get_pack_duration(grade=self.Id)
        self.ReactDuration = self._get_react_duration(grade=self.Grade)
        self.Duration = math.ceil(self.PackDuration + self.ReactDuration)

        self.StartTimeMax = self.DueDate - datetime.timedelta(hours=self.Duration)
        self.StartTimeMin = self.DueDate.replace(day=1, hour=8, minute=0, second=0)

    def _get_last_day_of_month(self, due_date: str):
        date_tmp: datetime.datetime = datetime.datetime.strptime(due_date, '%Y%m')
        last_day, month_len = calendar.monthrange(year=date_tmp.year, month=date_tmp.month)
        date_tmp = date_tmp.replace(day=month_len, hour=23, minute=59, second=59)
        return date_tmp

    def _get_pack_duration(self, grade: str):

        dataMgr: dbDataMgr.DataManager = comUtility.Utility.get_data_manager()
        dict_prod_yield: dict = dataMgr._get_dict_prod_yield()
        dict_prod_yield: dict = dict_prod_yield['package']
        if grade not in dict_prod_yield.keys():
            grade_adj: str = grade[:grade.find("_", grade.find("_") + 1)]
            if grade_adj not in dict_prod_yield.keys():
                raise Exception(
                    ""
                )
            rslt: float = self.Qty/dict_prod_yield[grade_adj]
        else:
            rslt: float = self.Qty/dict_prod_yield[grade]
        return rslt


    def _get_react_duration(self, grade: str):

        dataMgr: dbDataMgr.DataManager = comUtility.Utility.get_data_manager()
        dict_prod_yield: dict = dataMgr._get_dict_prod_yield()
        dict_prod_yield: dict = dict_prod_yield['reactor']
        if grade not in dict_prod_yield.keys():
            raise Exception(
                ""
            )
        rslt: float = self.Qty/dict_prod_yield[grade]
        return rslt

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

