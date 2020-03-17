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
        self.ProdCode:str = ""
        self.Grade: str = ""        # GRADE_A / GRADE_B / GRADE_C / GRADE_D / GRADE_E/ GRADE_F / GRADE_G / GRADE_H
        self.PackSize: str = ""     # P2 / P7 / P9
        self.PackType: str = ""     # WV / FS / BK / SB

        self.Lpst: int = 0

        self.Duration: int = 0
        self.ReactDurationFloat: float = 0.0
        self.ReactDuration: datetime.timedelta = datetime.timedelta(hours=0)
        self.PackDurationFloat: float = 0.0
        self.PackDuration: datetime.timedelta = datetime.timedelta(hours=0)

        self.DueDate: datetime.datetime = None
        self.StartTimeMin: datetime.datetime = None
        self.StartTimeMax: datetime.datetime = None

        # History
        self.ReactIn: datetime.datetime = None
        self.ReactOut: datetime.datetime = None
        self.BaggingIn: datetime.datetime = None
        self.BaggingOut: datetime.datetime = None

        self.Qty: float = 0.0

        self.Location: object = loc
        self.CurrLoc: str = ''
        self.ToLoc: str = ''

        # Silo 관련 속성
        self.Silo = ""

        self.Oper = simOperMgr.Operation = None
        self.Machine: objMachine.Machine = None
        self.WareHouse: objWarehouse.Warehouse = None

    def setupObject(self, due_date: str, prodCode: str, qty: float):

        self.ProdCode = prodCode
        self.Grade = self._get_attr_from_id(id=self.Id, attr="Grade")
        self.PackSize = self._get_attr_from_id(id=self.Id, attr="PackSize")
        self.PackType = self._get_attr_from_id(id=self.Id, attr="PackType").split("_")[0]
        self.Qty = qty

        # Time 관련 속성 setting
        self.DueDate = self._getLastDayOfMonth(due_date=due_date)
        self.PackDuration, self.PackDurationFloat = self._getBaggingDuration(grade=self.Id)
        self.ReactDuration, self.ReactDurationFloat = self._getReactorDuration(grade=self.Grade)
        self.Duration = math.ceil(self.PackDurationFloat + self.ReactDurationFloat)

        self.StartTimeMax = self.DueDate - datetime.timedelta(hours=self.Duration)
        self.StartTimeMin = self.DueDate.replace(day=1, hour=8, minute=0, second=0)

    def SetLocation(self, location: str, currLoc: str):
        self.Location = location
        self.CurrLoc = currLoc

    def set_attr(self, attr: str, value: object):
        if not self._does_attr_exist(attr=attr):
            raise AttributeError(
                f"{self.__class__.__name__} Object Does not have Attribute called {attr} !"
            )
        self.__setattr__(name=attr, value=value)

    def reduce_duration(self, by: float):
        self.PackDuration, self.PackDurationFloat = self._getBaggingDuration(grade=self.Id, by=by)
        self.ReactDuration, self.ReactDurationFloat = self._getReactorDuration(grade=self.Grade, by=by)
        self.Duration = math.ceil(self.PackDurationFloat + self.ReactDurationFloat)

        self.StartTimeMax = self.DueDate - datetime.timedelta(hours=self.Duration)
        self.StartTimeMin = self.DueDate.replace(day=1, hour=8, minute=0, second=0)

    def _getLastDayOfMonth(self, due_date: str):
        date_tmp: datetime.datetime = datetime.datetime.strptime(due_date, '%Y%m')
        last_day, month_len = calendar.monthrange(year=date_tmp.year, month=date_tmp.month)
        date_tmp = date_tmp.replace(day=month_len, hour=23, minute=59, second=59)
        return date_tmp

    def _getBaggingDuration(self, grade: str, by: float=1.0):
        dataMgr: dbDataMgr.DataManager = comUtility.Utility.GetDataManager()
        dict_prod_yield: dict = dataMgr._get_dict_prod_yield()
        dict_prod_yield: dict = dict_prod_yield['package']
        rslt: datetime.timedelta = datetime.timedelta(hours=0)
        if grade not in dict_prod_yield.keys():
            grade_adj: str = grade[:grade.find("_", grade.find("_") + 1)]
            if grade_adj not in dict_prod_yield.keys():
                raise Exception("")
            tmp_rslt: float = self.Qty/dict_prod_yield[grade_adj]
        else:
            tmp_rslt: float = self.Qty/dict_prod_yield[grade]
        rslt = datetime.timedelta(hours=tmp_rslt/by, microseconds=0)
        rslt_chp = comUtility.Utility.chop_microsecond(rslt)
        return rslt_chp, tmp_rslt

    def _getReactorDuration(self, grade: str, by: float=1.0):
        dataMgr: dbDataMgr.DataManager = comUtility.Utility.GetDataManager()
        dict_prod_yield: dict = dataMgr._get_dict_prod_yield()
        dict_prod_yield: dict = dict_prod_yield['reactor']
        rslt: datetime.timedelta = datetime.timedelta(hours=0)
        if grade not in dict_prod_yield.keys():
            raise Exception("")
        tmp_rslt: float = self.Qty/dict_prod_yield[grade]
        rslt = datetime.timedelta(hours=tmp_rslt/by, microseconds=0)
        rslt_chp = comUtility.Utility.chop_microsecond(rslt)
        return rslt_chp, tmp_rslt

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

    def _does_attr_exist(self, attr: str):
        does_attr_exists: bool = attr in self.__dict__.keys()
        return does_attr_exists

def test():
    pass

if __name__ == '__main__':
    test()

