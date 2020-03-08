# -*- coding: utf-8

import datetime

from M04_PhyProductionMgr import objMachine, objStocker
from M05_ProductManager import objLot

class Operation(object):
    def __init__(self, oper_id: str, kind: str):
        # self._facUtil: facUtility.FacUtility = None  #
        self.Id: str = oper_id
        self.Kind: str = kind

        self.ToLoc: object = None

        self.MacObjList: list = []
        self.StockObj: objStocker.Stocker = None

        self.FirstEventTime: datetime.datetime = None

    def setup_object(self):
        pass
        # self.set_to_location(to_loc)

    def SetupResumeData(self, lotObjList: list):
        # self.ProdLotList 에 추가하는 처리.
        # TAT모드인 경우, ActEndTime 계산 필요.
        # 09/19 self._lotCnt 에 카운트 필요.
        lotObjList = self._checkInitErrorLotList(lotObjList=lotObjList)

    def AppendMac(self, tgtMac: objMachine):
        self.MacObjList.append(tgtMac)

    def lot_arrive(self, lot: objLot.Lot):
        machines: list = self._get_available_machines()
        machine: objMachine.Machine = self._pick_machine(macList=machines)

    def _pick_machine(self, macList: list):
        return macList[0]

    def reset_first_event_time(self):
        mac_end_times: list = [mac.EndTime for mac in self.MacObjList]
        if sum([endTime is None for endTime in mac_end_times]) > 0:
            self.FirstEventTime = None
        else:
            mac_end_times = [endTime for endTime in mac_end_times if endTime is not None]
            self.FirstEventTime = min(mac_end_times)

    def set_to_location(self, to_loc: object):
        self.ToLoc = to_loc

    def _get_available_machines(self):
        avaliable_machines: list = []
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            if macObj.Status == "IDLE":
                avaliable_machines.append(macObj)
        return avaliable_machines

    def get_assignable_flag(self):
        available_machines: list = self._get_available_machines()
        return len(available_machines) == 0 , available_machines

