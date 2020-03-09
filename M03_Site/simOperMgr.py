# -*- coding: utf-8

import datetime

from M03_Site import simFactoryMgr
from M04_PhyProductionMgr import objMachine, objStocker, objWarehouse
from M05_ProductManager import objLot
from M06_Utility import comUtility


class Operation(object):
    def __init__(self, factory: simFactoryMgr, oper_id: str, kind: str):
        # self._facUtil: facUtility.FacUtility = None  #
        self.Id: str = oper_id
        self.Kind: str = kind
        self._factory: simFactoryMgr.Factory = factory

        self.FromLocs: list = []
        self.ToLoc: object = None

        self.MacObjList: list = []
        self._lotObjList: list = []
        self.StockObj: objStocker.Stocker = None

        self.FirstEventTime: datetime.datetime = None

    def setup_object(self):
        pass
        # self.set_to_location(to_loc)

    # def SetupResumeData(self, lotObjList: list):
    #     # self.ProdLotList 에 추가하는 처리.
    #     # TAT모드인 경우, ActEndTime 계산 필요.
    #     # 09/19 self._lotCnt 에 카운트 필요.
    #     lotObjList = self._checkInitErrorLotList(lotObjList=lotObjList)

    def AppendMac(self, tgtMac: objMachine):
        self.MacObjList.append(tgtMac)

    def set_from_location(self, from_locs: list):
        self.FromLocs = from_locs

    def SyncRunningTime(self):
        # to_loc: object
        self._update_lot_list()
        if len(self._lotObjList) == 0:
            self.resume_down_machines()
        else:
            self.lot_leave()
        self.inform_to_previous()
        self.reset_first_event_time()

    def resume_down_machines(self):
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            macObj.power_on()
            print(f"\t\t{macObj.__class__.__name__} ({macObj.Id}) RESUMED FROM DOWN STATE !")

    def inform_to_previous(self, runTime: datetime.datetime = None):
        if runTime is None:
            runTime = comUtility.Utility.runtime
        for obj in self.FromLocs:
            self.inform_to(from_obj=obj, runTime=runTime)

    def inform_to(self, from_obj: objWarehouse.Warehouse, runTime: datetime.datetime, downFlag: bool = False):
        from_loc: objWarehouse.Warehouse = from_obj
        if from_loc.FirstEventTime is None:
            if len(from_loc.LotObjList) > 0:
                from_loc.set_first_event_time(runTime=runTime)
        else:
            if downFlag:
                from_loc.set_first_event_time(runTime=runTime)

    def lot_leave(self):
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            if macObj.EndTime == comUtility.Utility.runtime:
                lotObj: objLot.Lot = macObj.lot_leave()
                available_wh: objWarehouse.Warehouse = self._pick_to_wh(lot=lotObj)
                print(f"\t\t{macObj.__class__.__name__}({macObj.Id}).lot_leave() >> {lotObj}")
                if available_wh is not None:
                    available_wh.lot_arrive(from_loc=macObj, lot=lotObj)

    def lot_arrive(self, lot: objLot.Lot):
        is_assignable, machines = self.get_assignable_flag(lot=lot)
        if not is_assignable:

            print(f"\t\t{self.__class__.__name__}({self.Id}).lot_arrive() >> <{'No Machines Available'}> / {machines}")
            return False
        machine: objMachine.Machine = self._pick_machine(macList=machines)
        machine.assign_lot(lot=lot)
        machine.RunMachine()
        self.reset_first_event_time()
        print(f"\t\t{self.__class__.__name__}({self.Id}/{machine.Id}).lot_arrive() >> {machine}")
        return True

    def are_machines_in_break(self, lot: objLot.Lot):
        breaktime_machines: list = []
        break_end_times: list = []
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            is_breakdown, break_end = macObj.chk_breakdown(lot=lot)
            if is_breakdown:
                breaktime_machines.append(macObj)
                break_end_times.append(break_end)
        break_end_time = min(break_end_times)
        return breaktime_machines, break_end_time

    def _update_lot_list(self):
        self._lotObjList = []
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            if macObj.Lot is not None:
                self._lotObjList.append(macObj)

    def _pick_to_wh(self, lot: objLot):
        whs: list = self._find_available_to_wh_list(lot=lot)
        if len(whs) == 0:
            return None
        return whs[0]

    def _find_available_to_wh_list(self, lot: objLot):
        rsltWhs: list = []
        lotObj: objLot.Lot = lot
        for obj in self._factory.WhouseObjList:
            whObj: objWarehouse.Warehouse = obj
            if self.ToLoc == whObj.Kind:
                is_wh_assignable: bool = whObj.get_assignable_flag(lot=lotObj)
                if is_wh_assignable:
                    rsltWhs.append(whObj)
        return rsltWhs

    def _pick_machine(self, macList: list):
        return macList[0]

    def set_first_event_time(self, runTime: datetime.datetime = None):
        self.FirstEventTime = runTime

    def reset_first_event_time(self):
        mac_end_times: list = [mac.EndTime for mac in self.MacObjList]
        if sum([endTime is None for endTime in mac_end_times]) == len(mac_end_times):
            self.set_first_event_time()
        else:
            mac_end_times = [endTime for endTime in mac_end_times if endTime is not None]
            self.set_first_event_time(min(mac_end_times))

    def set_to_location(self, to_loc: object):
        self.ToLoc = to_loc

    def _get_available_machines(self, lot: objLot.Lot):
        avaliable_machines: list = []
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            if macObj.Status == "IDLE":
                is_breakdown, break_end = macObj.chk_breakdown(lot=lot)
                if not is_breakdown:
                    avaliable_machines.append(macObj)
        return avaliable_machines

    def get_assignable_flag(self, lot: objLot.Lot):
        available_machines: list = self._get_available_machines(lot=lot)
        return len(available_machines) > 0, available_machines
