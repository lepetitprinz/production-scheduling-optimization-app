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

    def setupObject(self):
        pass
        # self.set_to_location(to_loc)

    # def SetupResumeData(self, lotObjList: list):
    #     # self.ProdLotList 에 추가하는 처리.
    #     # TAT모드인 경우, ActEndTime 계산 필요.
    #     # 09/19 self._lotCnt 에 카운트 필요.
    #     lotObjList = self._checkInitErrorLotList(lotObjList=lotObjList)

    def AppendMac(self, tgtMac: objMachine):
        self.MacObjList.append(tgtMac)

    def SetFromLoc(self, from_locs: list):
        self.FromLocs = from_locs

    def SetToLoc(self, to_loc: object):
        self.ToLoc = to_loc

    def SyncRunningTime(self):
        # to_loc: object
        self._updateLotList()
        if len(self._lotObjList) == 0:
            self.resume_down_machines()
        else:
            self.lotLeave()
        self.ResetFstEventTime()
        self.inform_to_previous(runTime=self.FirstEventTime)

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
                from_loc.setFstEventTime(runTime=runTime)
        else:
            if downFlag:
                from_loc.setFstEventTime(runTime=runTime)

    def lotLeave(self):
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            if macObj.EndTime == comUtility.Utility.runtime:
                lotObj: objLot.Lot = macObj.lot_leave(actual_leave_flag=False)

                # Machine에 있는 lot을 다음 warehouse에 할당하는 처리
                assignWh: objWarehouse.Warehouse = self._getAssignWh(lot=lotObj)

                # 할당 가능한 warehouse가 있는 경우 lot을 그 warehouse로 보내는 처리
                if assignWh != None:
                    print(f"\t\t{macObj.__class__.__name__}({macObj.Id}).lot_leave() >> {(lotObj.Id, lotObj.Lpst, lotObj.ReactDuration, lotObj.PackDuration)}")
                    assignWh.lotArrive(from_loc=macObj, lot=lotObj)
                    macObj.lot_leave()
                else:
                    print("할당 가능한 Warehouse가 현재 없음")

    def lotArrive(self, lot: objLot.Lot):
        is_assignable, machines = self.GetAssignableFlag(lot=lot)
        if not is_assignable:
            print(f"\t\t{self.__class__.__name__}({self.Id}).lot_arrive() >> <{'No Machines Available'}> / {machines}")
            return False
        lot.ToLoc = self.ToLoc
        machine: objMachine.Machine = self._assignMac(macList=machines)
        machine.assign_lot(lot=lot)
        machine.RunMachine()
        self.ResetFstEventTime()
        print(f"\t\t{self.__class__.__name__}({self.Id}/{machine.Id}).lot_arrive() >> {machine}")

        return True

    def ChkMacInBreak(self, lot: objLot.Lot):
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

    def _updateLotList(self):
        self._lotObjList = []
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            if macObj.Lot is not None:
                self._lotObjList.append(macObj.Lot)

    # 할당 가능한 Warehouse를 찾고 할당하는 처리
    def _getAssignWh(self, lot: objLot):
        lotWhKind = lot.ToLoc   # silo or FGI
        whObjList = self._getWhObjList(whKind=lotWhKind)

        # Silo Warehouse 할당 로직
        if lotWhKind == 'silo':

            # Lot의 Grade 제품이 들어있는 Silo가 존재하는지 검색
            for silo in whObjList:
                siloObj:objWarehouse.Warehouse = silo
                siloLotObjList = siloObj.LotObjList
                lotObjGradeList = self._getLotObjGrade(lotObjList=siloLotObjList)

                # Silo의 할당 된 lot이 아무것도 없는 경우 통과
                if len(siloLotObjList) == 0:
                    continue
                # Lot의 Grade 제품이 들어있는 Silo가 존재하고 Capa가 충분하면 할당
                elif lot.Grade == lotObjGradeList[0]:
                    if lot.Qty < siloObj.CurCapa:   # silo의 capa가 충분한 경우 할당
                        return siloObj
                    else:   # silo의 Capa가 충분하지 않은 경우 다른 silo 검색
                        continue
                else:   # Lot의 grade와 Silo에 들어있는 Lot의 grade가 다른경우 다른 silo 검색
                    continue

            # lot의 Grade와 같은 silo가 없거나 있어도 capa가 부족한 경우 -> lot이 없는 silo에 할당
            for silo in whObjList:
                siloObj: objWarehouse.Warehouse = silo
                siloLotObjList = siloObj.LotObjList

                # Silo의 할당 된 lot이 아무것도 없는 경우 할당
                if len(siloLotObjList) == 0:
                    return siloObj

            return None

        elif lotWhKind == 'FGI':
            fgiObj:objWarehouse.Warehouse = whObjList[0]    # FGI warehouse
            if lot.Qty < fgiObj.CurCapa:
                return fgiObj
            else:
                return None

    # Warehouse 종류별 list를 찾는 처리
    def _getWhObjList(self, whKind:str):
        whObjList = []
        for wh in self._factory.WhouseObjList:
            whObj:objWarehouse.Warehouse = wh
            if whObj.Kind == whKind:
                whObjList.append(whObj)

        return whObjList

    def _getLotObjGrade(self, lotObjList:list):
        lotObjGradeList = []

        for prodLotObj in lotObjList:
            lotObj:objLot.Lot = prodLotObj

            if lotObj.Grade not in lotObjGradeList:
                lotObjGradeList.append(lotObj.Grade)

        return lotObjGradeList

    def _assignMac(self, macList: list):
        return macList[0]

    def SetFstEventTime(self, runTime: datetime.datetime = None):
        self.FirstEventTime = runTime

    def ResetFstEventTime(self):
        macEndTimes: list = [mac.EndTime for mac in self.MacObjList]
        if sum([endTime is None for endTime in macEndTimes]) == len(macEndTimes):
            self.SetFstEventTime()
        else:
            macEndTimes = [endTime for endTime in macEndTimes if endTime is not None]
            self.SetFstEventTime(min(macEndTimes))

    def _getAvailableMac(self, lot: objLot.Lot):
        availableMacs: list = []
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            if macObj.Status == "IDLE":
                if macObj.hasCalendar:
                    is_breakdown, break_end = macObj.chk_breakdown(lot=lot)
                    if not is_breakdown:
                        availableMacs.append(macObj)
                else:
                    availableMacs.append(macObj)

        return availableMacs

    def GetAssignableFlag(self, lot: objLot.Lot):
        availableMacs: list = self._getAvailableMac(lot=lot)
        return len(availableMacs) > 0, availableMacs

    # def _find_available_to_wh_list(self, lot: objLot):
    #     rsltWhs: list = []
    #     lotObj: objLot.Lot = lot
    #     for obj in self._factory.WhouseObjList:
    #         whObj: objWarehouse.Warehouse = obj
    #         if self.ToLoc == whObj.Kind:
    #             is_wh_assignable: bool = whObj.getAssignableFlag(lot=lotObj)
    #             if is_wh_assignable:
    #                 rsltWhs.append(whObj)
    #     return rsltWhs
