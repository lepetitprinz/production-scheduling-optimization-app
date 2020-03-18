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
        self.Kind: str = kind   # reactor / bagging
        self._factory: simFactoryMgr.Factory = factory

        self.FromLocs: list = []
        self.FromLoc: object = None
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

    def SetFromLocs(self, from_locs: list):
        self.FromLocs = from_locs

    def SetFromLoc(self, from_loc: object):
        self.FromLoc = from_loc

    def SetToLoc(self, to_loc: object):
        self.ToLoc = to_loc

    def SyncRunningTime(self):
        # to_loc: object
        self._updateLotList()
        if len(self._lotObjList) == 0:
            # breaktime_machines, break_end_time = self.ChkMacInBreak()
            self.resume_down_machines()
        else:
            self.lotLeave()
        self.ResetFstEventTime()
        # self.inform_to_previous(runTime=self.FirstEventTime)

    def resume_down_machines(self):
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            macBreakEnd: datetime.datetime = macObj.getMacStopEndTime()
            if macBreakEnd == comUtility.Utility.Runtime:
                macObj.power_on()
                print(f"\t\t{macObj.__class__.__name__} ({macObj.Id}) RESUMED FROM DOWN STATE !")
        self.inform_to_previous()

    def inform_to_previous(self, runTime: datetime.datetime = None):
        if runTime is None:
            runTime = comUtility.Utility.Runtime
        for obj in self._factory.WhouseObjList:
            whObj: objWarehouse.Warehouse = obj
            if self.FromLoc == whObj.Kind:
                if whObj.Kind is "silo":
                    pass
                else:
                    self.inform_to(from_obj=whObj, runTime=runTime)

    def inform_to(self, from_obj: objWarehouse.Warehouse, runTime: datetime.datetime,
                  down_cause: str = "", downFlag: bool = False):
        from_loc: objWarehouse.Warehouse = from_obj
        if down_cause != "":
            from_loc.ShutDownFlag = down_cause == "shutdown"
        if from_loc.FirstEventTime is None:
            if len(from_loc.LotObjList) > 0:
                from_loc.setFstEventTime(runTime=runTime)
        else:
            if downFlag:
                from_loc.setFstEventTime(runTime=runTime, use_flag=True)
            else:
                if runTime < from_loc.FirstEventTime:
                    from_loc.setFstEventTime(runTime=runTime)
                elif runTime > from_loc.FirstEventTime:
                    from_loc.setFstEventTime(runTime=runTime)
                # if runTime <= from_loc.FirstEventTime:
                #     from_loc.setFstEventTime(runTime=runTime)
            # if runTime <= from_loc.FirstEventTime:
            #     from_loc.setFstEventTime(runTime=runTime)

    def lotLeave(self):
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            if macObj.EndTime == comUtility.Utility.Runtime:
                lotObj: objLot.Lot = macObj.lotLeave(actual_leave_flag=False)

                # Machine에 있는 lot을 다음 warehouse에 할당하는 처리
                assignWh: objWarehouse.Warehouse = self._getAssignWh(lot=lotObj)

                # 할당 가능한 warehouse가 있는 경우 lot을 그 warehouse로 보내는 처리
                if assignWh != None:
                    print(f"\t\t{macObj.__class__.__name__}({macObj.Id}).lot_leave() >> {(lotObj.Id, lotObj.Qty, lotObj.ReactDuration, lotObj.PackDuration)}")
                    assignWh.lotArrive(from_loc=macObj, lot=lotObj)
                    if self.Kind == "REACTOR":
                        lotObj.ReactOut = comUtility.Utility.Runtime
                    else:
                        lotObj.BaggingOut = comUtility.Utility.Runtime
                    assignWh.resetFstEventTime(arrival_flag=True)
                    self.inform_to_previous(runTime=macObj.EndTime)
                    macObj.lotLeave()
                else:
                    print("{} Lot 할당 가능한 Warehouse가 현재 없음".format(lotObj.Id))

    def lotArrive(self, lot: objLot.Lot):
        is_assignable, machines, _ = self.GetAssignableFlag(lot=lot)
        if not is_assignable:
            print(f"\t\t{self.__class__.__name__}({self.Id}).lot_arrive() >> <{'No Machines Available'}> / {machines}")
            return False
        lot.ToLoc = self.ToLoc
        machine: objMachine.Machine = self._assignMac(macList=machines)
        machine.assignLotToMac(lot=lot)
        machine.RunMachine()
        self.ResetFstEventTime()
        print(f"\t\t{self.__class__.__name__}({self.Id}/{machine.Id}).lot_arrive() >> {lot.Id, lot.Lpst, lot.ReactDuration, lot.PackDuration}")

        return True

    def ChkMacInBreak(self):
        breaktime_machines: list = []
        break_end_times: list = []
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            # is_breakdown, break_end = macObj.chk_breakdown(lot=lot)
            if macObj.Status is "DOWN":
                breaktime_machines.append(macObj)
                break_end_times.append(break_end_times)
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

    def ResetFstEventTime(self, init_flag: bool = False):
        macEndTimes: list = []
        newFstEventTime: datetime.datetime = None
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            if macObj.EndTime is not None:
                macEndTimes.append(macObj.EndTime)
            elif macObj.hasCalendar:
                if not init_flag:
                    if macObj.getMacStopEndTime() != comUtility.Utility.Runtime:
                        macEndTimes.append(macObj.getMacStopEndTime())
        if len(macEndTimes) > 0:
            newFstEventTime = min(macEndTimes)
        self.SetFstEventTime(newFstEventTime)
        # macEndTimes: list = [
        #     mac.EndTime
        #     for mac in self.MacObjList
        # ]
        # macEndTimes = [endTime if endTime is not None else
        #                self.get_earliest_down_end()
        #                for endTime in macEndTimes]
        # self.SetFstEventTime(min(macEndTimes))
        # if sum([endTime is None for endTime in macEndTimes]) == len(macEndTimes):
        #     self.SetFstEventTime()
        # else:
        #     # next_downtime_end: datetime.datetime = \
        #     #     comUtility.Utility.runtime + datetime.timedelta(days=0)
        #     macEndTimes = [endTime if endTime is not None else
        #                    self._get_earliest_down_end()
        #                    for endTime in macEndTimes]
        #     self.SetFstEventTime(min(macEndTimes))

    def get_earliest_down_end(self):
        down_ends: list = []
        earliest_down_end: datetime.datetime = None
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            if macObj.hasCalendar:
                macDown: tuple = macObj.get_current_downtime()
                macDownEnd: datetime.datetime = macDown[1]
                down_ends.append(macDownEnd)
        if len(down_ends) > 0:
            earliest_down_end = min(down_ends)
        return earliest_down_end

    def _has_down_time(self):
        has_down_time: bool = False
        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            if macObj.hasCalendar:
                has_down_time = True
                return has_down_time
        return has_down_time

    # ==============================================================================================#
    # 시간제약 반영
    # - Machine 비가용 계획 반영 (완료)
    # - Reactor Machine Grade Change Cost(Hour) 반영 (완료)
    # ==============================================================================================#
    def GetAssignableFlag(self, lot: objLot.Lot):
        availableMacList, notAvailableMacList = self._getAvailableMac(lot=lot)
        return len(availableMacList) > 0, availableMacList, notAvailableMacList

    # 공정에서 할당 가능한 machine을 찾는 처리
    def _getAvailableMac(self, lot: objLot.Lot):
        availableMacs: list = []
        notAvailableMacs: list = []

        for obj in self.MacObjList:
            macObj: objMachine.Machine = obj
            ## Reactor와 Bagging 공정을 구분하여 판단
            # Reactor의 경우 단일 machine이므로 machine의 상태만 판단
            if macObj.Oper.Kind == "REACTOR":
                if macObj.Status == "IDLE":     # Machine이 IDLE 상태일때 이용가능
                    if macObj.hasCalendar:      # Machine의 가용계획 check
                        isUnavailable, _, not_available_cause = macObj.chkMacAvailable(lot=lot)
                        if not isUnavailable:
                            availableMacs.append(macObj)
                        else:
                            notAvailableMacs.append((macObj, not_available_cause))
                    else:
                        availableMacs.append(macObj)

            # Bagging 공정의 경우 machine의 상태만 판단
            elif macObj.Oper.Kind == "BAGGING":
                if macObj.Status == "IDLE":     # Machine이 IDLE 상태일때 이용가능
                    if macObj.hasCalendar:      # Machine의 가용계획 check
                        isUnavailable, _, not_available_cause = macObj.chkMacAvailable(lot=lot)
                        if (not isUnavailable) & (macObj.Id == lot.PackSize):
                            availableMacs.append(macObj)
                        else:
                            notAvailableMacs.append((macObj, not_available_cause))
                    else:
                        if macObj.Id == lot.PackSize:
                            availableMacs.append(macObj)

        return availableMacs, notAvailableMacs

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
