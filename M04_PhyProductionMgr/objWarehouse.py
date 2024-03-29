# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
from scop import Model, Alldiff, Quadratic

from M03_Site import simFactoryMgr, simOperMgr
from M05_ProductManager import objLot
from M06_Utility import comUtility


class Warehouse:
    def __init__(self, factory: simFactoryMgr, whId: str, kind: str):
        self._factory: simFactoryMgr = factory
        self._fsVerId = ""
        self.Id: str = whId
        self.Kind: str = kind                            # RM / WareHouse / silo / hopper
        self.LotObjList: list = []
        self.LpstLotDict: dict = {}
        self.Capacity: int = 0          # warehouse 고유 capa
        self.CurCapa: int = 0           # 현재 할당된 재고를 고려한 capa

        self.ToLoc: object = None

        self.FirstEventTime: datetime.datetime = None

        # Time Constraint
        self.GradeChangeFinishConst: bool = False
        self.GradeGroupChangeConst: bool = False
        self.BaggingOperTimeConst: bool = False

        # Lot Sequence Optimization 관련
        self.BeforeLotList: list = []
        self._prodWheelHour: dict = {}
        self._seqOptTimeLimit: int = 1

        # flags
        self._waitFlag: bool = False

        # Constraint Flag
        self.ShutDownFlag: bool = False

        # Production Scheduling 결과 저장
        self.ProdScheduleRsltArr: list = []
        self.BagScheduleRsltArr: list = []

    def setupObject(self, capacity: float = None):
        self._fsVerId = comUtility.Utility.FsVerId

        self._setCapacity(capacity=capacity)

        # Time Constraint Configuration Setting
        self.GradeChangeFinishConst = comUtility.Utility.GradeChangeFinishConst
        self.GradeGroupChangeConst = comUtility.Utility.GradeGroupChangeConst
        self.BaggingOperTimeConst = comUtility.Utility.BaggingOperTimeConst

        self._prodWheelHour = comUtility.Utility.ProdWheelHour

    def setupResumeData(self, lotObjArr: pd.DataFrame):
        for idx, row in lotObjArr.iterrows():
            print(row)
            prodId: str = row['product'][:row['product'].find("_", row['product'].find("_") + 1)]
            lotObj: objLot.Lot = objLot.Lot(id=row['product'], prodId=prodId, loc=self)
            lotObj.setupObject(
                due_date=row['yyyymm'],
                prodCode = row['prodCode'],
                qty = row['qty']
            )
            self._registerLotObj(lotObj=lotObj)
            # lotObj: objLot = obj
            # self._register_lot_obj(lot_obj=lotObj)

    def setToLoc(self, to_loc: object):
        self.ToLoc = to_loc

    def SyncRunningTime(self):
        lotObj: objLot.Lot = self._pickAvailableLot()

        if lotObj != None:
            # 최종 생산완료 된 경우 출하 처리
            if self.ToLoc == "Sales":
                self.Shipping()
                self.resetFstEventTime()

            else:
                # 선택한 Lot에 대해 할당 가능한 Operation - Machine을 찾는 처리
                to_oper, available_machines, not_available_machines = self._findAvailableNextOper(lot=lotObj)
                if len(available_machines) > 0:
                    self.lotLeave(to_loc=to_oper, lot=lotObj)
                    # self.setFstEventTime()
                    if self.Kind is not "RM":
                        self.resetFstEventTime()
                    elif len(self.LotObjList) == 0:
                        self.resetFstEventTime()
                elif len(available_machines) == 0:
                    # is_in_break, break_end_time = to_oper.are_machines_in_break(lot=least_lpst_lot)
                    # if len(is_in_break)
                    print(f"\t\t{to_oper.__class__.__name__}({to_oper.Id}) No Machines Avaiable. Waiting for Processing...\n"
                          f"\t\t{lotObj.Id, lotObj.Qty, lotObj.ReactDuration, lotObj.PackDuration}>> ")
                    to_oper.ResetFstEventTime()
                    if self.Id != "RM":
                        to_oper.inform_to(from_obj=self, runTime=to_oper.FirstEventTime, downFlag=True)
                    else:
                        if len(not_available_machines) == 0:
                            to_oper.inform_to(from_obj=self, runTime=to_oper.FirstEventTime, downFlag=True)
                        else:
                            to_oper.inform_to(from_obj=self, runTime=to_oper.FirstEventTime,
                                              down_cause=not_available_machines[0][-1], downFlag=True)

                    # to_oper.set_first_event_time()
                    # self.set_first_event_time(break_end_time)

    def _shipping(self, lot: objLot):
        # least_lpst_lot: objLot.Lot = self._get_least_lpst_lot()

        # Lot 출하 완료시 DB에 저장할 결과 추가 처리
        self.SetProdScheduleRslt(lot=lot)

        self._removeLot(lot=lot, shipping_flag=True)
        self._updateCurrCapa(lot=lot, in_flag=False)
        # self._rebuild_lpst_lot_dict()
        self.setFstEventTime(use_flag=True)

        print(f"\t\t{self.__class__.__name__}({self.Id}).shipping() >> {lot}")

    def Shipping(self):
        for lot in self.LotObjList:
            self._shipping(lot=lot)

    def lotLeave(self, to_loc: simOperMgr, lot: objLot):
        # least_lpst_lot: objLot.Lot = self._get_least_lpst_lot()
        current_time: datetime.datetime = comUtility.Utility.DayStartDate

        print(f"\t\t{self.__class__.__name__}({self.Id}).lot_leave() >> {(lot.Id, lot.Qty, lot.ReactDuration, lot.PackDuration)}")

        to_loc.lotArrive(lot)
        self._removeLot(lot=lot)
        self._updateCurrCapa(lot=lot, in_flag=False)
        # self._rebuild_lpst_lot_dict()
        self.setFstEventTime(use_flag=True)

        # ======================================================================================= #
        # RM Lot Re-Sequencing
        # RM Warehouse에서 Lot leave 처리가 일어난 경우, 항상 Grade Sequence Change 확인
        # Grade Sequence Change가 발생한 경우, Grade Sequence re-optimization
        # ======================================================================================= #
        if self.Kind == "RM":
            # Grade Sequence Change 확인
            bfLotLeaveGradeSeq = self._getGradeSeqList(self.BeforeLotList)
            afLotLeaveGradeSeq = self._getGradeSeqList(self.LotObjList)

            # Grade Sequence Change가 일어나지 않는 경우
            if (bfLotLeaveGradeSeq == afLotLeaveGradeSeq) or (bfLotLeaveGradeSeq[1:] == afLotLeaveGradeSeq):
                self.setFstEventTime()
            # Grade Sequence Change가 일어난 경우 Grade Sequence re-Optimization
            else:
                # Grade Sequence Optimization using SCOP algorithm
                # Grade Sequence 별로 Lot을 그룹화해서 List 변환

                # Production Cycle: NONE 인 경우 전체 Lot List 최적화
                if comUtility.Utility.ProdCycle == 'NONE':
                    lotSeqOptList = self.SeqOptByScop(self.LotObjList)

                    # re-optimization 한 Lot List를 RM warehouse에 재할당
                    self.LotObjList = lotSeqOptList
                    self.setFstEventTime()

                # Production Cycle: Monthly 인 경우 runtime 기준 월의 Lot List만 최적화
                elif comUtility.Utility.ProdCycle == 'MONTHLY':
                    currMonth = comUtility.Utility.Runtime.strftime('%Y%m')  # runtime의 년+월
                    curMonLotList, exceptLotObjList = self._getSepMonLotList(currMonth=currMonth)
                    currLotSeqOptList = self.SeqOptByScop(curMonLotList)
                    lotSeqOptList = currLotSeqOptList.extend(exceptLotObjList)

                    # re-optimization 한 Lot List를 RM warehouse에 재할당
                    self.LotObjList = lotSeqOptList
                    self.setFstEventTime()
        else:
            # self._rebuild_lpst_lot_dict()
            # self.resetFstEventTime()
            self.setFstEventTime(use_flag=True)

    # ------------------------------------------------------------------------------------------------------ #
    # Grade Sequence Optimization Using SCOP algorithm
    # ------------------------------------------------------------------------------------------------------ #
    def SeqOptByScop(self, lotObjList: list, dueUom:str = "nan"):
        prodWheelCostUom = comUtility.Utility.ProdWheelCalStd
        prodWheel = comUtility.Utility.ProdWheelDf.copy()
        dmdLotGradeList = self._getGradeList(lotList=lotObjList)  # 전달받은 Lot List에 해당하는 Grade list 산출

        # 주어진 Grade List에 해당하는 Production Wheel Cost만 indexing
        dmdLotProdWheel = prodWheel[
            (prodWheel['grade_from'].isin(dmdLotGradeList)) & (prodWheel['grade_to'].isin(dmdLotGradeList))]
        dmdLotProdWheel = dmdLotProdWheel.reset_index(drop=True)  # Indexing 후 index reset

        # Product Wheel Cost Setting
        dmdLotProdWheel = dmdLotProdWheel.loc[:, ['grade_from', 'grade_to', prodWheelCostUom]]

        costList = []
        gradeLen = len(dmdLotGradeList)
        for i in range(gradeLen):
            tempCost = []
            for j in range(gradeLen):
                tempCost.append(dmdLotProdWheel.loc[gradeLen * i + j, prodWheelCostUom])
            costList.append(tempCost)

        # SCOP Modeling
        model = Model()
        varList = model.addVariables(dmdLotGradeList, range(gradeLen))

        cstr = Alldiff("AD", varList, "inf")
        model.addConstraint(cstr)

        obj = Quadratic('obj')
        for i in range(gradeLen):
            for j in range(gradeLen):
                if i != j:  # 동일한 거리는 처리하지 않음
                    for k in range(gradeLen):
                        if k == gradeLen - 1:  # 마지막을 0으로 처리하고 시작을 1부터 시작
                            ell = 0
                        else:
                            ell = k + 1
                        obj.addTerms(costList[i][j], varList[i], k, varList[j], ell)

        model.addConstraint(obj)
        model.Params.TimeLimit = self._seqOptTimeLimit  # 최적화 시간제약
        sol, violated = model.optimize()
        optSeqSol = sorted(sol.items(), key=lambda x: int(x[1]))
        oprtSeqGrade = [x[0] for x in optSeqSol]

        lotSeqOptList = self.GetLotSeqOptList(gradeSeqOpt=oprtSeqGrade, dmdLotList=lotObjList, dueUom=dueUom)

        return lotSeqOptList

    # --------------------------------------------------------- #
    # Grade Sequence Optimization에 Lot List를 Mapping하여 분배
    # : Due Uom - nan/mon  같은 Grade 제품 간의 구분 안함
    # : Due Uom - day 같은 Grade에서 due data 기준으로 순서 고려
    # --------------------------------------------------------- #
    def GetLotSeqOptList(self, gradeSeqOpt:list, dmdLotList:list, dueUom:str):
        lotSeqOptList = []

        if (dueUom == 'nan') or (dueUom == 'mon'):
            # Grade 별로 Lot Grouping (Group 안에서 lot의 순서는 고려하지 않음)
            lotByGradeGroupDict = {}
            for lot in dmdLotList:
                lotObj:objLot.Lot = lot
                if lotObj.Grade not in lotByGradeGroupDict.keys():
                    lotByGradeGroupDict[lotObj.Grade] = [lotObj]
                else:
                    lotByGradeGroupDict[lotObj.Grade].append(lotObj)
                    # tempList = lotByGradeGroupDict[lotObj.Grade]
                    # lotByGradeGroupDict[lotObj.Grade] = tempList

            # 최적화 한 Grade Sequence 별로 lot List 배열
            for grade in gradeSeqOpt:
                LotListByGrade = lotByGradeGroupDict[grade]
                # Packaging Type 최적화
                packTypeSeqOptList = self._getPackSizeSeqOptList(lotObjList=LotListByGrade)
                lotSeqOptList.extend(packTypeSeqOptList)

            # Lot Sequence 순서로 lpst 할당
            lpst = 1
            for lot in lotSeqOptList:
                lotObj:objLot.Lot = lot
                lotObj.Lpst = lpst
                lpst += 1

        # Due Date가 일 단위 일때 처리
        else:
            return None

        return lotSeqOptList

    def _getPackSizeSeqOptList(self, lotObjList:list):
        return lotObjList

    def _getGradeSeqList(self, lotList:list):
        gradeSeqList = []

        for lot in lotList:
            lotObj:objLot.Lot = lot
            if len(gradeSeqList) == 0:
                gradeSeqList.append(lotObj.Grade)
            else:
                if gradeSeqList[-1] != lotObj.Grade:
                    gradeSeqList.append(lotObj.Grade)

        return gradeSeqList

    def _getGradeList(self, lotList:list):
        lotGradeList = []

        for lot in lotList:
            lotObj:objLot.Lot = lot
            if lotObj.Grade not in lotGradeList:
                lotGradeList.append(lotObj.Grade)

        return lotGradeList

    def lotArrive(self, from_loc: object, lot: objLot):
        lotObj: objLot.Lot = lot
        lotObj.ToLoc = self.ToLoc
        print(f"\t\t{self.__class__.__name__}({self.Id}).lot_arrive() "
              f">> {lotObj.Id, lotObj.Lpst, lotObj.ReactDuration, lotObj.PackDuration}")
        self._registerLotObj(lotObj=lotObj)
        self._updateCurrCapa(lot=lotObj, in_flag=True)
        # self._rebuild_lpst_lot_dict()

        # Silo -> Bagging 전송 유예 처리 반영
        FirstEventTime = comUtility.Utility.Runtime
        if self.Kind == "silo":
            if comUtility.Utility.BaggingLeadTimeConst:
                self._waitFlag = True
            FirstEventTime = comUtility.Utility.Runtime + comUtility.Utility.SiloWait
        if self.FirstEventTime is None:
            self.setFstEventTime(FirstEventTime, use_flag=True)
        elif FirstEventTime <= self.FirstEventTime:
            self.setFstEventTime(FirstEventTime, use_flag=True)

    def getAssignableFlag(self, lot: objLot):
        isAssignable = False
        lotObj: objLot.Lot = lot
        if self.CurCapa >= lotObj.Qty:
            isAssignable = True
        return isAssignable

    def resetCurCapa(self):
        self.CurCapa = int(self.Capacity)

    def _pickAvailableLot(self):
        prodCycle = comUtility.Utility.ProdCycle
        if prodCycle == 'NONE':
            firstLotObj = self._pickAvailableLotNone()
        elif prodCycle == 'MONTHLY':
            firstLotObj = self._pickAvailableLotMonth()

        return firstLotObj

    # Shut Down 후 특정 Grade 제품을 먼저 생산하기 위해 Lot List 순서를 reorder 하는 처리
    def _getSdReorderedLotList(self, grade:str):
        whLotList = self.LotObjList.copy()
        GradeLotList = []
        exceptGradeLotList = []
        reorderedLotList = []

        for lot in whLotList:
            lotObj:objLot.Lot = lot
            if lotObj.Grade == grade:
                GradeLotList.append(lotObj)
            else:
                exceptGradeLotList.append(lotObj)

        # Grade Lot - 나머지 Lot 순서로 재배열
        reorderedLotList.extend(GradeLotList)
        reorderedLotList.extend(exceptGradeLotList)

        return reorderedLotList

    # Warehouse에 있는 Lot List에서 필요한 lot을 가져오는 처리
    def _pickAvailableLotNone(self):

        # Shut Down Constraint 반영 여부 체크
        if self.ShutDownFlag == True:
            afterShutdownGrade = comUtility.Utility.AfterSdGrade    # Shutdown 후 지정한 Grade

            # Shut Down 후 특정 Grade 우선 생산 조건 반영을 위한 setting
            sdReorderedLotList = self._getSdReorderedLotList(grade=afterShutdownGrade)  # Lot List Reordering
            self.LotObjList = sdReorderedLotList    # Warehouse의 Lot List Update
            self.ShutDownFlag = False               # Shutdown Flag 초기화
            reorderedLotObj: objLot.Lot = self.LotObjList[0]

            return reorderedLotObj

        else:
            firstLotObj: objLot.Lot = self.LotObjList[0]
            return firstLotObj

    # Warehouse에 있는 Lot List에서 필요한 lot을 가져오는 처리
    def _pickAvailableLotMonth(self):
        if self.Kind == 'RM':
            currMonth = comUtility.Utility.Runtime.strftime('%Y%m')     # runtime의 년+월
            curMonLotList, exceptLotObjList = self._getSepMonLotList(currMonth=currMonth)

            # Runtime 월의 lot이 모두 처리된 경우 다음 월 1일이 올떄까지 pick 하는 처리 중지
            if len(curMonLotList) == 0:
                nextMonth = comUtility.Utility.Runtime + relativedelta(months=+1)
                nextMonthStr = nextMonth.strftime('%Y%m')
                nextMonthFstDayStr = nextMonthStr + '01000000'
                nextMonthFstDay = datetime.datetime.strptime(nextMonthFstDayStr, '%Y%m%d%H%M%S')
                self.FirstEventTime = nextMonthFstDay   # RM의 Event Time을 runtime의 다음 달 1일로 초기화
                return None

            # runtime의 월에 lot이 존재하므로 당월 duedate인 lot을 선택
            else:
                # Shut Down Constraint 반영 여부 체크
                if self.ShutDownFlag == True:
                    afterShutdownGrade = comUtility.Utility.AfterSdGrade  # Shutdown 후 지정한 Grade

                    # Shut Down 후 특정 Grade 우선 생산 조건 반영을 위한 setting
                    sdReorderedLotList = self._getSdReorderedLotListMonth(grade=afterShutdownGrade,
                                                                          currLotList=curMonLotList,
                                                                          exceptLotList=exceptLotObjList)  # Lot List Reordering
                    self.LotObjList = sdReorderedLotList  # Warehouse의 Lot List Update
                    self.ShutDownFlag = False  # Shutdown Flag 초기화
                    reorderedLotObj: objLot.Lot = self.LotObjList[0]

                    return reorderedLotObj

                else:
                    for lot in self.LotObjList:
                        lotObj:objLot.Lot = lot
                        lotCurrMonth = lotObj.DueDate.strftime('%Y%m')
                        if lotCurrMonth == currMonth:
                            return lotObj

        else:
            firstLotObj: objLot.Lot = self.LotObjList[0]
            return firstLotObj

    def _getSepMonLotList(self, currMonth: str):
        currLotObjList = []
        exceptLotObjList = []
        for lot in self.LotObjList:
            lotObj: objLot.Lot = lot
            lotProdMonth = lotObj.DueDate.strftime('%Y%m')
            if lotProdMonth == currMonth:
                currLotObjList.append(lotObj)
            else:
                exceptLotObjList.append(lotObj)

        return currLotObjList, exceptLotObjList

    # Shut Down 후 특정 Grade 제품을 먼저 생산하기 위해 Lot List 순서를 reorder 하는 처리
    def _getSdReorderedLotListMonth(self, grade:str, currLotList:list, exceptLotList:list):
        GradeLotList = []
        exceptGradeLotList = []
        reorderedLotList = []

        for lot in currLotList:
            lotObj:objLot.Lot = lot
            if lotObj.Grade == grade:
                GradeLotList.append(lotObj)
            else:
                exceptGradeLotList.append(lotObj)

        # Grade Lot - 나머지 Lot 순서로 재배열
        reorderedLotList.extend(GradeLotList)
        reorderedLotList.extend(exceptGradeLotList)

        #
        reorderedLotList.extend(exceptLotList)

        return reorderedLotList

    def _updateCurrCapa(self, lot: objLot, in_flag: bool):
        rslt: float = 0.0
        lotObj: objLot.Lot = lot
        if in_flag:
            self.CurCapa -= lotObj.Qty
        else:
            self.CurCapa += lotObj.Qty

    def _findAvailableNextOper(self, lot: objLot):
        # rsltOper: simOperMgr.Operation = None
        # rsltMachines: list = []

        targetOperList: list = \
            [oper for oper in self._factory.OperList if oper.Kind == self.ToLoc]
        targetOper: simOperMgr.Operation = targetOperList[0]    # targetOper : reactor / bagging

        # Target으로 하는 공정에 대해애서 가능한 machine들을 찾는 처리
        is_oper_assignable, available_machines, not_available_machines = targetOper.GetAssignableFlag(lot=lot)

        return targetOper, available_machines, not_available_machines

        # if self.Id == "RM":
        #     for obj in self._factory.OperList:
        #         operObj: simOperMgr.Operation = obj
        #         is_oper_assignable, available_machines = operObj.get_assignable_flag()
        #         if operObj.Id == "REACTOR" and is_oper_assignable:
        #             rsltOper = operObj
        #             rsltMachines = available_machines

    def RemoveLotList(self):
        self.LotObjList.clear()
        self._factory._lot_obj_list.clear()

    def _removeLot(self, lot: objLot, shipping_flag: bool = False):
        try:
            self.BeforeLotList = self.LotObjList.copy()
            self.LotObjList.remove(lot)
            if shipping_flag:
                self._factory._removeLot(lot=lot)
        except ValueError as e:
            raise e

    def _getMinLpstLot(self):
        # self.assign_random_lpst()
        self._rebuildLpstLotDict()
        Warning(f"Fix Me !! from {self.__class__}._get_least_lpst_lot !!")
        least_lpst_lot: objLot.Lot = self.LpstLotDict[min(self.LpstLotDict.keys())][0]
        return least_lpst_lot

    def _rebuildLpstLotDict(self):
        self.LpstLotDict = dict()
        for obj in self.LotObjList:
            lotObj: objLot.Lot = obj
            lpst: int = lotObj.Lpst
            if lpst not in self.LpstLotDict.keys():
                self.LpstLotDict[lpst] = [lotObj]
            else:
                raise KeyError(
                    f"WareHouse {self.Id} 에 Lpst 값 (Lpst={lpst}) 이 중복되는 Lot 이 있습니다 ! "
                    f">> {self.LpstLotDict[lpst][0].Lpst} / {lotObj.Lpst}"
                )
                # self.LpstLotDict[lpst].append(lotObj)

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
        lotObj: objLot.Lot = lotObj
        lotObj.SetLocation(self, self.Id)
        self.LotObjList.append(lotObj)
        self._factory._register_lot_to(lot_obj=lotObj, to="self")

    def resetFstEventTime(self, arrival_flag: bool = False):
        lot_arrived_times: list = []
        if len(self.LotObjList) == 0:
            self.setFstEventTime(runTime=None, use_flag=True)
        else:
            if not self._waitFlag:
                self.setFstEventTime(comUtility.Utility.Runtime, use_flag=True)
                # if arrival_flag:
                #     # Lot 이 들어온 경우
                #
                # else:
                #     # Lot 을 내보내는 경우
                #     for obj in self.LotObjList:
                #         lotObj: objLot.Lot = obj
                #         delayed_time: datetime.datetime = None
                #         if self.Kind is "FGI":
                #             delayed_time = \
                #                 lotObj.BaggingOutf if arrival_flag else lotObj
                #             lot_arrived_times.append(delayed_time)
                #
                #         lot_arrived_times.append(delayed_time)
            else:
                if self.Kind is "silo":
                    for obj in self.LotObjList:
                        lotObj: objLot.Lot = obj
                        delayed_time: datetime.datetime = lotObj.ReactOut + comUtility.Utility.SiloWait
                        lot_arrived_times.append(delayed_time)

                silo_delayed_time_min: datetime.datetime = min(lot_arrived_times)
                self.setFstEventTime(runTime=silo_delayed_time_min, use_flag=True)


    def setFstEventTime(self, runTime: datetime.datetime = None, use_flag: bool = False):
        if not use_flag:
            runTime = comUtility.Utility.Runtime
            self.FirstEventTime = runTime
        else:
            if self.FirstEventTime is not None:
                if runTime is not None:
                    self.FirstEventTime = runTime
                    # if runTime <= self.FirstEventTime:
                    #     self.FirstEventTime = runTime
                else:
                    if not self._waitFlag:
                        self.FirstEventTime = runTime
                    else:
                        self.FirstEventTime = runTime
            else:
                self.FirstEventTime = runTime

    def SetProdScheduleRslt(self, lot):
        lotOjb:objLot.Lot = lot

        # Reactor 공정 추가
        reactInStr = lotOjb.ReactIn.strftime("%Y-%m-%d %H:%M:%S")
        reactOutStr = lotOjb.ReactOut.strftime("%Y-%m-%d %H:%M:%S")
        reactorProdCode = comUtility.Utility.ProdMstDict[lotOjb.Grade]
        reactorScheduleRslt = [
                    comUtility.Utility.FsVerId,  # FS_VRSN_ID
                    'REACTOR',              # PLANT_NAME
                    'M1',                   # LINE_NAME
                    'PRODUCTION',            # PLAN TYPE
                    'Act['+lotOjb.Grade+']',# PLAN_CODE
                    '',                     # SALE_MAN
                    reactorProdCode,        # PRODUCT
                    '',                     # CUSTOMER
                    lotOjb.Id,              # LOT_NO
                    '',                     # DATE_FROM
                    '',                     # DATE_TO
                    reactInStr,             # DATE_FROM_TEXT
                    reactOutStr,            # DATE_TO_TEXT
                    '',                     # COLOR
                    lotOjb.ReactDuration.seconds,   # DURATION
                    lotOjb.Qty              # QTY
                    ]

        # Bagging 공정 추가
        baggingInStr = lotOjb.BaggingIn.strftime("%Y-%m-%d %H:%M:%S")
        baggingOutStr = lotOjb.BaggingOut.strftime("%Y-%m-%d %H:%M:%S")
        baggingScheduleRslt = [
                    comUtility.Utility.FsVerId, # FS_VRSN_ID
                    'BAGGING',              # PLANT_NAME
                    lotOjb.PackSize,        # LINE_NAME
                    'PRODUCTION',           # PLAN TYPE
                    'Act['+lotOjb.Id+']',   # PLAN_CODE
                    '',                     # SALE_MAN
                    lotOjb.ProdCode,        # PRODUCT
                    '',                     # CUSTOMER
                    lotOjb.Id,              # LOT_NO
                    '',                     # DATE_FROM
                    '',                     # DATE_TO
                    baggingInStr,           # DATE_FROM_TEXT
                    baggingOutStr,          # DATE_TO_TEXT
                    '',                     # COLOR
                    lotOjb.PackDuration.seconds,    # DURATION
                    lotOjb.Qty              # QTY
                    ]

        self.ProdScheduleRsltArr.append(reactorScheduleRslt)
        self.ProdScheduleRsltArr.append(baggingScheduleRslt)
        self.BagScheduleRsltArr.append(baggingScheduleRslt)


