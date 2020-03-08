# -*- coding: utf-8 -*-
import pandas as pd
import datetime

from M01_Simulator import PE_Simulator
from M02_DataManager import dbDataMgr
from M03_Site import simOperMgr
from M04_PhyProductionMgr import objWarehouse, objMachine
from M05_ProductManager import objLot
from M06_Utility import comUtility

class Factory:
    def __init__(self, simul: PE_Simulator, facID: str):
        # --- Factory Standard 관련 속성 ---
        self._simul: PE_Simulator = simul
        self._utility = comUtility.Utility  # Common M06_Utility

        self._defWorkYn: str = ""  # Factory Calendar가 없을 시 디폴드로 Working 인지 여부 Y/N
        self._dayStartTime: str = ""  # Factory 디폴드 시작시간

        # --- M03_Site 관련 속성 ---
        self.ID: str = facID  # SiteID

        # --- Time 관련 객체 ---
        self._startTime: datetime.datetime = None  # 시스템 구동 시작 시간
        self._operTime: datetime.datetime = None
        self._whTime: datetime.datetime = None

        # --- Resource 관련 객체 리스트 ---
        self.OperList: list = []  # OperationMgr 리스트
        self.MachineList: list = []  # Machine 객체 리스트
        self.WhouseObjList: list = []  # objWarehouse 객체 리스트
        # self._initWipLotList: list = []  # Lot 초기상태 리스트
        # self._initInvLotList: list = []  # WH에 있던 Lot 초기상태 정보
        self._lot_obj_list: list = []

        # --- DB 연결 결과 취득 ---
        self._dataMgr: dbDataMgr.DataManager = simul.DataMgr  # DB에서 기준정보를 가지고 있는 객체

    def SetupObject(self, dayStartTime: str, year: int, month: int, day: int, horizon_days: int, silo_qty: int, nof_silo: int = 1):
        self._utility.setDayStartTime(value=dayStartTime)
        self._utility.setDayStartDate(year=year, month=month, day=day)
        self._utility.setDayHorizon(days=horizon_days)
        self._utility.calcDayEndDate()

        self._startTime = self._utility.DayStartDate

        self._buildFactory(silo_qty=silo_qty, nof_silo=nof_silo)

        self._base_first_event_time()

        self._prodWheelDict = self._setProdWheelDict()

        # self._register_new_machine(mac_id="MAC01")
        # self.StockList = self._facUtil.GetStockObjList()
        # self._register_new_warehouse(wh_id="RM")
        # self._register_new_warehouse(wh_id="WH01")
        # self.OperMgrList = self._facUtil.GetOperMgrObjList()

    def SetupResumeData(self):
        # Warehouse 의 Lot을 배정하는 처리.
        wh_rm: objWarehouse.Warehouse = self._findWhById(wh_id="RM")
        df_demand = self._dataMgr.df_demand
        dfDmdLotSizing = self._setDmdProdLotSizing(df_demand)
        wh_rm.setup_resume_data(dfDmdLotSizing)

    def _setDmdProdLotSizing(self, demand: pd.DataFrame):

        minLotSize = comUtility.Utility.MinLotSize
        maxLotSize = comUtility.Utility.MaxLotSize

        dmdProdLot = pd.DataFrame(columns=['yyyymm', 'product', 'lotId', 'qty', 'region'])

        idx = 0
        for _, row in demand.iterrows():
            # Lot Sizing
            if row['qty'] < minLotSize:
                dmdProdLot.loc[idx, 'qty'] = minLotSize
                dmdProdLot.loc[idx, 'yyyymm'] = row['yyyymm']
                dmdProdLot.loc[idx, 'product'] = row['product']
                dmdProdLot.loc[idx, 'region'] = row['region']
                idx += 1
            elif row['qty'] > minLotSize and row['qty'] < maxLotSize:
                dmdProdLot.loc[idx, 'qty'] = row['qty']
                dmdProdLot.loc[idx, 'yyyymm'] = row['yyyymm']
                dmdProdLot.loc[idx, 'product'] = row['product']
                dmdProdLot.loc[idx, 'region'] = row['region']
                idx += 1
            else:
                quotient = row['qty'] // maxLotSize
                remainder = row['qty'] % maxLotSize
                # Maximum Lot 단위 처리
                for i in range(int(quotient)):
                    dmdProdLot.loc[idx, 'qty'] = maxLotSize
                    dmdProdLot.loc[idx, 'yyyymm'] = row['yyyymm']
                    dmdProdLot.loc[idx, 'product'] = row['product'] + '_' + str(i+1)
                    dmdProdLot.loc[idx, 'region'] = row['region']
                    idx += 1
                # 나머지 lot 추가 처리
                dmdProdLot.loc[idx, 'qty'] = remainder
                dmdProdLot.loc[idx, 'yyyymm'] = row['yyyymm']
                dmdProdLot.loc[idx, 'product'] = row['product'] + '_' + str(quotient+1)
                dmdProdLot.loc[idx, 'region'] = row['region']
                idx += 1

        return dmdProdLot

        # facID = self.ID
        # totalWipList = []
        # operLotDict: dict = {}  # {OperID : [LotObj1, LotObj2, ...]}
        # for obj in self.WhouseObjList:
        #     whObj: objWarehouse.Warehouse = obj
        #     self._dataMgr
        #     whObj.setup_resume_data(lot_obj_array=)

        # # Lot 셋팅: Machine
        # for obj in self.MachineList:
        #     macObj: objMachine = obj
        #     if macObj.ID in operLotDict.keys():
        #         macObj.SetupResumeData(operLotDict[macObj.ID])
        #     # Machine SetupType 셋팅 오류 수정
        #     oper.FixMachineSetupTypeError()

    def _buildFactory(self, silo_qty: float, nof_silo: int = 1):
        reactor: simOperMgr.Operation = self._register_new_oper(oper_id="REACTOR", kind="REACTOR", return_flag=True)
        self._register_new_machine(mac_id="M1", oper=reactor)

        bagging: simOperMgr.Operation = self._register_new_oper(oper_id="BAGGING", kind="BAGGING", return_flag=True)
        self._register_new_machine(mac_id="P2", oper=bagging, uom="25 KG")
        self._register_new_machine(mac_id="P7", oper=bagging, uom="750 KG")
        self._register_new_machine(mac_id="P9", oper=bagging, uom="BULK")

        # self.StockList = self._facUtil.GetStockObjList()
        rm: objWarehouse.Warehouse = self._register_new_warehouse(wh_id="RM", kind="RM", return_flag=True)     # RM / WareHouse / silo / hopper
        rm.assign_random_lpst()
        silo_qty /= nof_silo
        silos: list = []
        for i in range(1, 1 + nof_silo):
            silo: objWarehouse.Warehouse = self._register_new_warehouse(wh_id=f"SILO{'%02d' % i}", kind="silo", capacity=silo_qty, return_flag=True)
            silos.append(silo)
        # hopper: objWarehouse.Warehouse = self._register_new_warehouse(wh_id="HOPPER", kind="hopper", return_flag=True)
        fgi: objWarehouse.Warehouse = self._register_new_warehouse(wh_id="FGI", kind="WareHouse", capacity=43000, return_flag=True)

        rm.set_to_location(to_loc=reactor.Id)
        reactor.set_to_location(to_loc=silos[0].Kind)
        for silo in silos:
            silo.set_to_location(to_loc=bagging.Id)
        # hopper.set_to_location(to_loc=fgi.Id)
        bagging.set_to_location(to_loc=fgi.Id)
        fgi.set_to_location(to_loc="Sales")     # Ternminal Status

    def _base_first_event_time(self):
        for obj in self.OperList:
            operObj: simOperMgr.Operation = obj
            operObj.reset_first_event_time()
        for obj in self.WhouseObjList:
            whObj: objWarehouse.Warehouse = obj
            if whObj.Id == "RM":
                whObj.set_first_event_time(runTime=self._utility.DayStartDate)
            else:
                whObj.set_first_event_time()

    def sendInitEvent(self):
        """공장 객체 초기화 정보를 DB에 전달하는 메서드"""
        pass

    def run_factory(self):
        print(f"Factory {self.ID} is Running.")

        endFlag: bool = False
        end_date: datetime.datetime = self._utility.DayEndDate
        loopCnt: int = 0
        while not endFlag:
            # OperTAT, Machine, Transporter, Warehouse 에서 이벤트 처리 대상을 찾기
            runTime: datetime.datetime = self.Get1stTgtMinTime()
            tgtArr: list = self.Get1stTgtArray(runTime=runTime)

            if len(tgtArr) < 1:
                # self._utility.LotStatusObj.PrintWaitLotList()
                endFlag = True
                continue
            elif runTime >= end_date:
                endFlag = True
                self._utility.set_runtime(runtime=end_date)
                continue

            # if len(tgtArr) < 1:
            #     endFlag = len(util.LotStatusObj.WaitLotIdDict.keys()) < 1
            #     if endRTime < 1:
            #         endRTime = runTime
            #     else:
            #         loopCnt += 1
            #     continue
            # else:
            #     endFlag, prevRunTime, prevLotEvtCnt, prevMacEvtCnt, loopCnt = self._chkInfiniteLoop(
            #         prevTime=prevRunTime,
            #         prevLotEvt=prevLotEvtCnt,
            #         prevMacEvt=prevMacEvtCnt,
            #         runTIme=runTime,
            #         loopCnt=loopCnt
            #     )

            endRTime = 0

            if runTime != self._utility.runtime:
                self._utility.set_runtime(runTime)

            tgtCnt = len(tgtArr)
            for obj in tgtArr:
                obj.SyncRunningTime()
                # # [[Time], [FacID], [TgtID], [TgtType], [PrevTime], [FlowID]]
                # if row[3] == self._TGT_TRANS:  # "TRANS"
                #     self.TransObj.SyncRunningTime(runTime=runTime, tgtCnt=tgtCnt)
                # elif row[3] == self._TGT_OPER:  # "OPER_TAT"
                #     operObj: simOperMgr.OperationManager = self._geTatOperObj(operID=row[2])
                #     operObj.SyncRunningTime(runTime=runTime)
                # elif row[3] == self._TGT_WH:
                #     whObj: objWarehouse.Warehouse = self.GetWhouseObj(whID=row[2])
                #     whObj.SyncRunningTime(runTime=runTime)

        # prevRunTime = -1  # 전 회차 Time
        # prevLotEvtCnt = -1  # 전 회차 Lot이벤트 누적 횟수.
        # prevMacEvtCnt = -1  # 전 회차 Mac이벤트 누적 횟수.
        # endFlag = False  # 처리 종료 여부
        # horizon = self._utility.EngPlanHorizonLength
        # loopCnt = 0
        # endRTime = 0
        # util = comUtility.Utility
        # prevTime = 0
        #
        # while endFlag == False:
        #     # OperTAT, Machine, Transporter, Warehouse 에서 이벤트 처리 대상을 찾기
        #     runTime = self.Get1stTgtMinTime()
        #     tgtArr: list = self.Get1stTgtArray(runTime=runTime)
        #
        #     if len(tgtArr) < 1:
        #         # self._utility.LotStatusObj.PrintWaitLotList()
        #         endFlag = True
        #         continue
        #     elif runTime >= horizon:
        #         endFlag = True
        #         self._utility.SetRunningTime(iTime=horizon)
        #         continue
        #
        #     if len(tgtArr) < 1:
        #         endFlag = len(util.LotStatusObj.WaitLotIdDict.keys()) < 1
        #         if endRTime < 1:
        #             endRTime = runTime
        #         else:
        #             loopCnt += 1
        #         continue
        #     else:
        #         endFlag, prevRunTime, prevLotEvtCnt, prevMacEvtCnt, loopCnt = self._chkInfiniteLoop(
        #             prevTime=prevRunTime,
        #             prevLotEvt=prevLotEvtCnt,
        #             prevMacEvt=prevMacEvtCnt,
        #             runTIme=runTime,
        #             loopCnt=loopCnt
        #         )
        #         # loopCnt = 0
        #
        #     endRTime = 0
        #
        #     if runTime != self._utility.RunningTime:
        #         # if runTime < self._utility.RunningTime:
        #         #     print("Runtime: {},\t Target: {}".format(str(runTime), tgtArr[0]))
        #
        #         self._utility.SetRunningTime(runTime)
        #         if devFlag == True:
        #             print("Runtime: {},\t Target: {}".format(str(runTime), tgtArr))
        #         else:
        #             if runTime - prevTime > 9999:
        #                 print(runTime)
        #                 prevTime = round(runTime, -4)
        #
        #     tgtCnt = len(tgtArr)
        #     for row in tgtArr:
        #         # [[Time], [FacID], [TgtID], [TgtType], [PrevTime], [FlowID]]
        #         if row[3] == self._TGT_TRANS:  # "TRANS"
        #             self.TransObj.SyncRunningTime(runTime=runTime, tgtCnt=tgtCnt)
        #         elif row[3] == self._TGT_OPER:  # "OPER_TAT"
        #             operObj: simOperMgr.OperationManager = self._geTatOperObj(operID=row[2])
        #             operObj.SyncRunningTime(runTime=runTime)
        #         elif row[3] == self._TGT_WH:
        #             whObj: objWarehouse.Warehouse = self.GetWhouseObj(whID=row[2])
        #             whObj.SyncRunningTime(runTime=runTime)
        #
        #     # 무한루프 제어
        #     endFlag, prevRunTime, prevLotEvtCnt, prevMacEvtCnt, loopCnt = self._chkInfiniteLoop(
        #         prevTime=prevRunTime,
        #         prevLotEvt=prevLotEvtCnt,
        #         prevMacEvt=prevMacEvtCnt,
        #         runTIme=runTime,
        #         loopCnt=loopCnt
        #     )
        #
        # print("Lot events: {}, Machine events: {}".format(comUtility.Utility.LotStatusObj.HistoryCount,
        #                                                   self._facUtil.MacStatusObj.HistoryCount))

    def Get1stTgtArray(self, runTime: datetime.datetime):
        rslt = []
        if runTime is None:
            return rslt

        if self._operTime == runTime:
            # rslt에 OperTAT 처리대상 정보 추가
            rslt = self._get1stOperTgtInfo(tgtList=rslt, eventTime=runTime)
            self._operTime = None
        if self._whTime == runTime:
            # rslt에 Warehouse 처리대상 정보 추가
            rslt = self._get1stWhouseTgtInfo(tgtList=rslt, eventTime=runTime)
            self._whTime = None
        return rslt

    def _get1stOperTgtInfo(self, tgtList: list, eventTime: datetime.datetime):

        for obj in self.OperList:
            operObj: simOperMgr.Operation = obj
            if operObj.FirstEventTime == eventTime:
                tgtList.append(operObj)

        return tgtList

    def _get1stWhouseTgtInfo(self, tgtList: list, eventTime: datetime.datetime):

        for obj in self.WhouseObjList:
            whObj: objWarehouse.Warehouse = obj
            if whObj.FirstEventTime == eventTime:
                tgtList.append(whObj)

        return tgtList

    def Get1stTgtMinTime(self):
        self._operTime = self._getOperFirstTime()
        self._whTime = self._getWhouseFirstTime()

        tmpList: list = []
        if self._operTime is not None:
            tmpList.append(self._operTime)
        if self._whTime is not None:
            tmpList.append(self._whTime)

        if len(tmpList) < 1:
            return None

        return min(tmpList)

    def _getOperFirstTime(self):
        operFirstEventTimes: list = [oper.FirstEventTime for oper in self.OperList]
        if sum([ft is not None for ft in operFirstEventTimes]) == 0:
            return None
        else:
            operFirstEventTimes = [ft for ft in operFirstEventTimes if ft is not None]
            return min(operFirstEventTimes)

    def _getWhouseFirstTime(self):
        whFirstEventTimes: list = [wh.FirstEventTime for wh in self.WhouseObjList]
        if sum([ft is not None for ft in whFirstEventTimes]) == 0:
            return None
        else:
            whFirstEventTimes = [ft for ft in whFirstEventTimes if ft is not None]
            return min(whFirstEventTimes)

    def wake_up_machine(self):
        # Lot이 버퍼에 장착 된 채 IDLE인 머신을 깨우는 처리
        for obj in self.MachineList:
            macObj: objMachine = obj
            macObj.RunMachine()

    def AssignLot(self):
        rslt = False

        macIdleList = self.GetAssignAbleMacObjList()
        if len(macIdleList) > 0:
            rslt = self._assignLotByMac(macIdleList)

        return rslt

    def GetAssignAbleMacObjList(self):
        mac_list = []

        for obj in self.MachineList:
            macObj: objMachine.Machine = obj

            if macObj.Status is not "IDLE":
                mac_list.append(macObj)

            # ResStatus = "IDLE" / ReserveFlag = Lot 가져오기 예약 여부
            # if mac.ResStatus == mac.GetStatus("idle") and mac.ReserveFlag == False :
            # if mac.ChkAssignAbleFlag() == True:
            #     # Buffer에 Lot이 존재하고 / PosIdx = 3 (Machine)
            #     if mac.InputBuffer.Count() > 0 and mac.InputBuffer.LotObjBuffer[0].Position == self._POS_MACHINE:
            #         # 머신 깨우기
            #         mac.WakeUpMacDown(comUtility.Utility.RunningTime)
            #     else :
            #         if mac.MacTypeObj.MacTypeID == self._MAC_TYPE_TABLE:
            #             macArr.append(mac)
            #         elif mac.MacTypeObj.MacTypeID == self._MAC_TYPE_INLINE:
            #             # macArr.append(mac)
            #             pass

        return mac_list

    # ================================================================================= #
    # Lot Sizing
    # ================================================================================= #

    def _setDmdProdLotSizing(self, demand: pd.DataFrame):

        minLotSize = comUtility.Utility.MinLotSize
        maxLotSize = comUtility.Utility.MaxLotSize

        dmdProdLot = pd.DataFrame(columns=['yyyymm', 'product', 'lotId', 'qty', 'region'])

        idx = 0
        for _, row in demand.iterrows():
            # Lot Sizing
            if row['qty'] < minLotSize:
                dmdProdLot.loc[idx, 'qty'] = minLotSize
                dmdProdLot.loc[idx, 'yyyymm'] = row['yyyymm']
                dmdProdLot.loc[idx, 'product'] = row['product']
                dmdProdLot.loc[idx, 'region'] = row['region']
                idx += 1
            elif row['qty'] > minLotSize and row['qty'] < maxLotSize:
                dmdProdLot.loc[idx, 'qty'] = row['qty']
                dmdProdLot.loc[idx, 'yyyymm'] = row['yyyymm']
                dmdProdLot.loc[idx, 'product'] = row['product']
                dmdProdLot.loc[idx, 'region'] = row['region']
                idx += 1
            else:
                quotient = int(row['qty'] // maxLotSize)
                remainder = row['qty'] % maxLotSize
                # Maximum Lot 단위 처리
                for i in range(quotient):
                    dmdProdLot.loc[idx, 'qty'] = maxLotSize
                    dmdProdLot.loc[idx, 'yyyymm'] = row['yyyymm']
                    dmdProdLot.loc[idx, 'product'] = row['product'] + '_' + str(i+1)
                    dmdProdLot.loc[idx, 'region'] = row['region']
                    idx += 1
                # 나머지 lot 추가 처리
                dmdProdLot.loc[idx, 'qty'] = remainder
                dmdProdLot.loc[idx, 'yyyymm'] = row['yyyymm']
                dmdProdLot.loc[idx, 'product'] = row['product'] + '_' + str(quotient+1)
                dmdProdLot.loc[idx, 'region'] = row['region']
                idx += 1

        return dmdProdLot

    # ================================================================================= #
    # Lot Sequencing Optimization
    # ================================================================================= #
    # Logic
    # 1.RM Warehouse 에서 LotObjList 받아오기
    # 2.Production Cycle 기준으로 Lot Sequencing 최적화(dueDataUom 기준)
    #   - nan: 고정생산주기가 없는 경우, Grade Sequencing (Production Wheel만 고려해서 산)
    #   - mon: 월 단위로
    #          우선순위
    #           - 월 생산 Capa > 월 수요 총 Capa : Production Wheel 고려)
    #           - 월 생산 Capa < 월 수요 총 Capa : 납기일 고려)
    #   - day: 납기일을 최우선 기준으로하여 lot Sequencing (우선순위: 납기일 > prodcution wheel cost)
    # 3.
    # ================================================================================= #

    def geOptLotSeqList(self, dmdLotObjList:list):
        '''
        - dmdProdLotList: 월 단위의 Demand Product Lot List
                           ex) [prodLotObj1, prodLotObj2, prodLotObj3, prodLotObj4, ...]
        - dueDateUom (mon / week / day)
            1) nan : 생산주기가 없는 경우 Grade 별로 Sequencing 후 각 Grade 별로 Lot을 임의 할당
            2) mon : 월 단위의 납기 고려하여 Seqeuncing을 진행
            3) day : 일 단위의 납기 기준의 경우 납기 기준으로

        '''

        dueDateUom = comUtility.Utility.DueDateUom  # 납기 기준 단위: 없음 / 월 / 일

        if dueDateUom == 'nan':
            optLotSeqList = self._optLotSeqNan(dmdLotObjList=dmdLotObjList)

        elif dueDateUom == 'mon':
            optLotSeqList = self._optLotSeqMon(dmdLotObjList=dmdLotObjList)

        else:
            optLotSeqList = self._optLotSeqDay(dmdLotObjList=dmdLotObjList)

        return optLotSeqList

    def _optLotSeqNan(self, dmdLotObjList:list):
        facStartDate = comUtility.Utility.DayStartDate  # 공장 시작시간
        prodWheelDict = self._prodWheelDict



    def _optLotSeqMon(self, dmdLotObjList:list):
        facStartDate = comUtility.Utility.DayStartDate  # 공장 시작시간

        # 월별 생산 제품 분리


        # prodSeqArr = []
        # # 각 Grade 별 product 초기화
        # for val in gradeDueSeqDict.values():
        #     prodSeqArr = prodSeqArr.append[[val[0]]]     # 각 Grade 별 duedate가 가장 작은 값으로 초기화
        #
        # for prodSeq in prodSeqArr:
        #     # prodSeq: [FistProdLotObj]
        #     candidate = {}
        #     for prodLotObj in dmdProdLotList:
        #         if prodLotObj != prodSeq[0]:
        #             lotObj:objLot.Lot = prodLotObj
        #             ### 제약조건 반영
        #             ## 1. 납기 조건
        #             if True == True:
        #                 ## 2. 제약 조건
        #                 if True == True:
        #                     candidate.update({(prodSeq, lotObj) : prodWheelDict[(prodSeq, lotObj)]})


        pass
    def _optLotSeqDay(self, dmdLotObjList:list):

        facStartDate = comUtility.Utility.DayStartDate  # 공장 시작시간
        pass

    # 월별 생산 할 lot 분리 처리
    def _getMonDmdLotDict(self, ):
        monDmdLotDict = {}

        return monDmdLotDict

    # 월별 생산 Capa / 월별 수요 capa 계산
    def _calcMonDmdCapa(self):
        pass

    def GetFirstBestGrade(self):

        prodWheelDict = self._prodWheelDict

        # bestGrade = []
        bestGrade = 'GRADE_A'   # 임시적
        # SLOP Optimization 적용 예정

        return bestGrade

    def GetNextBestGrade(self, grade):
        pass

    def getReactorDmdProdGroup(self, gradeGroup:dict, dmdGradeList:list):
        '''
        gradeGroup: {gradeGroup1 : [GRADE_A, ...],
                    gradeGroup2 : [GRADE_D, ...],
                    ....}
        dmdGradeList: ['GRADE_A', 'GRADE_B', ...., 'GRADE_G]
        '''
        dmdReactorProdDict = {}

        for key in gradeGroup.keys():
            for grade in dmdGradeList:
                if grade in gradeGroup[key]:
                    if key not in dmdReactorProdDict.keys():
                        dmdReactorProdDict.update({key: [grade]})
                    else:
                        if grade not in dmdReactorProdDict[key]:
                            valList = dmdReactorProdDict[key].append(grade)
                            dmdReactorProdDict.update({key: valList})

        return dmdReactorProdDict

    def getDmdGradeList(self, dmdProdLotObjList:list):
        dmdGradeList = []

        for prodLotObj in dmdProdLotObjList:
            lotObj:objLot.Lot = prodLotObj
            if lotObj.Grade not in dmdGradeList:
                dmdGradeList.append(lotObj.Grade)

        return dmdGradeList

    def getLotDueDate(self, lotObj):
        return lotObj.DueDate

    def getGradeDueSeq(self, dmdProdLotObjList:list):
        dmdGradeList = self.getDmdGradeList(dmdProdLotObjList=dmdProdLotObjList)

        gradeDueSeqDict = {}
        # 모든 Grade를 Dictionary Key로 setting
        for dmdGrade in dmdGradeList:
            gradeDueSeqDict.update({dmdGrade, []})

        # Grade 별 Lot List 생성
        for prodLotObj in dmdProdLotObjList:
            lotObj:objLot.Lot = prodLotObj
            gradeList = gradeDueSeqDict[lotObj.Grade]
            gradeList.append(lotObj)
            gradeDueSeqDict.update({lotObj.Grade : gradeList})

        # Grade별 Due date 기준으로 sorting
        for key, val in gradeDueSeqDict.items():
            sorted_val = val.sort(key=self.getLotDueDate)
            gradeDueSeqDict.update({key:sorted_val})

        return gradeDueSeqDict

    def _setProdWheelDict(self):
        costCalStd = comUtility.Utility.ProdWheelCalStd
        # prodWheel = self._prodWheel
        prodWheel = self._dataMgr.dfProdWheel
        appliedProdWheel = {}

        for i in range(len(prodWheel)):
            if costCalStd == 'hour':
                appliedProdWheel[(prodWheel.loc[i, 'grade_from'], prodWheel.loc[i, 'grade_to'])] = prodWheel.loc[i, 'hour']
            elif costCalStd == 'og':
                appliedProdWheel[(prodWheel.loc[i, 'grade_from'], prodWheel.loc[i, 'grade_to'])] = prodWheel.loc[i, 'og']
            else:
                continue   # hour / og weight 버전 추가

        return appliedProdWheel

    # RM에서 LotObjList를 받아오는 처
    def _getRmLotObjList(self):
        rmLotObjList = []

        for wh in self.WhouseObjList:
            whObj:objWarehouse.Warehouse = wh
            if whObj.Kind == 'RM':
                rmLotObjList = whObj.LotObjList

        return rmLotObjList

    # ================================================================================= #
    # RM -> Reactor
    # ================================================================================= #

    def AssignLotToReactor(self, lotObjList:list):

        reactorOperObj = self._getReactorOper()

        for lot in lotObjList:
            lotObj:objLot.Lot = lot

            macObj = reactorOperObj.MacObjList[0]
            lotObj.Machine = macObj
            lotObj.WareHouse = None
            lotObj.Location = reactorOperObj
            lotObj.ToLoc = reactorOperObj.ToLoc

    def _getReactorOper(self):

        for oper in self.OperList:
            operObj:simOperMgr.Operation = oper

            if operObj.Kind == "REACTOR":
                return operObj

        print("RACTOR 공정 존재하지 않음")
        raise AssertionError()

    # ================================================================================= #
    # Reactor -> Silo
    # ================================================================================= #

    def CheckLotObjSiloGrade(self, lotObjList:list):
        '''
        Lot을 각 silo의 현재 상태를 고려해서 할당 가능한
        '''

        siloObjList = self.GetCurSiloState()

        for prodLotObj in lotObjList:
            lotObj:objLot.Lot = prodLotObj

            # 각각의 Silo에 lotObj가 들어있는지 search
            for silo in siloObjList:
                siloObj:objWarehouse.Warehouse = silo
                siloLotObjList = silo.LotObjList
                lotObjGradeList = self._getLotObjGrade(lotObjList=siloLotObjList)

                # silo에 여러 grade 제품이 들어있는지 확인 - 았으면 에러처리
                if len(lotObjGradeList) > 1:
                    print("{} Silo에 여러 grade 제품이 들어있음")
                    raise AssertionError()

                if lotObjGradeList == None: # Silo에 제품이 없는 경우
                    continue

                elif lotObj.Grade == lotObjGradeList[0]:
                    if lotObj.Qty < siloObj.CurCapa:        # Silo capa
                        lotObj.Silo = siloObj.Id
                        # siloObj.CurCapa -= lotObj.Qty       # silo에 할당된 양 capa에서 차감하는 처리
                        # siloObj.LotObjList.append(lotObj)   # silo에 할당할 lot을 추가하는 처리
                        break

        return lotObjList

    def AssignLotToSilo(self, lotObjList:list):

        siloObjList = self.GetCurSiloState()

        for prodLotObj in lotObjList:
            lotObj:objLot.Lot = prodLotObj


            if len(lotObj.Silo) != 0:   # Silo가 존재하는 경우 그 silo에 할당
                whObj = self._getSiloWhObj(whId=lotObj.Silo)
                lotObj.WareHouse = whObj
                lotObj.Machine = None
                lotObj.Location = whObj
                # lotObj.FromLoc = whObj.FromLoc
                lotObj.ToLoc = whObj.ToLoc

                # 할당된 silo에 lot을 추가하는 처리
                for silo in siloObjList:
                    siloObj: objWarehouse.Warehouse = silo
                    if lotObj.Silo == siloObj.Id:
                        siloObj.LotObjList.append(lotObj)   # silo에 할당할 lot을 추가하는 처리
                        siloObj.CurCapa -= lotObj.Qty       # silo에 할당된 양 capa에서 차감하는 처리

            else:   # mapping 할 silo가 없는 경우 새로 할당할 silo를 찾는 처리
                for silo in siloObjList:
                    siloObj:objWarehouse.Warehouse = silo
                    siloLotObjList = siloObj.LotObjList
                    # lotObjGradeList = self._getLotObjGrade(siloLotObjList)

                    if len(siloObj.LotObjList) == 0:    # Silo에 할당 된 lot이 없으면 할당 처리
                       whObj = self._getSiloWhObj(whId=lotObj.Silo)
                       lotObj.WareHouse = whObj
                       lotObj.Machine = None
                       lotObj.Location = whObj
                       # lotObj.FromLoc = whObj.FromLoc
                       lotObj.ToLoc = whObj.ToLoc

                       # 할당된 silo에 lot을 추가하는 처리
                       for silo in siloObjList:
                           siloObj: objWarehouse.Warehouse = silo
                           if lotObj.Silo == siloObj.Id:
                               siloObj.LotObjList.append(lotObj)    # silo에 할당할 lot을 추가하는 처리
                               siloObj.CurCapa -= lotObj.Qty        # silo에 할당된 양 capa에서 차감하는 처리

    def GetCurSiloState(self):

        siloWhList = []

        for whObj in self.WhouseObjList:
            wh:objWarehouse.Warehouse = whObj

            if wh.Kind == 'silo':
                siloWhList.append(wh)

        return siloWhList

    def _getSiloWhObj(self, whId:str):

        for wh in self.WhouseObjList:
            whObj:objWarehouse.Warehouse = wh

            if whObj.Kind == 'silo' and whObj.Id == whId:
                return whObj

    def _getLotObjGrade(self, lotObjList:list):
        lotObjGradeList = []

        for prodLotObj in lotObjList:
            lotObj:objLot.Lot = prodLotObj

            if lotObj.Grade not in lotObjGradeList:
                lotObjGradeList.append(lotObj.Grade)

        return lotObjGradeList


    # ================================================================================= #
    # Silo -> Packaging
    # ================================================================================= #

    def AssignLotToPackaging(self, lotObjList:list):
        packOperObj = self._getPackOper()
        packMacList = self._getPackMacList()

        for lot in lotObjList:
            lotObj:objLot.Lot = lot

            for packMac in packMacList:
                packMacObj:objMachine.Machine = packMac

                if lotObj.PackSize == packMacObj.Id:
                    if packMacObj.Status == 'IDLE':     # Machine이 idle 상태인 경우 제품 할당 처리
                        lotObj.Oper = packOperObj
                        lotObj.Machine = packMacObj.Id
                        lotObj.Location = packOperObj
                        # FromLoc = packOperObj.FromLoc
                        lotObj.ToLoc = packOperObj.ToLoc

                    elif packMacObj.Status == 'PROGRESS':   # 해당 machine이 이미 돌아가는 있는 경우 할당 불가
                        continue

    def _getPackOper(self):

        for oper in self.OperList:
            operObj:simOperMgr.Operation = oper
            if operObj.ID == 'BAGGING':
                return operObj

    def _getPackMacList(self):

        packMacList = []

        for oper in self.OperList:
            operObj:simOperMgr.Operation = oper
            if operObj.ID == 'BAGGING':
                packMacList = operObj.MacObjList

        return packMacList


    # ================================================================================= #
    # Packagin -> FGI
    # ================================================================================= #

    def AssignLotToFGI(self, lotObjList:list):

        fgiWhObj = self._getFgiWh()

        for lot in lotObjList:
            lotObj:objLot.Lot = lot

            if fgiWhObj.CurCapa > lotObj.Qty:   # 자동창고의 현재 capa 체크
                lotObj.Oper = None
                lotObj.Machine = None
                lotObj.Location = fgiWhObj
                lotObj.ToLoc = fgiWhObj.ToLoc

                fgiWhObj.LotObjList.append(lotObj)  # 자동창고에 lot 추가
                fgiWhObj.CurCapa -= lotObj.Qty      # 추가한 lot 수량만큰 현재 창고 capa에서 차감 처리

            else:
                # ==================================== #
                # 중합공정 중단 처리 or 강제 출하처리 추가 필요
                # ==================================== #
                break    # 자동창고가 Full 인 상태이므로 적재 중


    def _getFgiWh(self):

        for wh in self.WhouseObjList:
            whObj:objWarehouse.Warehouse = wh

            if whObj.Kind == "FGI":
                return whObj

        print("FGS warehouse 객체가 없음")
        raise AssertionError()

    def _findWhById(self, wh_id: str):
        for obj in self.WhouseObjList:
            whObj: objWarehouse.Warehouse = obj
            if whObj.Id == wh_id:
                return whObj

        return None

    def _register_new_oper(self, oper_id: str, kind: str, return_flag: bool = False):

        operObj: simOperMgr.Operation = simOperMgr.Operation(oper_id=oper_id, kind=kind)
        operObj.setup_object()
        self.OperList.append(operObj)

        if return_flag:
            return operObj

    def _register_new_machine(self, mac_id: str, oper: simOperMgr, uom=""):

        macObj: objMachine = objMachine.Machine(factory=self, mac_id=mac_id)
        macObj.setup_object(status="IDLE", uom=uom)

        operObj: simOperMgr.Operation = oper
        operObj.MacObjList.append(macObj)

        self.MachineList.append(macObj)

    def _register_new_warehouse(self, wh_id: str, kind: str, capacity: float = None, return_flag: bool = False):

        whObj: objWarehouse = objWarehouse.Warehouse(factory=self, whId=wh_id, kind=kind)
        whObj.setup_object(capacity)
        self.WhouseObjList.append(whObj)

        if return_flag:
            return whObj

    def _register_lot_to(self, lot_obj: objLot, to: str):
        if not (self._chk_is_type(attr=to, obj_type=objMachine.Machine) or
                self._chk_is_type(attr=to, obj_type=objWarehouse.Warehouse) or
                to == "self"):
            raise TypeError(
                "Machine 이나 Warehouse 가 아닌 곳에 Lot 을 등록하려 합니다."
            )
        if to == "self":
            self._lot_obj_list.append(lot_obj)
        else:
            attr_obj = self._get_attr(attr=to)
            # attr_obj.

    def _chk_is_type(self, attr: str, obj_type: type):
        is_type: bool = False
        if not self._chk_exists(attr=attr):
            return is_type
        attr_obj = self._get_attr(attr=attr)
        if type(attr_obj) is not obj_type:
            return is_type
        is_type = True
        return is_type

    def _chk_is_warehouse(self, attr: str):
        is_warehouse: bool = False
        if not self._chk_exists(attr=attr):
            return is_warehouse
        attr_obj = self._get_attr(attr=attr)
        if type(attr_obj) is not objWarehouse.Warehouse:
            return is_warehouse
        is_warehouse = True
        return is_warehouse

    def _chk_is_machine(self, attr: str):
        is_machine: bool = False
        if not self._chk_exists(attr=attr):
            return is_machine
        attr_obj = self._get_attr(attr=attr)
        if type(attr_obj) is not objMachine.Machine:
            return is_machine
        is_machine = True
        return is_machine

    def _get_attr(self, attr: str):
        attr_obj = None
        if self._chk_exists(attr=attr):
            attr_obj = self.__getattribute__(name=attr)
        return attr_obj

    def _chk_exists(self, attr: str):
        existence: bool = attr in self.__dict__.keys()
        return existence
