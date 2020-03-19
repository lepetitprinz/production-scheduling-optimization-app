# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import calendar
from scop import Model, Alldiff, Quadratic

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
        self._prodWheelDf = self._dataMgr.dbProdWheel.copy()
        self.ProdWheelHour = ""

        # Configuration 정보
        self._seqOptTimeLimit: int = 1

    def SetupObject(self,
                    dayStartTime: str, year: int, month: int, day: int, horizon_days: int,
                    silo_qty: int, nof_silo: int = 1, silo_wait_hours: int = 0):
        self._utility.SetDayStartTime(value=dayStartTime)
        self._utility.setDayStartDate(year=year, month=month, day=day)
        self._utility.SetDayHorizon(days=horizon_days)
        self._utility.CalcDayEndDate()

        self._utility.setSiloWaitTime(hours=int(silo_wait_hours))

        self._startTime = self._utility.DayStartDate
        self._buildFactory(silo_qty=silo_qty, nof_silo=nof_silo)    # Factory 기본 Configuration 정보 Setting
        self._base_first_event_time()
        self._prodWheelDict = self._setProdWheelDict(costCalStd=self._utility.ProdWheelCalStd)
        self.ProdWheelHour = self._setProdWheelDict(costCalStd='hour')

        self._utility.ProdWheelHour = self.ProdWheelHour.copy()

        self._seqOptTimeLimit = self._utility.OptTimeLimit
        # self._register_new_machine(mac_id="MAC01")
        # self.StockList = self._facUtil.GetStockObjList()
        # self._register_new_warehouse(wh_id="RM")
        # self._register_new_warehouse(wh_id="WH01")
        # self.OperMgrList = self._facUtil.GetOperMgrObjList()

    def SetupResumeData(self, use_mac_down_cal_db: bool = False):
        # RM Warehouse 의 Lot을 배정하는 처리.
        self.setupRmWh()

        # DB 에 등록된 머신별 비가용 시간 정보를 실제 머신에 할당하는 처리
        if use_mac_down_cal_db:
            self.setupMacDownCal()

        self.rebuildMacDownCal()

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

    def rebuildMacDownCal(self):
        for obj in self.MachineList:
            macObj: objMachine.Machine = obj
            if not macObj.hasCalendar:
                continue
            macObj._calendar.rebuild_break_sequence(which_seq="daily")
            macObj._calendar.sort_seq(which_seq="daily")
            macObj._calendar.rebuild_break_sequence(which_seq="shutdown")
            macObj._calendar.sort_seq(which_seq="shutdown")
            macObj._calendar.rebuild_break_sequence(which_seq="breakdown")
            macObj._calendar.sort_seq(which_seq="breakdown")
            macObj._calendar.build_full_sequence()
            macObj._calendar.rebuild_break_sequence(which_seq="full")
            macObj._calendar.sort_seq(which_seq="full")

    def setupMacDownCal(self):
        MacUnAvlTime: pd.DataFrame = self._dataMgr.dbMacUnAvlTime

        print("\nSetting Down Time Calendar from DB...\n")
        for _, row in MacUnAvlTime.iterrows():
            macObj: objMachine.Machine = [obj for obj in self.MachineList if obj.Id == row['macId']][0]

            from_date_str: str = row['fromTime']
            to_date_str: str = row['toTime']

            if not (len(from_date_str) == 14 and len(to_date_str) == 14):
                raise ValueError
            else:
                from_date_str_hh: int = int(from_date_str[8:8 + 1 + 1])
                to_date_str_hh: int = int(to_date_str[8:8 + 1 + 1])
                if not (from_date_str_hh in [i for i in range(0, 1 + 23)] and
                        to_date_str_hh in [i for i in range(0, 1 + 23)]):
                    # print(f"STRPTIME ERROR {from_date_str} / {to_date_str}")
                    from_date_str_hh = from_date_str_hh if from_date_str_hh in [i for i in range(0, 1 + 23)] else 0
                    to_date_str_hh = to_date_str_hh if to_date_str_hh in [i for i in range(0, 1 + 23)] else 0

                from_yymmdd, from_hh, from_mmss = from_date_str[:8], '%02d' % from_date_str_hh, from_date_str[
                                                                                                8 + 1 + 1:]
                from_date_str = f"{datetime.datetime.strftime(datetime.datetime.strptime(from_yymmdd, '%Y%m%d') + datetime.timedelta(days=1), '%Y%m%d')}" \
                                f"{from_hh}{from_mmss}"
                to_yymmdd, to_hh, to_mmss = to_date_str[:8], '%02d' % to_date_str_hh, to_date_str[8 + 1 + 1:]
                to_date_str = f"{datetime.datetime.strftime(datetime.datetime.strptime(to_yymmdd, '%Y%m%d') + datetime.timedelta(days=1), '%Y%m%d')}" \
                              f"{to_hh}{to_mmss}"

            from_date: datetime.datetime = datetime.datetime.strptime(from_date_str, "%Y%m%d%H%M%S")
            to_date: datetime.datetime = datetime.datetime.strptime(to_date_str, "%Y%m%d%H%M%S")
            if comUtility.Utility.EqpOperationTimeLimitYn == 'Y':
                macObj.append_downtime(from_date=from_date, to_date=to_date, to_which="daily")

    def setupRmWh(self):
        rmWh: objWarehouse.Warehouse = self._findWhById(wh_id="RM")
        demand = self._dataMgr.dbDemand.copy()

        # Lot Sizing
        dfDmdLotSizing = self._setDmdProdLotSizing(demand)
        rmWh.setupResumeData(dfDmdLotSizing)
        rmWhLotList: list = rmWh.LotObjList

        lotSeqOptList: list = []
        # NONE >> 기존방식
        # MONTHLY >>
        if self._utility.ProdCycle == "MONTHLY":
            for month in [3, 4, 5, 6]:      # 수정 필요
                tmpRmWhLotList: list = [lot for lot in rmWhLotList if lot.DueDate.month == month]
                if len(tmpRmWhLotList) == 0:
                    continue
                # Grade Sequence Optimization using SCOP algorithm
                tmpLotSeqOptList = rmWh.SeqOptByScop(lotObjList=tmpRmWhLotList, dueUom='nan')
                lotSeqOptList.extend(tmpLotSeqOptList)
        else:
            lotSeqOptList = rmWh.SeqOptByScop(lotObjList=rmWhLotList, dueUom='nan')
        rmWh.RemoveLotList()  # 기존 RM Warehouse에 있는 Lot List 삭제

        # RM warehouse에 최적화 한 lot Sequence 등록
        for obj in lotSeqOptList:
            lotObj: objLot.Lot = obj
            rmWh._registerLotObj(lotObj=lotObj)
            # for Debugging
            # lotObj.reduce_duration(by=10)
            # rmWh._registerLotObj(lotObj=lotObj)

        # # Grade Sequence Optimization using SCOP algorithm
        # gradeSeqOpt = self.SeqOptByScop(dmdLotList=rmWhLotList)
        #
        # # Grade Sequence 별로 Lot을 그룹화해서 List 변환
        # lotSeqOptList = self.GetLotSeqOptList(gradeSeqOpt=gradeSeqOpt, dmdLotList=rmWhLotList, dueUom="nan")
        # rmWh.truncate_lot_list()   # 기존 RM Warehouse에 있는 Lot List 삭제

    def _buildFactory(self, silo_qty: float, nof_silo: int = 1):
        reactor: simOperMgr.Operation = self._registerNewOper(oper_id="REACTOR", kind="REACTOR", return_flag=True)
        m1: objMachine.Machine = self._registerNewMac(mac_id="M1", oper=reactor, uom="", return_flag=True)

        bagging: simOperMgr.Operation = self._registerNewOper(oper_id="BAGGING", kind="BAGGING", return_flag=True)
        p2: objMachine.Machine = self._registerNewMac(mac_id="P2", oper=bagging, uom="25 KG",
                                                      work_start_hour=comUtility.Utility.BaggingWorkStartHour,
                                                      work_end_hour=comUtility.Utility.BaggingWorkEndHour,
                                                      use_work_hour=comUtility.Utility.BaggingOperTimeConst,
                                                      return_flag=True)
        p7: objMachine.Machine = self._registerNewMac(mac_id="P7", oper=bagging, uom="750 KG",
                                                      work_start_hour=comUtility.Utility.BaggingWorkStartHour,
                                                      work_end_hour=comUtility.Utility.BaggingWorkEndHour,
                                                      use_work_hour=comUtility.Utility.BaggingOperTimeConst,
                                                      return_flag=True)
        p9: objMachine.Machine = self._registerNewMac(mac_id="P9", oper=bagging, uom="BULK",
                                                      work_start_hour=comUtility.Utility.BaggingWorkStartHour,
                                                      work_end_hour=comUtility.Utility.BaggingWorkEndHour,
                                                      use_work_hour=comUtility.Utility.BaggingOperTimeConst,
                                                      return_flag=True)
        # self._register_new_machine(mac_id="P2", oper=bagging, uom="25 KG", need_calendar=True,
        #                            work_start_hour=8, work_end_hour=20)
        # self._register_new_machine(mac_id="P7", oper=bagging, uom="750 KG", need_calendar=True,
        #                            work_start_hour=8, work_end_hour=20)
        # self._register_new_machine(mac_id="P9", oper=bagging, uom="BULK", need_calendar=True,
        #                            work_start_hour=8, work_end_hour=20)

        # self.StockList = self._facUtil.GetStockObjList()
        rm: objWarehouse.Warehouse = self._registerNewWarehouse(wh_id="RM", kind="RM", return_flag=True)     # RM / WareHouse / silo / hopper
        # rm.assign_random_lpst()
        silo_qty /= nof_silo
        silos: list = []
        for i in range(1, 1 + nof_silo):
            silo: objWarehouse.Warehouse = self._registerNewWarehouse(wh_id=f"SILO{'%02d' % i}", kind="silo", capacity=silo_qty, return_flag=True)
            silos.append(silo)
        # hopper: objWarehouse.Warehouse = self._register_new_warehouse(wh_id="HOPPER", kind="hopper", return_flag=True)
        fgi: objWarehouse.Warehouse = self._registerNewWarehouse(wh_id="FGI", kind="FGI", capacity=43000, return_flag=True)

        rm.setToLoc(to_loc=reactor.Id)
        reactor.SetFromLocs(from_locs=[rm])
        reactor.SetFromLoc(from_loc=rm.Kind)
        reactor.SetToLoc(to_loc=silos[0].Kind)
        for silo in silos:
            silo.setToLoc(to_loc=bagging.Id)
        # hopper.set_to_location(to_loc=fgi.Id)
        bagging.SetFromLocs(from_locs=silos)
        bagging.SetFromLoc(from_loc=silos[0].Kind)
        bagging.SetToLoc(to_loc=fgi.Id)
        fgi.setToLoc(to_loc="Sales")     # Ternminal Status

        # 머신 가동 제약 등록
        if comUtility.Utility.ReactorShutdownYn == 'Y':
            m1.append_downtime(from_date=self._utility.ReactorShutdownStartDate,
                               to_date=self._utility.ReactorShutdownEndDate,
                               to_which="shutdown")
        # p2.append_downtime(from_date=, to_date=)
        # p7.append_downtime(from_date=, to_date=)
        # p9.append_downtime(from_date=, to_date=)

        # m1._calendar.build_full_sequence()
        # p2._calendar.build_full_sequence()
        # p7._calendar.build_full_sequence()
        # p9._calendar.build_full_sequence()

    def _base_first_event_time(self):
        for obj in self.OperList:
            operObj: simOperMgr.Operation = obj
            operObj.ResetFstEventTime(init_flag=True)
        for obj in self.WhouseObjList:
            whObj: objWarehouse.Warehouse = obj
            if whObj.Id == "RM":
                whObj.setFstEventTime(runTime=self._utility.DayStartDate, use_flag=True)
            else:
                whObj.setFstEventTime(use_flag=True)

    def sendInitEvent(self):
        """공장 객체 초기화 정보를 DB에 전달하는 메서드"""
        pass

    def RunFactory(self):
        print(f"\n\nFactory {self.ID} is Running.")

        endFlag: bool = False
        end_date: datetime.datetime = self._utility.DayEndDate  # 공장 가동의 종료시간
        loopCnt: int = 0

        while not endFlag:
            # OperTAT, Machine, Transporter, Warehouse 에서 이벤트 처리 대상을 찾기
            runTime: datetime.datetime = self.Get1stTgtMinTime()
            tgtArr: list = self.Get1stTgtArray(runTime=runTime)

            print(f"runTime: {runTime}")

            if len(tgtArr) < 1:
                # self._utility.LotStatusObj.PrintWaitLotList()
                endFlag = True
                continue

            elif runTime >= end_date:
                endFlag = True
                self._utility.SetRuntime(runtime=end_date)
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

            if runTime != self._utility.Runtime:
                self._utility.SetRuntime(runTime)

            tgtCnt = len(tgtArr)
            for obj in tgtArr:
                print(f"\t{obj.__class__.__name__}.{obj.Id}")
                obj.SyncRunningTime()

            print("")
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
    # : Demand 제품의 요구 수량이 minimum
    # ================================================================================= #
    def _setDmdProdLotSizing(self, demand: pd.DataFrame):

        minLotSize = comUtility.Utility.MinLotSize
        maxLotSize = comUtility.Utility.MaxLotSize

        # 빈 DataFrame 생성
        dmdProdLot = pd.DataFrame(columns=['yyyymm', 'prodCode', 'product', 'lotId', 'qty'])

        idx = 0
        for _, row in demand.iterrows():
            # Lot Sizing
            if row['qty'] < minLotSize:
                dmdProdLot.loc[idx, 'qty'] = minLotSize
                dmdProdLot.loc[idx, 'yyyymm'] = row['yyyymm']
                dmdProdLot.loc[idx, 'prodCode'] = row['prodCode']
                dmdProdLot.loc[idx, 'product'] = row['product'] + '_' + str(1)
                idx += 1

            elif row['qty'] > minLotSize and row['qty'] < maxLotSize:
                dmdProdLot.loc[idx, 'qty'] = row['qty']
                dmdProdLot.loc[idx, 'yyyymm'] = row['yyyymm']
                dmdProdLot.loc[idx, 'prodCode'] = row['prodCode']
                dmdProdLot.loc[idx, 'product'] = row['product'] + '_' + str(1)
                idx += 1

            else:
                quotient = int(row['qty'] // maxLotSize)
                remainder = row['qty'] % maxLotSize

                # Maximum Lot 단위 처리
                for i in range(quotient):
                    dmdProdLot.loc[idx, 'qty'] = maxLotSize
                    dmdProdLot.loc[idx, 'yyyymm'] = row['yyyymm']
                    dmdProdLot.loc[idx, 'prodCode'] = row['prodCode']
                    dmdProdLot.loc[idx, 'product'] = row['product'] + '_' + str(i+1)
                    idx += 1

                # 나머지 lot 추가 처리
                if remainder < minLotSize:
                    dmdProdLot.loc[idx, 'qty'] = minLotSize
                else:
                    dmdProdLot.loc[idx, 'qty'] = remainder
                dmdProdLot.loc[idx, 'yyyymm'] = row['yyyymm']
                dmdProdLot.loc[idx, 'prodCode'] = row['prodCode']
                dmdProdLot.loc[idx, 'product'] = row['product'] + '_' + str(quotient+1)
                idx += 1

        return dmdProdLot

    # ------------------------------------------------------------------------------------------------------ #
    # Grade Sequence Optimization Using SCOP algorithm
    # ------------------------------------------------------------------------------------------------------ #
    def SeqOptByScop(self, dmdLotList:list):
        prodWheelCostUom = comUtility.Utility.ProdWheelCalStd
        prodWheel = self._prodWheelDf
        dmdLotGradeList = self._getLotGradeList(lotList=dmdLotList)     # 전달받은 Lot List에 해당하는 Grade list 산출

        # 주어진 Grade List에 해당하는 Production Wheel Cost만 indexing
        dmdLotProdWheel = prodWheel[(prodWheel['grade_from'].isin(dmdLotGradeList)) & (prodWheel['grade_to'].isin(dmdLotGradeList))]
        dmdLotProdWheel = dmdLotProdWheel.reset_index(drop=True)    # Indexing 후 index reset

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
                        if k == gradeLen - 1:   # 마지막을 0으로 처리하고 시작을 1부터 시작
                            ell = 0
                        else:
                            ell = k + 1
                        obj.addTerms(costList[i][j], varList[i], k, varList[j], ell)

        model.addConstraint(obj)
        model.Params.TimeLimit = self._seqOptTimeLimit  # 최적화 시간제약
        sol, violated = model.optimize()
        optSeqSol = sorted(sol.items(), key=lambda x: int(x[1]))
        oprtSeqGrade = [x[0] for x in optSeqSol]

        return oprtSeqGrade

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
                lotSeqOptList.extend(lotByGradeGroupDict[grade])

            # Lot Sequence 순서로 lpst 할당
            lpst = 1
            for lot in lotSeqOptList:
                lotObj:objLot.Lot = lot
                lotObj.Lpst = lpst
                lpst += 1

        else:
            pass

        return lotSeqOptList

    def _getLotGradeList(self, lotList:list):
        lotGradeList = []

        for lot in lotList:
            lotObj:objLot.Lot = lot
            if len(lotGradeList) == 0:
                lotGradeList.append(lotObj.Grade)
            else:
                if lotGradeList[-1] != lotObj.Grade:
                    lotGradeList.append(lotObj.Grade)

        return lotGradeList

    # 월별 생산 할 lot 분리 처리
    def _getMonDmdLotDict(self, dmdLotList:list):
        monDmdLotDict = {}
        dmdLotDf = self._dataMgr.dbDemand.copy()
        months = dmdLotDf['yyyymm'].unique().tolist()

        for month in months:
            for lot in dmdLotList:
                lotObj:objLot.Lot = lot
                if lotObj.DueDate == self._getLastDayOfMon(dueDate=month):
                    if month not in monDmdLotDict.keys():
                        monDmdLotDict[month] = [lotObj]
                    else:
                        tempList = monDmdLotDict[month].append(lotObj)
                        monDmdLotDict[month] = tempList

        return monDmdLotDict

    def _getLastDayOfMon(self, dueDate: str):
        dateTemp: datetime.datetime = datetime.datetime.strptime(dueDate, '%Y%m')
        last_day, month_len = calendar.monthrange(year=dateTemp.year, month=dateTemp.month)
        lastDayOfMon = dateTemp.replace(day=month_len, hour=23, minute=59, second=59)
        return lastDayOfMon

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

    # Production Wheel Cost - cost 기준에 맞춰 Dictionary로 생성
    def _setProdWheelDict(self, costCalStd:str):
        # prodWheel = self._prodWheel
        prodWheel = self._dataMgr.dbProdWheel.copy()
        appliedProdWheel = {}

        for i in range(len(prodWheel)):
            if costCalStd == 'hour':
                appliedProdWheel[(prodWheel.loc[i, 'grade_from'], prodWheel.loc[i, 'grade_to'])] = prodWheel.loc[i, 'hour']
            elif costCalStd == 'og':
                appliedProdWheel[(prodWheel.loc[i, 'grade_from'], prodWheel.loc[i, 'grade_to'])] = prodWheel.loc[i, 'og']
            else:
                continue   # hour / og weight 버전 추가

        return appliedProdWheel

    def _findWhById(self, wh_id: str):
        for obj in self.WhouseObjList:
            whObj: objWarehouse.Warehouse = obj
            if whObj.Id == wh_id:
                return whObj

        return None

    def _registerNewOper(self, oper_id: str, kind: str, return_flag: bool = False):

        operObj: simOperMgr.Operation = simOperMgr.Operation(factory=self, oper_id=oper_id, kind=kind)
        operObj.setupObject()
        self.OperList.append(operObj)

        if return_flag:
            return operObj

    def _registerNewMac(self, mac_id: str, oper: simOperMgr, uom="",
                        work_start_hour: int = None,
                        work_end_hour: int = None,
                        use_work_hour: bool = False,
                        return_flag: bool = False
                        ):

        macObj: objMachine = objMachine.Machine(factory=self, operation=oper, mac_id=mac_id)
        macObj.setup_object(status="IDLE", uom=uom, use_work_hour=use_work_hour,
                            work_start_hour=work_start_hour, work_end_hour=work_end_hour)

        operObj: simOperMgr.Operation = oper
        operObj.MacObjList.append(macObj)

        self.MachineList.append(macObj)

        if return_flag:
            return macObj

    def _registerNewWarehouse(self, wh_id: str, kind: str, capacity: float = None, return_flag: bool = False):

        whObj: objWarehouse = objWarehouse.Warehouse(factory=self, whId=wh_id, kind=kind)
        whObj.setupObject(capacity)
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
            if lot_obj not in self._lot_obj_list:
                self._lot_obj_list.append(lot_obj)
        else:
            attr_obj = self._get_attr(attr=to)
            # attr_obj.

    def _removeLot(self, lot: objLot):
        lotObj: objLot.Lot = lot
        try:
            self._lot_obj_list.remove(lotObj)
        except ValueError as e:
            raise e

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

        # ============================================================================================================== #
        # Lot Sequencing Optimization
        # ============================================================================================================== #
        # Logic
        # 1.RM Warehouse 에서 LotObjList 받아오기
        # 2.Production Cycle 기준으로 Lot Sequencing 최적화(dueDataUom 기준)
        #   - nan: 고정생산주기가 없는 경우, Grade Sequencing (Production Wheel만 고려)
        #   - mon: 월 단위의 납기인 경
        #          우선순위
        #           - 월 생산 Capa > 월 수요 총 Capa : Production Wheel 고려)
        #           - 월 생산 Capa < 월 수요 총 Capa : Demand Priority(내수/수출) 고려)
        #   - day: 납기일을 최우선 기준으로하여 lot Sequencing (우선순위: Demand Priority > 납기일 > prodcution wheel cost)
        # 3.SCOP algorithms을 이용하여 Grade Sequence Optimization
        # ============================================================================================================== #
        # 4.제약 반영
        # ============================================================================================================== #
        # 4-1.Capacity 제약 Module (Warehouse에 적용)
        # objWarehouse 에서 Check
        # - Silo Warehouse
        # - FGI Warehouse
        # ============================================================================================================== #
        # 4-2.Time 제약 Module - Operation의 Machine에 적용
        # somOperMgr 에서 Check
        # - 중합공정 (Reactor)
        # - 포장공정 (Bagging)
        # ============================================================================================================== #

    def geOptLotSeqList(self, dmdLotList: list):
        '''
        - dmdLotObjList: 월 단위의 Demand Product Lot List
                           ex) [prodLotObj1, prodLotObj2, prodLotObj3, prodLotObj4, ...]
        - dueDateUom (mon / week / day)
            1) nan : 생산주기가 없는 경우 Grade 별로 Sequencing 후 각 Grade 별로 Lot을 임의 할당
            2) mon : 월 단위의 납기 고려하여 Seqeuncing을 진행
            3) day : 일 단위의 납기 기준의 경우 납기 기준으로

        '''

    # --------------------------------------------------------- #
    # Packaging Type Sequence Optimization
    # : Due Uom - nan/mon  같은 Grade 제품 간의 구분 안함
    # : Due Uom - day 같은 Grade에서 due data 기준으로 순서 고려
    # --------------------------------------------------------- #

    # Warehouse에서 Check
    # ------------------------------------------------------------------------------------------------------ #
    # Grade Sequence Change Check 모둘
    # ------------------------------------------------------------------------------------------------------ #
    # - Lot Leave 이전 Grade Sequence와 Lot Leave 이후 Grade Sequence가 같은 경우 : Grade Sequence Change 없음
    # - Lot Leave 이전 Grade Sequence에서 첫번째 Grade 제외한  Grade Sequence와 Lot Leave 이후 Grade Sequence가 같은 경우
    #   : Grade Sequence Change 없음
    # - Lot Leave 이전 Grade Sequence에서 첫번째 Grade 제외한  Grade Sequence와 Lot Leave 이후 Grade Sequence가 다른 경우
    #   : Grade Sequence Change 발생
    # ------------------------------------------------------------------------------------------------------ #
    # def ChkGradeSeqChange(self, beforeLotList, afterLotList):
    #     beforeGradeSeq = []
    #     afterGradeSeq = []
    #
    #     for bfLot in beforeLotList:
    #         bfLotObj:objLot.Lot = bfLot
    #         if len(beforeGradeSeq) == 0:
    #             beforeGradeSeq.append(bfLotObj.Grade)
    #         else:
    #             if beforeGradeSeq[-1] != bfLotObj.Grade:
    #                 beforeGradeSeq.append(bfLotObj.Grade)
    #
    #     for afLot in afterLotList:
    #         afLotObj:objLot.Lot = afLot
    #         if len(afterGradeSeq) == 0:
    #             afterGradeSeq.append(afLotObj.Grade)
    #         else:
    #             if afterGradeSeq[-1] != afLotObj.Grade:
    #                 afterGradeSeq.append(afLotObj.Grade)
    #
    #     if (beforeGradeSeq == afterGradeSeq) or (beforeGradeSeq[1:] == afterGradeSeq):
    #         return False    # Grade Sequence Change 없음
    #     else:
    #         return True     # Grade Sequence Change 발생