# -*- coding: utf-8

from M04_PhyProductionMgr import objMachine, objStocker


class Operation(object):
    def __init__(self, oper_id: str):
        # self._facUtil: facUtility.FacUtility = None  #
        self.ID: str = oper_id
        self.MacObjList: list = []
        self.StockObj: objStocker.Stocker = None

        self.FirstEventTime: int = -1


    def setup_object(self):
        pass

    def SetupResumeData(self, lotObjList: list):
        # self.ProdLotList 에 추가하는 처리.
        # TAT모드인 경우, ActEndTime 계산 필요.
        # 09/19 self._lotCnt 에 카운트 필요.
        lotObjList = self._checkInitErrorLotList(lotObjList=lotObjList)

    def AppendMac(self, tgtMac: objMachine):
        self.MacObjList.append(tgtMac)

