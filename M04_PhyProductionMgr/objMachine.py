# -*- coding: utf-8 -*-

import datetime
from datetime import timedelta

from M03_Site import simFactoryMgr, simOperMgr
from M05_ProductManager import objLot
from M06_Utility import comUtility, comCalMgr

class Machine(object):
    def __init__(self, factory: simFactoryMgr, operation: simOperMgr, mac_id: str):
        self._factory: simFactoryMgr = factory
        self.Oper: simOperMgr.Operation = operation
        self._calendar: comCalMgr.CalendarManager = None
        self.Id: str = mac_id

        self.hasCalendar: bool = False

        # PACKAGING MACHINE PROPERTIES
        self.Uom: str = ""          # 25 KG / 750 KG / BULK
        self.PackKind: str = ""     # WV / FS / BK / SB

        # STATUS
        self.Status: str = "IDLE"   # IDLE / PROC / DOWN
        self.StartTime: datetime.datetime = None
        self.EndTime: datetime.datetime = None

        # CURRENT PROCESSING LOT
        self.Lot: objLot.Lot = None
        self.BfLotGrade: str = None

    def setup_object(self, status: str, uom: str = "", need_calendar: bool = False,
                     work_start_hour: int = None, work_end_hour: int = None):
        self.Status = status

        # PACKAGING MACHINE PROPERTIES
        self.Uom = uom

        # Setup Calendar
        if need_calendar or sum([hour is not None for hour in [work_start_hour, work_end_hour]]) >= 2:
            self.hasCalendar = True
            self._calendar = comCalMgr.CalendarManager()
            self._calendar.SetupObject(factory=self._factory, machine=self,
                                       start_date=comUtility.Utility.DayStartDate,
                                       end_date=comUtility.Utility.DayEndDate,
                                       start_hour=work_start_hour, end_hour=work_end_hour
                                       )
        else:
            self.hasCalendar = False

    def assignLotToMac(self, lot: objLot):
        self.Lot = lot

        # Reactor의 Machine의 경우 Lot의 최신 Grade 정보를 기록
        if self.Oper.Kind == "REACTOR":
            # Lot을 할당하고 이전의 Lot의 Grade를 갱신하기 전에 grade change cost를 산출
            bfLotGrade = self.BfLotGrade
            currLotGrade = lot.Grade
            if bfLotGrade != None:
                gradeChangeCost = comUtility.Utility.ProdWheelHour[(bfLotGrade, currLotGrade)]
                self.BfLotGrade = lot.Grade     # Grade Chnage Cost 계산 후 update

                runTime = comUtility.Utility.runtime
                startTime = runTime + timedelta(hours=int(gradeChangeCost))
                self._setStartTime(startTime=startTime)
                self.Lot.ReactIn = self.StartTime
                self.Lot.set_location(location=self)

            else:
                runTime = comUtility.Utility.runtime
                self._setStartTime(startTime=runTime)
                self.Lot.ReactIn = self.StartTime
                self.Lot.set_location(location=self)

                self.BfLotGrade = lot.Grade

        # Bagging의 machine의 경우 Grade Change Cost가 없음
        elif self.Oper.Kind == "BAGGING":
            self._setStartTime(startTime=comUtility.Utility.runtime)
            self.Lot.BaggingIn = self.StartTime
            self.Lot.set_location(location=self)

        duration: datetime.timedelta = self._getLotProcTime(lot=lot)
        endTime = self.StartTime + duration
        self._setEndTime(end_time=endTime)

    def lotLeave(self, actual_leave_flag: bool = True):
        leaving_lot: objLot.Lot = self.Lot
        if actual_leave_flag:
            self.Lot = None
            self._setStartTime()
            self._setEndTime()
            self._set_status(status="IDLE")
        return leaving_lot

    def RunMachine(self):
        if self.Lot is not None:
            self._set_status("PROC")

    # Machine의 비가용 계획 구간에 포함되는지 확인하는 처리
    def chkMacAvailable(self, lot: objLot.Lot):
        chkUnavailableToMac: bool = False
        duration: datetime.timedelta = self._getLotProcTime(lot=lot)
        lotProcStartTime: datetime.datetime = comUtility.Utility.runtime
        lotProcEndTime: datetime.datetime = lotProcStartTime + duration

        # Reactor 공정의 경우 Grade Change Cost를 반영
        if self.Oper.Kind == 'REACTOR':
            bfLotGrade = self.BfLotGrade
            currLotGrade = lot.Grade
            gradeChangeCost = comUtility.Utility.ProdWheelHour[(bfLotGrade, currLotGrade)]

            # Lot이 Machine에 할당되는 시간에 Grade Chage Cost 반영
            lotProcStartTime += timedelta(hours=gradeChangeCost)
            lotProcEndTime += timedelta(hours=gradeChangeCost)

        macStopEndTime: datetime.datetime = None

        if self._calendar is None:
            return chkUnavailableToMac
        for macStop in self._calendar.macStopSeq:
            chkOverlap: bool = self._chkOverlapToMacStopPeriod(
                from_to_tuple=macStop,
                start_time=lotProcStartTime, end_time=lotProcEndTime
            )
            if chkOverlap:
                chkUnavailableToMac = True
                macStopEndTime = macStop[1]
                break

        return chkUnavailableToMac, macStopEndTime

    def getMacStopEndTime(self):
        seq: list = self._calendar.macStopSeq
        seq_end: list = [tup[1] for tup in seq if tup[1] >= comUtility.Utility.runtime]
        break_end = min(seq_end)
        return break_end

    def power_on(self):
        if self.Status != "DOWN":
            pass
        else:
            if self.Lot is None:
                self._set_status("IDLE")
                self._setStartTime(startTime=None)
                self._setEndTime(end_time=None)

    def power_off(self):
        downtime: tuple = self.get_current_downtime()
        self._set_status(status="DOWN")
        self._setStartTime(startTime=downtime[0])
        self._setEndTime(end_time=downtime[1])

    def append_downtime(self, from_date: datetime.datetime, to_date: datetime.datetime):
        if self.hasCalendar:
            self._calendar.append_downtime(from_date=from_date, to_date=to_date)

    def get_current_downtime(self):
        downtime: tuple = None
        for downtime in self._calendar.macStopSeq:
            is_between = self._is_between(
                from_to_tuple=downtime,
                value=comUtility.Utility.runtime
            )
            if is_between:
                return downtime
        return downtime

    def _chkOverlapToMacStopPeriod(self, from_to_tuple: tuple,
                                   start_time: datetime.datetime,
                                   end_time: datetime.datetime):
        is_overlapping: bool = self._is_between(from_to_tuple, start_time) or \
                               self._is_between(from_to_tuple, end_time) or \
                               self._is_including(from_to_tuple, start=start_time, end=end_time)

        return is_overlapping

    def _is_including(self, from_to_tuple: tuple, start: datetime, end: datetime):
        is_including: bool = start <= from_to_tuple[0] and from_to_tuple[1] <= end
        return is_including

    def _is_between(self, from_to_tuple: tuple, value: datetime):
        is_between: bool = from_to_tuple[0] < value < from_to_tuple[1]
        return is_between

    def _set_status(self, status: str):
        self.Status = status

    def _setStartTime(self, startTime: datetime.datetime = None):
        self.StartTime = startTime

    def _setEndTime(self, end_time: datetime.datetime = None):
        self.EndTime = end_time

    def _getLotProcTime(self, lot: objLot):
        duration: datetime.timedelta = \
            lot.ReactDuration if self.Oper.Kind == "REACTOR" else \
            lot.PackDuration
        return duration


def test():

    from PE_Simulator import Simulator

    simulator: Simulator = Simulator()

    factory: simFactoryMgr.Factory = simFactoryMgr.Factory(
        simul=simulator, facID="IM_FACTORY"
    )

    oper: simOperMgr.Operation = simOperMgr.Operation(
        factory=factory, oper_id="BAG", kind="BAGGING"
    )

    macObj: Machine = Machine(factory=simulator, operation=oper, mac_id="BEGGAR")
    macObj.setup_object(status="IDLE", uom="UnitOfMeasure",
                        work_start_hour=8, work_end_hour=20)

    macObj.append()

    print("DEBUGGING")


if __name__ == '__main__':

    test()
