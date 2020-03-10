# -*- coding: utf-8 -*-

import datetime

from M03_Site import simFactoryMgr, simOperMgr
from M05_ProductManager import objLot
from M06_Utility import comUtility, comCalMgr


class Machine(object):
    def __init__(self, factory: simFactoryMgr, operation: simOperMgr, mac_id: str):
        self._factory: simFactoryMgr = factory
        self._oper: simOperMgr.Operation = operation
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

    def setup_object(self, status: str, uom: str = "", need_calendar: bool = False,
                     work_start_hour: int = None, work_end_hour: int = None):
        self.Status = status

        # PACKAGING MACHINE PROPERTIES
        self.Uom = uom

        # Setup Calendar
        if need_calendar:
            self.hasCalendar = True
            self._calendar = comCalMgr.CalendarManager()
            self._calendar.SetupObject(factory=self._factory, machine=self,
                                       start_date=comUtility.Utility.DayStartDate,
                                       end_date=comUtility.Utility.DayEndDate,
                                       start_hour=work_start_hour, end_hour=work_end_hour
                                       )
        else:
            self.hasCalendar = False

    def assign_lot(self, lot: objLot):
        self.Lot = lot
        self._set_start_time(comUtility.Utility.runtime)
        self.Lot.set_location(location=self)
        duration: datetime.timedelta = self._get_lot_proc_time(lot=lot)
        endTime = self.StartTime + duration
        self._set_end_time(end_time=endTime)

    def lot_leave(self, actual_leave_flag: bool = True):
        leaving_lot: objLot.Lot = self.Lot
        if actual_leave_flag:
            self.Lot = None
            self._set_start_time()
            self._set_end_time()
            self._set_status(status="IDLE")
        return leaving_lot

    def RunMachine(self):
        if self.Lot is not None:
            self._set_status("PROC")

    def chk_breakdown(self, lot: objLot.Lot):
        is_break: bool = False
        duration: datetime.timedelta = self._get_lot_proc_time(lot=lot)
        lot_start_time: datetime.datetime = comUtility.Utility.runtime
        lot_end_time: datetime.datetime = lot_start_time + duration
        break_end: datetime.datetime = None
        if self._calendar is None:
            return is_break
        for breakdown in self._calendar.breakdown_seq:
            is_overlapping: bool = self._is_overlapping_to_break(
                from_to_tuple=breakdown,
                start_time=lot_start_time, end_time=lot_end_time
            )
            if is_overlapping:
                is_break = True
                break_end = breakdown[1]
                break
        return is_break, break_end

    def get_break_end_time(self):
        seq: list = self._calendar.breakdown_seq
        seq_end: list = [tup[1] for tup in seq if tup[1] >= comUtility.Utility.runtime]
        break_end = min(seq_end)
        return break_end

    def power_on(self):
        if self.Status != "DOWN":
            pass
        else:
            if self.Lot is None:
                self.Status = "IDLE"
                self.StartTime = None
                self.EndTime = None

    def _is_overlapping_to_break(self, from_to_tuple: tuple,
                                 start_time: datetime.datetime,
                                 end_time: datetime.datetime):
        is_overlapping: bool = self._is_between(from_to_tuple, start_time) or \
                               self._is_between(from_to_tuple, end_time) or \
                               self._is_including(from_to_tuple, start=start_time, end=end_time)
        # if is_overlapping:
        #     self.StartTime = from_to_tuple[0]
        #     self.EndTime = from_to_tuple[1]
        #     self.Status = "DOWN"
        # else:
        #     self.StartTime = None
        #     self.EndTime = None
        #     self.Status = "IDLE"
        return is_overlapping

    def _is_including(self, from_to_tuple: tuple, start: datetime, end: datetime):
        is_including: bool = start <= from_to_tuple[0] and from_to_tuple[1] <= end
        return is_including

    def _is_between(self, from_to_tuple: tuple, value: datetime):
        is_between: bool = from_to_tuple[0] < value < from_to_tuple[1]
        return is_between

    def _set_status(self, status: str):
        self.Status = status

    def _set_start_time(self, start_time: datetime.datetime = None):
        self.StartTime = start_time

    def _set_end_time(self, end_time: datetime.datetime = None):
        self.EndTime = end_time

    def _get_lot_proc_time(self, lot: objLot):
        duration: datetime.timedelta = \
            lot.ReactDuration if self._oper.Kind == "REACTOR" else \
            lot.PackDuration
        return duration

