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

        # PACKAGING MACHINE PROPERTIES
        self.Uom: str = ""          # 25 KG / 750 KG / BULK
        self.PackKind: str = ""     # WV / FS / BK / SB

        # STATUS
        self.Status: str = "IDLE"   # IDLE / PROC
        self.StartTime: datetime.datetime = None
        self.EndTime: datetime.datetime = None

        # CURRENT PROCESSING LOT
        self.Lot: objLot.Lot = None

    def setup_object(self, status: str, uom: str = "",
                     work_start_hour: int = None, work_end_hour: int = None):
        self.Status = status

        # PACKAGING MACHINE PROPERTIES
        self.Uom = uom

        # Setup Calendar
        self._calendar = comCalMgr.CalendarManager()
        self._calendar.SetupObject(factory=self._factory, machine=self,
                                   start_date=comUtility.Utility.DayStartDate,
                                   end_date=comUtility.Utility.DayEndDate,
                                   start_hour=work_start_hour, end_hour=work_end_hour
                                   )

    def assign_lot(self, lot: objLot):
        self.Lot = lot
        self._set_start_time(comUtility.Utility.runtime)
        self.Lot.set_location(location=self)
        duration: datetime.timedelta = self._get_lot_proc_time(lot=lot)
        endTime = self.StartTime + duration
        self._set_end_time(end_time=endTime)

    def lot_leave(self):
        leaving_lot: objLot.Lot = self.Lot
        self.Lot = None
        self._set_start_time()
        self._set_end_time()
        self._set_status(status="IDLE")
        return leaving_lot

    def RunMachine(self):
        if self.Lot is not None:
            self._set_status("PROC")

    def chk_breakdown(self):
        is_break: bool = False
        if self._calendar is None:
            return is_break
        for breakdown in self._calendar.breakdown_seq

    def _set_status(self, status: str):
        self.Status = status

    def _set_start_time(self, start_time: datetime.datetime = None):
        self.StartTime = start_time

    def _set_end_time(self, end_time: datetime.datetime = None):
        self.EndTime = end_time

    def _get_lot_proc_time(self, lot: objLot):
        duration: datetime.timedelta = \
            self.Lot.ReactDuration if self._oper.Kind == "REACTOR" else \
            self.Lot.PackDuration
        return duration

