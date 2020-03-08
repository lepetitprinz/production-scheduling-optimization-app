# -*- coding: utf-8 -*-

import datetime

from M03_Site import simFactoryMgr
from M05_ProductManager import objLot


class Machine(object):
    def __init__(self, factory: simFactoryMgr, mac_id: str):
        self._factory: simFactoryMgr = factory
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


    def setup_object(self, status: str, uom: str = ""):
        self.Status = status

        # PACKAGING MACHINE PROPERTIES
        self.Uom = uom

    def RunMachine(self):
        if self.Lot is not None:
            self._set_status("PROC")

    def _set_status(self, status: str):
        self.Status = status
