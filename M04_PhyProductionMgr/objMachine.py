# -*- coding: utf-8 -*-

from M03_Site import simFactoryMgr
from M05_ProductManager import objLot


class Machine(object):
    def __init__(self, factory: simFactoryMgr, mac_id: str):
        self._factory: simFactoryMgr = factory
        self.Id: str = mac_id

        # PACKAGING MACHINE PROPERTIES
        self.Uom: str = ""          # 25 KG / 750 KG / BULK

        # STATUS
        self.status: str = "IDLE"

        # CURRENT PROCESSING LOT
        self.Lot: objLot.Lot = None

    def setup_object(self, status: str, uom: str = ""):
        self.status = status

        # PACKAGING MACHINE PROPERTIES
        self.Uom = uom

    def RunMachine(self):
        pass
