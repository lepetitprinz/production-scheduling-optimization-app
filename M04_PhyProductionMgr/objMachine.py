# -*- coding: utf-8 -*-

from M03_Site import simFactoryMgr


class Machine(object):
    def __init__(self, factory: simFactoryMgr, mac_id: str):
        self._factory: simFactoryMgr = factory
        self.id: str = mac_id

        self.status: str = "IDLE"

    def setup_object(self, status: str):
        self.status = status

    def RunMachine(self):
        pass
