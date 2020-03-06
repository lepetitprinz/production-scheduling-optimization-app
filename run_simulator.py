# -*- coding: utf-8 -*-

from M01_Simulator.PE_Simulator import Simulator


def run_simulator():
    simul: Simulator = Simulator()
    flag = simul.SetupDbObject()

    # Simul 구동
    simul.run_simulator()

    print("\nSIMULATION END")


if __name__ == '__main__':
    run_simulator()
