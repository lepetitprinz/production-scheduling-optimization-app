# -*- coding: utf-8 -*-

from M01_Simulator.PE_Simulator import Simulator


def run_simulator(year: int, month: int, day: int, day_start_time: str, horizon_days: int):
    simul: Simulator = Simulator()
    flag = simul.SetupDbObject(year=year, month=month, day=day, day_start_time=day_start_time, horizon_days=horizon_days, silo_qty=4000, nof_silo=10)

    # Simul 구동
    simul.run_simulator()

    print("\nSIMULATION END")


if __name__ == '__main__':
    run_simulator(year=2020, month=3, day=1, day_start_time="08:00:00", horizon_days=92)
