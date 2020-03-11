# -*- coding: utf-8 -*-

from M01_Simulator.PE_Simulator import Simulator


def run_simulator(source: str, year: int, month: int, day: int, day_start_time: str, horizon_days: int, silo_qty: int =4000, nof_silo: int =10, silo_wait_hours: int = 0):
    simul: Simulator = Simulator()
    flag = simul.SetupDbObject(source=source, year=year, month=month, day=day, day_start_time=day_start_time, horizon_days=horizon_days, silo_qty=silo_qty, nof_silo=nof_silo, silo_wait_hours=silo_wait_hours)

    # Simulation 구동
    simul.run_simulator()
    print("\nSIMULATION END")

    # Simulation 가동 종료 후 Scheduling 결과 저장
    # simul.SaveSimulData()


if __name__ == '__main__':
    run_simulator(source="db", year=2020, month=3, day=1, day_start_time="08:00:00", horizon_days=31, silo_qty=4000, nof_silo=10, silo_wait_hours=12)
