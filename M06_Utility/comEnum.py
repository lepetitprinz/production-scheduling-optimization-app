# -*- coding: utf-8 -*-

import enum


class RegexCollection(enum.Enum):
    day_start_time: str = r'[0-2][0-9]:[0-5][0-9]:[0-5][0-9]'
    lot_id: str = r'[A-Z]*[_][0-9]/[A-Z][0-9]/[a-zA-Z]*'

class LotConfiguration(enum.Enum):
    minSize: int = 50
    maxSize: int = 400
