# -*- coding: utf-8 -*-

import enum


class RegexCollection(enum.Enum):
    day_start_time: str = '[0-2][0-9][:][0-5][0-9]:[0-5][0-9]'

