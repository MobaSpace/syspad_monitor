# coding: utf8

import enum


class PatientConst(enum.Enum):
    bed_data = "bed_actions"
    blood_press_data = "blood_press"
    sleep_data = "sleep"
    tracker_data = "tracker"

    BED_IN = 1
    BED_OUT = 0

    LYING = 0
    SITTING = 1
    STANDING = 2


class AlarmConst(enum.Enum):
    HIGH = 85  # >75
    MEDIUM = 65  # >=50 and <75
    LOW = 35  # >=25 and <50
    INFO = 20  # >=0 and <25
