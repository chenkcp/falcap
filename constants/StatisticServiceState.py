class StatisticServiceState:
    PASS = "P"
    FAIL = "F"
    BLOCKED = "B"
    # NA is used when getting work orders
    NA = "NA"
    # SKIP is used when testing work orders
    SKIP = "SKIP"
    SKIP_CONSTRAINT = "SKIP_CONSTRAINT"
