class Constraint:
    def __init__(
        self,
        constraint_key,
        criteria_key,
        criteria_name,
        prod_color_dim_ky,
        upper_bound,
        lower_bound,
        centile_pct,
        slot_type_cd,
        table_name,
        column_name,
        active,
    ):
        self._constraint_key = constraint_key
        self._criteria_key = criteria_key
        self._criteria_nm = criteria_name
        self._prod_color_dim_ky = prod_color_dim_ky
        self._upper_bound = upper_bound
        self._lower_bound = lower_bound
        self._centile_pct = centile_pct
        self._slot_type_cd = slot_type_cd
        self._table_name = table_name
        self._column_name = column_name
        self._active = active

    @property
    def constraint_key(self):
        return self._constraint_key

    @constraint_key.setter
    def constraint_key(self, value):
        self._constraint_key = value

    @property
    def prod_color_dim_ky(self):
        return self._prod_color_dim_ky

    @prod_color_dim_ky.setter
    def prod_color_dim_ky(self, value):
        self._prod_color_dim_ky = value

    @property
    def upper_bound(self):
        return self._upper_bound

    @upper_bound.setter
    def upper_bound(self, value):
        self._upper_bound = value

    @property
    def lower_bound(self):
        return self._lower_bound

    @lower_bound.setter
    def lower_bound(self, value):
        self._lower_bound = value

    @property
    def centile_pct(self):
        return self._centile_pct

    @centile_pct.setter
    def centile_pct(self, value):
        self._centile_pct = value

    @property
    def slot_type_cd(self):
        return self._slot_type_cd

    @slot_type_cd.setter
    def slot_type_cd(self, value):
        self._slot_type_cd = value

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    @property
    def column_name(self):
        return self._column_name

    @column_name.setter
    def column_name(self, value):
        self._column_name = value

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, value):
        self._active = value

    @property
    def criteria_key(self):
        return self._criteria_key

    @criteria_key.setter
    def criteria_key(self, value):
        self._criteria_key = value

    @property
    def criteria_name(self):
        return self._criteria_nm

    @criteria_name.setter
    def criteria_name(self, value):
        self._criteria_nm = value

    def get_dict(self):
        return {
            "constraint_key": self._constraint_key,
            "criteria_key": self._criteria_key,
            "criteria_name": self._criteria_nm,
            "prod_color_dim_ky": self._prod_color_dim_ky,
            "upper_bound": self._upper_bound,
            "lower_bound": self._lower_bound,
            "centile_pct": self._centile_pct,
            "slot_type_cd": self._slot_type_cd,
            "table_name": self._table_name,
            "column_name": self._column_name,
            "active": self._active,
        }
