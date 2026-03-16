import logging
import json

class TestType:
    ANY_KEY = "Any"

    def __init__(
        self,
        test_type_key,
        name,
        ink_type_dim_ky,
        arch_id,
        min_pen_ct,
        days_to_process_wo_ct,
        active,
        constraints,
        test_clou=False,
        test_hue=False,
    ):
        self.logger = logging.getLogger(__name__)
        self._test_type_key = test_type_key
        self._name = name
        self._ink_type_dim_ky = ink_type_dim_ky
        self._arch_id = arch_id
        self._min_pen_ct = min_pen_ct
        self._days_to_process_wo_ct = days_to_process_wo_ct
        self._active = active
        self._constraints = constraints
        self._test_clou = test_clou
        self._test_hue = test_hue
        self._constraints_by_criteria_key_sort_by_colors = {}
        # self.sort_constraints_by_criteria_key_sort_by_colors()

    @property
    def test_type_key(self):
        return self._test_type_key

    @test_type_key.setter
    def test_type_key(self, value):
        self._test_type_key = value

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def ink_type_dim_ky(self):
        return self._ink_type_dim_ky

    @ink_type_dim_ky.setter
    def ink_type_dim_ky(self, value):
        self._ink_type_dim_ky = value

    @property
    def arch_id(self):
        return self._arch_id

    @arch_id.setter
    def arch_id(self, value):
        self._arch_id = value

    @property
    def min_pen_ct(self):
        return self._min_pen_ct

    @min_pen_ct.setter
    def min_pen_ct(self, value):
        self._min_pen_ct = value

    @property
    def days_to_process_wo_ct(self):
        return self._days_to_process_wo_ct

    @days_to_process_wo_ct.setter
    def days_to_process_wo_ct(self, value):
        self._days_to_process_wo_ct = value

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, value):
        self._active = value

    @property
    def constraints(self):
        return self._constraints

    @constraints.setter
    def constraints(self, value):
        self._constraints = value
        self.sort_constraints_by_criteria_key_sort_by_colors()

    @property
    def test_clou(self):
        return self._test_clou

    @test_clou.setter
    def test_clou(self, value):
        self._test_clou = value

    @property
    def test_hue(self):
        return self._test_hue

    @test_hue.setter
    def test_hue(self, value):
        self._test_hue = value

    @property
    def constraints_by_criteria_key_sort_by_colors(self):
        return self._constraints_by_criteria_key_sort_by_colors

    def sort_constraints_by_criteria_key_sort_by_colors(self):
        self.logger.debug(
            f"DEBUG: Starting constraint sorting for TestType {self._test_type_key}, "
            f"ink_type_dim_ky: {self._ink_type_dim_ky}, arch_id: {self._arch_id}"
        )
        if self._constraints:
            # Log all raw constraint data first
            constraint_summary = []
            for key, constraint in self._constraints.items():
                constraint_summary.append(
                    f"Key: {key}, Criteria: {constraint.criteria_key}, "
                    f"Color: {constraint.prod_color_dim_ky}, Table: {constraint.table_name}"
                )
            self.logger.info(
                f"DEBUG: TestType {self._test_type_key} raw constraints before processing: "
                f"{len(self._constraints)} constraints found"
            )
            for summary in constraint_summary:
                self.logger.debug(f"DEBUG: Raw constraint - {summary}")
                
            self.logger.debug(
                f"DEBUG: Processing {len(self._constraints)} constraints: {list(self._constraints.keys())}"
            )
            for constraint in self._constraints.values():
                self.logger.debug(
                    f"DEBUG: Processing constraint {constraint.constraint_key} - "
                    f"criteria_key: {constraint.criteria_key}, "
                    f"prod_color_dim_ky: {constraint.prod_color_dim_ky}, "
                    f"table_name: {constraint.table_name}"
                )
                if (
                    constraint.criteria_key
                    not in self._constraints_by_criteria_key_sort_by_colors
                ):
                    self._constraints_by_criteria_key_sort_by_colors[
                        constraint.criteria_key
                    ] = {
                        "table_name": constraint.table_name,
                        "column_name": constraint.column_name,
                        "by_colors": {},
                    }

                prod_color_dim_ky = constraint.prod_color_dim_ky
                if prod_color_dim_ky is None:
                    prod_color_dim_ky = TestType.ANY_KEY
                    self.logger.debug(
                        f"DEBUG: Constraint {constraint.constraint_key} has NULL prod_color_dim_ky, "
                        f"setting to 'Any' fallback"
                    )

                if (
                    prod_color_dim_ky
                    not in self._constraints_by_criteria_key_sort_by_colors[
                        constraint.criteria_key
                    ]["by_colors"]
                ):
                    self._constraints_by_criteria_key_sort_by_colors[
                        constraint.criteria_key
                    ]["by_colors"][prod_color_dim_ky] = []

                self._constraints_by_criteria_key_sort_by_colors[
                    constraint.criteria_key
                ]["by_colors"][prod_color_dim_ky].append(constraint.constraint_key)
                
                self.logger.debug(
                    f"DEBUG: Added constraint {constraint.constraint_key} to color {prod_color_dim_ky} "
                    f"for criteria_key {constraint.criteria_key}"
                )

        # Log the final by_colors structure
        self.logger.debug(
            f"DEBUG: TestType {self._test_type_key} final constraints_by_criteria_key_sort_by_colors: "
            f"{json.dumps(self._constraints_by_criteria_key_sort_by_colors, indent=2)}"
        )
        
        # Log summary of colors per criteria key
        # for criteria_key, config in self._constraints_by_criteria_key_sort_by_colors.items():
        #     colors_list = list(config['by_colors'].keys())
        #     self.logger.info(
        #         f"DEBUG: TestType {self._test_type_key} - Criteria key {criteria_key}: "
        #         f"Table: {config['table_name']}, Available colors: {colors_list}, "
        #         f"Total colors: {len(colors_list)}, Has 'Any' fallback: {'Any' in colors_list}"
        #     )

    def __str__(self):
        return (
            f"TestType: test_type_key={self._test_type_key}, name={self._name}, ink_type_dim_ky={self._ink_type_dim_ky}, "
            f"arch_id={self._arch_id}, min_pen_ct={self._min_pen_ct}, days_to_process_wo_ct={self._days_to_process_wo_ct}, "
            f"active={self._active}, constraints={self._constraints}"
        )
