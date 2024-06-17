import pandas as pd
from pytest import raises

from tsdat.qc.checkers.oceanography import CheckGoringNikora2002


class TestCheckGoringNikora2002:
    test_data = pd.read_csv(
        "./test/qc/check_goring_nikora_2002/gn2002_test_data.csv"
    ).to_xarray()
    test_col = "water_level"

    def test_defaults(self):
        checker = CheckGoringNikora2002()
        checker_result = checker.run(self.test_data, self.test_col)
        assert checker_result.sum() == 8635
        assert len(self.test_data[self.test_col]) == len(checker_result)

    def test_wrong_var_name(self):
        checker = CheckGoringNikora2002()
        with raises(KeyError):
            checker.run(self.test_data, "wrong_var_name")
