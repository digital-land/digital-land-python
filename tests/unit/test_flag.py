from digital_land.datatype.flag import FlagDataType


def test_flag_normalise():
    flag = FlagDataType()

    assert flag.normalise("yes") == "yes"
    assert flag.normalise("yEs") == "yes"
    assert flag.normalise("no") == "no"
    assert flag.normalise("No") == "no"


def test_flag_normalise_lookup():
    flag = FlagDataType()

    assert flag.normalise("y") == "yes"
    assert flag.normalise("Y") == "yes"
    assert flag.normalise("n") == "no"
    assert flag.normalise("N") == "no"


def test_flag_normalise_bad_value():
    flag = FlagDataType()

    assert flag.normalise("blarg") == ""
