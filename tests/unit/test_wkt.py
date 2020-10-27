import platform

from digital_land.datatype.wkt import WktDataType
from digital_land.issues import Issues

running_on_macos = platform.system() == "Darwin"


def test_wkt_normalise_osgb_multipolygon():
    issues = Issues()
    wkt = WktDataType()
    value = "MULTIPOLYGON (((180639.012 33243.8609999996,180668.2981 33194.8710999992,180707.5521 33152.8077000007, 180639.012 33243.8609999996)))"

    if running_on_macos:
        expected = (
            "MULTIPOLYGON (((-5.072464303134637 50.158564820564, -5.072026691725868 50.1581357012866, "
            "-5.071453739250331 50.15777247284136, -5.072464303134637 50.158564820564)))"
        )
    else:
        expected = (
            "MULTIPOLYGON (((-5.072464303134637 50.158564820564, -5.072026691725867 50.1581357012866, "
            "-5.071453739250332 50.15777247284136, -5.072464303134637 50.158564820564)))"
        )

    assert wkt.normalise(value, issues=issues) == expected
