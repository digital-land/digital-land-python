from digital_land.datatype.wkt import WktDataType
from digital_land.issues import Issues


def test_wkt_normalise_osgb_multipolygon():
    issues = Issues()
    wkt = WktDataType()
    value = "MULTIPOLYGON (((180639.012 33243.8609999996,180668.2981 33194.8710999992,180707.5521 33152.8077000007, 180639.012 33243.8609999996)))"
    expected = "MULTIPOLYGON (((-5.072464 50.158565,-5.072027 50.158136,-5.071454 50.157772,-5.072464 50.158565)))"

    assert wkt.normalise(value, issues=issues) == expected
