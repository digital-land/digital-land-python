from digital_land.datatype.wkt import WktDataType
from digital_land.issues import Issues


def test_wkt_normalise_osgb_multipolygon():
    issues = Issues()
    wkt = WktDataType()
    value = "MULTIPOLYGON (((180639.012 33243.8609999996,180668.2981 33194.8710999992,180707.5521 33152.8077000007, 180639.012 33243.8609999996)))"
    expected = "MULTIPOLYGON (((-5.072464 50.158565,-5.072027 50.158136,-5.071454 50.157772,-5.072464 50.158565)))"
    assert wkt.normalise(value, issues=issues) == expected


def test_wkt_normalise_osm_multipolygon():
    issues = Issues()
    wkt = WktDataType()
    value = "MULTIPOLYGON (((-7946.4687 6701859.138,-7925.9829 6701856.258,-7926.5076 6701852.447,-7946.5692 6701855.314,-7946.4687 6701859.138)))"
    expected = "MULTIPOLYGON (((-0.071384 51.453226,-0.071200 51.453210,-0.071205 51.453189,-0.071385 51.453205,-0.071384 51.453226)))"
    assert wkt.normalise(value, issues=issues) == expected
