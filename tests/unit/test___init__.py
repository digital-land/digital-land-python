from digital_land import __version__


def test___version___is_defined():

    assert __version__ is not None
    assert isinstance(__version__, str)
    assert len(__version__) > 0
