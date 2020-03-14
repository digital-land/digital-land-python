from digital_land.datatype.address import AddressDataType


def test_address_normalise():
    address = AddressDataType()

    assert (
        address.normalise("11 Main Street,Town,  County")
        == "11 Main Street, Town, County"
    )
    assert (
        address.normalise('”11” "Main -  Street"\nTown;County')
        == "11 Main-Street, Town, County"
    )
