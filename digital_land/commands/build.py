from digital_land.package.organisation import OrganisationPackage


def organisation_create(**kwargs):
    package = OrganisationPackage(**kwargs)
    package.create()


def organisation_check(**kwargs):
    output_path = kwargs.pop("output_path")
    lpa_path = kwargs.pop("lpa_path")
    package = OrganisationPackage(**kwargs)
    package.check(lpa_path, output_path)
