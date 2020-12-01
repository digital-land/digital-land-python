import pluggy

hookspec = pluggy.HookspecMarker("digital-land")


class HarmoniserPlugin:
    @hookspec
    def init_harmoniser_plugin(self, harmoniser):
        """Perform any initialisation required by harmoniser plugins

        :param harmoniser: the harmoniser instance
        """

    @hookspec
    def apply_patch_post(self, fieldname, value):
        """Hook to run at the end of apply_patch


        :param fieldname: the field to patch
        :param value: current value of the field to patch
        """

    @hookspec
    def set_resource_defaults_post(self, resource: str):
        """Set default values for use while harmonising a resource

        :param resource: resource hash
        """
