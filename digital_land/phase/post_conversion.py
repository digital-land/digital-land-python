from ..expectations.commands import run_converted_resource_checkpoint


class PostConversionPhase:
    def __init__(
        self,
        converted_resource_path,
        output_dir,
        dataset,
        typology,
        act_on_critical_error=False,
    ):
        self.converted_resource_path = converted_resource_path
        self.output_dir = output_dir
        self.dataset = dataset
        self.typology = typology
        self.act_on_critical_error = act_on_critical_error

    def process(self):
        return self.run()

    def run(self):
        """
        Executes the converted resource checkpoint using the provided parameters.
        """
        # Run the checkpoint on the converted resource
        run_converted_resource_checkpoint(
            self.converted_resource_path,
            self.output_dir,
            self.dataset,
            self.typology,
            self.act_on_critical_error,
        )
