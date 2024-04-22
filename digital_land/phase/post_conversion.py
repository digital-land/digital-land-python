from digital_land.phase.phase import Phase
from ..expectations.commands import run_converted_resource_checkpoint


class PostConversionPhase(Phase):
    def __init__(
        self,
        output_dir,
        dataset,
        typology,
        act_on_critical_error=False,
    ):
        self.output_dir = output_dir
        self.dataset = dataset
        self.typology = typology
        self.act_on_critical_error = act_on_critical_error

    def process(self, stream=None):
        self.run(stream.f.name)
        return stream

    def run(self, converted_resource_path):
        """
        Executes the converted resource checkpoint using the provided parameters.
        """
        # Run the checkpoint on the converted resource
        run_converted_resource_checkpoint(
            converted_resource_path,
            self.output_dir,
            self.dataset,
            self.typology,
            self.act_on_critical_error,
        )
