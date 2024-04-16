from expectations.commands import run_converted_resource_checkpoint


class PostConversionPhase:
    def __init__(
        self,
        converted_resource_path,
        output_dir,
        dataset,
        typology,
        act_on_critical_error=False,
    ):
        """
        Initializes the PostConversionPhase with necessary parameters.
        :param converted_resource_path: Path to the converted CSV file.
        :param output_dir: Directory to store output files.
        :param dataset: Dataset related information for the checkpoint.
        :param typology: Typology information for the checkpoint.
        :param act_on_critical_error: Whether to act on critical errors during the checkpoint.
        """
        self.converted_resource_path = converted_resource_path
        self.output_dir = output_dir
        self.dataset = dataset
        self.typology = typology
        self.act_on_critical_error = act_on_critical_error

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
