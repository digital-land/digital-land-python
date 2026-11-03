from io import StringIO

from digital_land.phase.load import LoadPhase
from digital_land.phase.parse import ParsePhase
from digital_land.phase.save import SavePhase
from digital_land.pipeline import run_pipeline


def TestPhase(phase, inputs):
    output = StringIO()
    run_pipeline(LoadPhase(f=StringIO(inputs)), ParsePhase(), phase, SavePhase(f=output))
    return output.getvalue()
