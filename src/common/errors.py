"""Pipeline/system exceptions.

Data-quality problems are NOT exceptions: those records go to quarantine with a
reason. Exceptions here represent pipeline or system failures (missing source
file, DB unreachable, bug) and must identify the stage and run that failed.
"""


class PipelineError(Exception):
    """Base class for pipeline failures."""


class PipelineStageError(PipelineError):
    """A pipeline stage failed; carries the stage name and run id for diagnosis."""

    def __init__(self, stage: str, run_id: str | None, cause: BaseException):
        self.stage = stage
        self.run_id = run_id
        self.cause = cause
        super().__init__(
            f"stage={stage} run_id={run_id} failed: {type(cause).__name__}: {cause}"
        )
