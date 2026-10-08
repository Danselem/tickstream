"""Quality package: Great Expectations validation over a local bronze scan."""

from pipeline.quality.gx_runner import QualityResult, run_quality

__all__ = ["QualityResult", "run_quality"]
