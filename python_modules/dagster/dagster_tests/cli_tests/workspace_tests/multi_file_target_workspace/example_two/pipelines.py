# mypy: disable-error-code=attr-defined
from solids import example_two_solid  # pylint: disable=no-name-in-module

from dagster._legacy import pipeline


@pipeline
def example_two_pipeline():
    example_two_solid()
