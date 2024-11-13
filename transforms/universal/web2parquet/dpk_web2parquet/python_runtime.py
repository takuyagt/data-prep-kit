import time

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.runtime.pure_python.runtime_configuration import (
    PythonTransformRuntimeConfiguration,
)
from data_processing.utils import get_logger
from dpk_web2parquet.config import Web2ParquetTransformConfiguration


logger = get_logger(__name__)


class Web2ParquetPythonTransformConfiguration(PythonTransformRuntimeConfiguration):
    """
    Implements the PythonTransformConfiguration for NOOP as required by the PythonTransformLauncher.
    NOOP does not use a RayRuntime class so the superclass only needs the base
    python-only configuration.
    """

    def __init__(self):
        """
        Initialization
        :param base_configuration - base configuration class
        """
        super().__init__(transform_config=Web2ParquetTransformConfiguration())


if __name__ == "__main__":
    launcher = PythonTransformLauncher(Web2ParquetPythonTransformConfiguration())
    logger.info("Launching web2parquet transform")
    launcher.launch()