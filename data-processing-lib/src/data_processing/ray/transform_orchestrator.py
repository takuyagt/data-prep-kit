import time
import traceback
from datetime import datetime

import ray
from data_processing.data_access import DataAccessFactory
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    RayUtils,
    TransformOrchestratorConfiguration,
    TransformStatistics,
    TransformTableProcessor,
)
from data_processing.utils import ParamsUtils, get_logger
from ray.util import ActorPool
from ray.util.metrics import Gauge


logger = get_logger(__name__)


@ray.remote(num_cpus=1, scheduling_strategy="SPREAD")
def orchestrate(
    preprocessing_params: TransformOrchestratorConfiguration,
    data_access_factory: DataAccessFactory,
    transform_runtime_config: DefaultTableTransformConfiguration,
) -> int:
    """
    orchestrator for transformer execution
    :param preprocessing_params: orchestrator configuration
    :param data_access_factory: data access factory
    :param transform_runtime_config: transformer runtime configuration
    :return: 0 - success or 1 - failure
    """
    start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"orchestrator started at {start_ts}")
    try:
        # create data access
        data_access = data_access_factory.create_data_access()
        if data_access is None:
            logger.error("No DataAccess instance provided - exiting")
            return 1
        # Get files to process
        files, profile = data_access.get_files_to_process()
        if len(files) == 0:
            logger.error("No input files to process - exiting")
            return 0
        logger.info(f"Number of files is {len(files)}, source profile {profile}")
        # Print interval
        print_interval = int(len(files) / 100)
        if print_interval == 0:
            print_interval = 1
        # Get Resources for execution
        resources = RayUtils.get_cluster_resources()
        logger.info(f"Cluster resources: {resources}")
        # print execution params
        logger.info(
            f"Number of workers - {preprocessing_params.n_workers} " f"with {preprocessing_params.worker_options} each"
        )
        # create transformer runtime
        runtime = transform_runtime_config.create_transform_runtime()
        # create statistics
        statistics = TransformStatistics.remote({})
        # create executors
        processor_params = {
            "data_access_factory": data_access_factory,
            "transform_class": transform_runtime_config.get_transform_class(),
            "transform_params": runtime.get_transform_config(
                data_access_factory=data_access_factory, statistics=statistics, files=files
            ),
            "statistics": statistics,
        }
        logger.info("Creating actors")
        processors = RayUtils.create_actors(
            clazz=TransformTableProcessor,
            params=processor_params,
            actor_options=preprocessing_params.worker_options,
            n_actors=preprocessing_params.n_workers,
            creation_delay=preprocessing_params.creation_delay,
        )
        processors_pool = ActorPool(processors)
        # create gauges
        files_in_progress_gauge = Gauge("files_in_progress", "Number of files in progress")
        files_completed_gauge = Gauge("files_processed_total", "Number of files completed")
        available_cpus_gauge = Gauge("available_cpus", "Number of available CPUs")
        available_gpus_gauge = Gauge("available_gpus", "Number of available GPUs")
        available_memory_gauge = Gauge("available_memory", "Available memory")
        available_object_memory_gauge = Gauge("available_object_store", "Available object store")
        # process data
        logger.info("Begin processing files")
        RayUtils.process_files(
            executors=processors_pool,
            files=files,
            print_interval=print_interval,
            files_in_progress_gauge=files_in_progress_gauge,
            files_completed_gauge=files_completed_gauge,
            available_cpus_gauge=available_cpus_gauge,
            available_gpus_gauge=available_gpus_gauge,
            available_memory_gauge=available_memory_gauge,
            object_memory_gauge=available_object_memory_gauge,
        )
        logger.info("Done processing files, waiting for flush() completion.")
        # invoke flush to ensure that all results are returned
        start = time.time()
        replies = [processor.flush.remote() for processor in processors]
        RayUtils.wait_for_execution_completion(replies)
        logger.info(f"done flushing in {time.time() - start} sec")
        # Compute execution statistics
        logger.info("Computing execution stats")
        stats = runtime.compute_execution_stats(ray.get(statistics.get_execution_stats.remote()))

        # build and save metadata
        logger.info("Building job metadata")
        cleaned_tranform_metadata = transform_runtime_config.get_transform_metadata()
        cleaned_tranform_metadata = ParamsUtils.hide_secrets(cleaned_tranform_metadata)
        # logger.info(f"daf input params: {data_access_factory.get_input_params()}")
        metadata = {
            "pipeline": preprocessing_params.pipeline_id,
            "job details": preprocessing_params.job_details
            | {"start_time": start_ts, "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "status": "success"},
            "code": preprocessing_params.code_location,
            "job_input_params": cleaned_tranform_metadata
            | data_access_factory.get_input_params()
            | preprocessing_params.get_input_params(),
            "execution_stats": resources,
            "job_output_stats": stats,
        }
        logger.debug(f"Saved job metadata: {metadata}.")
        data_access.save_job_metadata(metadata)
        logger.info("Saved job metadata.")
        return 0
    except Exception as e:
        logger.error(f"Exception during execution {e}: {traceback.print_exc()}")
        return 1
