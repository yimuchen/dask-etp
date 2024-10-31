import os
import warnings
from typing import List, Optional

import dask
from dask_jobqueue import HTCondorCluster
from dask_jobqueue.htcondor import HTCondorJob


class ETPCondorJob(HTCondorJob):
    """
    Additional configuration that one should use to construct the JDL file
    """

    config_name = "kitetp"

    def __init__(self, scheduler=None, name=None, disk=None, **base_class_kwargs):
        """
        Custom methods for over-writing the dask classes
        """

        warnings.simplefilter(action="ignore", category=FutureWarning)
        super().__init__(scheduler=scheduler, name=name, disk=disk, **base_class_kwargs)
        warnings.resetwarnings()


class ETPCondorCluster(HTCondorCluster):
    __doc__ = (
        HTCondorCluster.__doc__
        + """
        A customized :class:`dask_jobqueue.HTCondorCluster` subclass for
        spawning Dask workers in the KIT-ETP computing cluster HTCondor queue.
        pool. Notice that the common defaults can be found in the accompanying
        `./jobqueue-kitetp.yaml` file.
        """
    )
    config_name = ETPCondorJob.config_name
    job_cls = ETPCondorJob

    def __init__(
        self,
        docker_image: Optional[str] = None,
        cvmfs_image: Optional[str] = None,
        transfer_input_files: Optional[List[str]] = None,
        ship_env=True,
        gpus=None,
        **kwargs,
    ):
        """
        - :param: docker_image: The docker image to pull from dockerhub
        - :param: cvmfs_image: The container image to use from cvmfs
        - :param: gpus: The number of GPUs to request.  Defaults to ``0``.
        - :param: ship_env: Whether or not to pass the full python environment over to the worker
        - :param kwargs: Additional keyword arguments to pass to the underlying HTCondorCluster
        """

        # Modifications to job_extra_directive - For docker/singularity images
        img_job_extra = {"universe": "vanilla"}
        if docker_image and cvmfs_image:
            raise AssertionError("Cannot specify both docker image and cvmfs image")
        elif docker_image:
            img_job_extra.update(
                {"universe": "docker", "docker_image": f'"{docker_image}"'}
            )
        elif cvmfs_image:
            img_job_extra.update({"MY.SingularityImage": f'"{cvmfs_image}"'})

        # Adding paths required to be transferred to the remote worker
        transfer_job_extra = {}
        if transfer_input_files is None:
            transfer_input_files = []
        if ship_env and os.getenv("VIRTUAL_ENV"):
            transfer_input_files += os.getenv("VIRTUAL_ENV")
        if len(transfer_input_files):
            transfer_job_extra = {
                "transfer_input_files": ",".join(transfer_input_files)
            }

        # Setting up the master kwargs to be passed to the underlying constructor.
        # Allow any option to be overwritten by the user if requested
        job_extra = dask.config.get(
            f"jobqueue.{ETPCondorCluster.config_name}.job_extra_directives"
        )
        job_extra.update(kwargs.get("job_extra_directives", {}))
        job_extra.update(img_job_extra)
        job_extra.update(transfer_job_extra)
        kwargs["job_extra_directives"] = job_extra

        # Updating the worker
        python_config = f"jobqueue.{ETPCondorCluster.config_name}.python"
        print(dask.config.get(python_config))
        if ship_env and os.getenv("VIRTUAL_ENV"):
            python_path = os.getenv("VIRTUAL_ENV") + "/bin/python"
            dask.config.set({python_config: python_path})
        print(dask.config.get(python_config))

        # Running constructor with warnings suppressed
        warnings.simplefilter(action="ignore", category=FutureWarning)
        super().__init__(**kwargs)
        warnings.resetwarnings()
