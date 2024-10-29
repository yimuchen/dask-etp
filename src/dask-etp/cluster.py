import logging
import re
import sys
import warnings
from collections import ChainMap
from typing import Optional

import dask
from dask_jobqueue import HTCondorCluster
from dask_jobqueue.htcondor import HTCondorJob


class ETPCondorJob(HTCondorJob):
    """
    Additional configuration that one should use to construct the JDL file
    """

    config_name = "kit-etp"

    def __init__(self, scheduler=None, name=None, disk=None, **base_class_kwargs):
        """
        Custom methods for over-writing the dask classes
        """

        warnings.simplefilter(action="ignore", category=FutureWarning)
        super().__init__(scheduler=scheduler, name=name, disk=disk, **base_class_kwargs)
        warnings.resetwarnings()


class CernCluster(HTCondorCluster):
    __doc__ = (
        HTCondorCluster.__doc__
        + """
    A customized :class:`dask_jobqueue.HTCondorCluster` subclass for spawning Dask workers in the CERN HTCondor pool

    It provides the customizations and submit options required for the CERN pool.
    
    Additional CERN parameters:
    worker_image: The container to run the Dask workers inside. Defaults to: 
    ``"/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/batch-team/dask-lxplus/lxdask-cc7:latest"``
    container_runtime: If a container runtime is not required, choose ``none``, otherwise ``singularity`` (the default) 
    or ``docker``. If using ``lcg`` it shouldn't be necessary as long as the scheduler side matches the client, which 
    at CERN means the lxplus version corresponding to the lxbatch version.
    batch_name: The HTCondor JobBatchName assigned to the worker jobs. The default ends up as ``"dask-worker"``
    lcg: If set to ``True`` will use the LCG environment in CVMFS and use that to run the python interpreter on server 
    and client. Needs to be sourced before running the python interpreter. Defaults to False. 
    """
    )
    config_name = ETPCondorJob.config_name
    job_cls = ETPCondorJob

    def __init__(
        self,
        docker_image: Optional[str] = None,
        cvmfs_image: Optional[str] = None,
        gpus=None,
        **kwargs,
    ):
        """
        - :param: docker_image: The docker image to pull from dockerhub
        - :param: cvmfs_image: The container image to use from cvmfs
        - :param: gpus: The number of GPUs to request.  Defaults to ``0``.
        - :param kwargs: Additional keyword arguments to pass to the underlying HTCondorCluster
        """

        # Modifications to job_extra_directive - For docker/singularity images
        job_extra = {"universe": "vanilla"}
        if docker_image and cvmfs_image:
            raise AssertionError("Cannot specify both docker image and cvmfs image")
        elif docker_image:
            job_extra.update(
                {"universe": "docker", "docker_image": f'"{docker_image}"'}
            )
        elif cvmfs_image:
            job_extra.update({"MY.SingularityImage": f'"{cvmfs_image}"'})

        # Modifications to job_extra_directive - For GPUs?
        # TODO: Implement???

        # Common modifications to ETP TODO: Is this needed for ETP?
        job_extra.update({"MY.IsDaskWorker": "true"})

        # Modifications to worker_args
        worker_args = kwargs.get("worker_extra_args", [])
        worker_args.extend(["--worker-port", "10000:10100"])

        # Setting up the master kwargs to be passed to the underlying constructor
        kwargs.setdefault("job_extra_directives", job_extra)
        kwargs["worker_extra_args"] = worker_args

        # Running constructor with warnings suppressed
        warnings.simplefilter(action="ignore", category=FutureWarning)
        super().__init__(**kwargs)
        warnings.resetwarnings()

