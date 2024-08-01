import os
import sys
import logging
from pathlib import Path
from argparse import ArgumentParser

logging.basicConfig(level=logging.INFO)

from Pegasus.api import *

class LDAWTWorkflow():
    wf = None
    sc = None
    tc = None
    rc = None
    props = None

    dagfile = None
    wf_name = None
    wf_dir = None

    def __init__(self, dagfile="workflow.yml"):
        self.dagfile = dagfile
        self.wf_name = "ldawt"
        self.wf_dir = str(Path(__file__).parent.resolve())

    def write(self):
        if not self.sc is None:
            self.sc.write()
        self.props.write()
        self.rc.write()
        self.tc.write()
        self.wf.write()

    def create_pegasus_properties(self):
        self.props = Properties()

        #self.props["pegasus.mode"] = "development"
        #self.props["pegasus.mode"] = "debug"
        #self.properties["pegasus.integrity.checking"] = "none"
        return
    
    def create_sites_catalog(self, exec_site_name="condorpool"):
        self.sc = SiteCatalog()

        shared_scratch_dir = os.path.join(self.wf_dir, "scratch")
        local_storage_dir = os.path.join(self.wf_dir, "output")

        local = Site("local")\
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, shared_scratch_dir)
                            .add_file_servers(FileServer("file://" + shared_scratch_dir, Operation.ALL)),
                        Directory(Directory.LOCAL_STORAGE, local_storage_dir)
                            .add_file_servers(FileServer("file://" + local_storage_dir, Operation.ALL))
                    )
        
        exec_site = Site(exec_site_name)\
                        .add_pegasus_profile(style="condor")\
                        .add_condor_profile(universe="vanilla")\
                        .add_profiles(Namespace.PEGASUS, key="data.configuration", value="condorio")
        
        self.sc.add_sites(local, exec_site)

    def create_transformation_catalog(self, exec_site_name="condorpool"):
        self.tc = TransformationCatalog()

        SplitParquet = Transformation(
            "SplitParquet",
            site=exec_site_name,
            pfn=os.path.join(self.wf_dir, 'bin/SplitParquet'),
            is_stageable=True
        )

        Download = Transformation(
            "Download",
            site=exec_site_name,
            pfn=os.path.join(self.wf_dir, 'bin/Download'),
            is_stageable=True
        )

        self.tc.add_transformations(SplitParquet, Download)

    def create_replica_catalog(self):
        self.rc = ReplicaCatalog()

        self.rc.add_replica("local", "hundred_AIIRA.csv", os.path.join(self.wf_dir, "input", "hundred_AIIRA.csv"))

    def create_workflow(self):
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        # Define input and output files
        aiira = File("hundred_AIIRA.csv")

        splitNum = 10
        for i in range(1, splitNum+1):
            groupFile = File(f'group{i}.parquet')
            splitParquetJob = Job("SplitParquet")\
                                .add_args("--file", aiira,
                                          "--grouping_col", "name",
                                          "--groups", str(splitNum),
                                          "--output_file", "output")\
                                .add_inputs(aiira)\
                                .add_outputs(groupFile, stage_out=True, register_replica=False)
            self.wf.add_jobs(splitParquetJob)
        # group1_input = "group1"
        # group1_tar = File("group1.tar.gz")
        # download_job = Job("Download")\
        #                 .add_args("--input_path", output_tar,
        #                             "--output_folder", "group1",
        #                             "--url_name", "photo_url",
        #                             "--class_name", "name",
        #                             "--download_number", "1")\
        #                 .add_inputs(output_tar)\
        #                 .add_outputs(group1_tar, stage_out=True, register_replica=True)\
        #                 .add_profiles(Namespace.CONDOR, key="request_memory", value="200")
        # Add the job to the workflow
        
    

if __name__ == '__main__':
    parser = ArgumentParser(description="Pegasus Diamond Workflow")

    parser.add_argument("-s", "--skip_sites_catalog", action="store_true", help="Skip site catalog creation")
    parser.add_argument("-e", "--execution_site_name", metavar="STR", type=str, default="condorpool", help="Execution site name (default: condorpool)")
    parser.add_argument("-o", "--output", metavar="STR", type=str, default="workflow.yml", help="Output file (default: workflow.yml)")

    args = parser.parse_args()
    
    workflow = LDAWTWorkflow(args.output)

    if not args.skip_sites_catalog:
        print("Creating execution sites...")
        workflow.create_sites_catalog(args.execution_site_name)

    print("Creating workflow properties...")
    workflow.create_pegasus_properties()
    
    print("Creating transformation catalog...")
    workflow.create_transformation_catalog(args.execution_site_name)

    print("Creating replica catalog...")
    workflow.create_replica_catalog()

    print("Creating diamond workflow dag...")
    workflow.create_workflow()

    workflow.write()
