#!/usr/bin/env python3
# Run gridpack to NANO on condor

import os
import sys
import subprocess
import socket
import json
import math

smmcdir = "/home/davidryu/MC/"
tarball_dir = f"/project/users/davidryu/tarballs/"
tarball_path = f"{tarball_dir}/usercode_rivet.tar.gz"

def make_tarball():
    os.system(f"tar -czvf {tarball_dir}/usercode_rivet.tar.gz -C {smmcdir}/rivet batch env.sh \
   --exclude='*.root*' \
   --exclude='*.tar.gz*' \
   --exclude='*/.html*' \
   --exclude='*.png*' \
   --exclude='*.pdf*' \
   --exclude='*ipynb ' \
   --exclude='batch/jobs'")

def make_proxy(proxy_path):
    os.system("voms-proxy-init -voms cms -out {} -valid 72:00".format(proxy_path))

def get_proxy_lifetime(proxy_path):
    import subprocess
    lifetime = float(subprocess.check_output("voms-proxy-info -timeleft -file {}".format(proxy_path), shell=True).strip())
    print("Proxy remaining lifetime: {}".format(lifetime))
    return lifetime

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run rivet-mkhtml and make-plots on condor")
    parser.add_argument("dataset", type=str, help="Name of dataset (see index_inputs.py for full list)")
    parser.add_argument("--retar", action="store_true", help="Retar usercode")
    parser.add_argument("--overwrite", "-f", action="store_true", help="Overwrite output folder")
    parser.add_argument("--analysis", "-a", type=str, help="Rivet analysis name")
    parser.add_argument("--files_per_job", "-n", type=int, default=1, help="Files per job")
    args = parser.parse_args()

    if args.retar:
        make_tarball()

    if "," in args.analysis:
        raise ValueError("--analysis only accepts one analysis at a time")

    # Create and move to working directory
    csub_dir = f"{smmcdir}/rivet/batch/jobs/rivet/{args.dataset}_{args.analysis}"
    if os.path.isdir(csub_dir) and not args.overwrite:
        raise ValueError("Working directory {} already exists! Specify -f to overwrite".format(csub_dir))
    os.system("mkdir -pv {}".format(csub_dir))
    cwd = os.getcwd()
    os.chdir("{}".format(csub_dir))

    # Input files
    with open(f"{smmcdir}/rivet/batch/pythia_index.json", "r") as f:
        input_index = json.load(f)
        input_files = input_index[args.dataset]

    # Subjob splitting
    nsubjobs = int(math.ceil(1. * len(input_files) / args.files_per_job))
    subjob_splitting = [input_files[ijob * args.files_per_job:(ijob+1)*args.files_per_job] for ijob in range(nsubjobs)]

    for ijob, subjob_files in enumerate(subjob_splitting):
        files_to_transfer = [tarball_path]
        files_to_transfer.extend(subjob_files)

        # Modify input files to find once transferred to condor
        local_subjob_files = [f"$_CONDOR_SCRATCH_DIR/{os.path.basename(x)}" for x in subjob_files]

        # Submit to condor
        with open(f"{csub_dir}/run_script{ijob}.sh", 'w') as run_script:
            run_script.write(f"""#!/bin/bash
    echo "Starting run_script"
    pwd
    ls -lrth
    mkdir work
    cd work
    echo "Contents of working directory:"
    ls -lrth
    mv $_CONDOR_SCRATCH_DIR/usercode_rivet.tar.gz .
    tar -xzf usercode_rivet.tar.gz
    source env.sh
    rivet --analysis {args.analysis} {' '.join(local_subjob_files)} -o rivet_subjob{ijob}.yoda
    gzip rivet_subjob{ijob}.yoda
    mv rivet_subjob{ijob}.yoda.gz $_CONDOR_SCRATCH_DIR

    cd $_CONDOR_SCRATCH_DIR
    mkdir hide
    mv *hepmc* hide

    echo "Job done"
    echo "Contents of condor scratch dir:"
    ls -lrth $_CONDOR_SCRATCH_DIR
    """)

        csub_command = f"{smmcdir}/rivet/batch/csub {csub_dir}/run_script{ijob}.sh \
-t tomorrow --mem {4000} --nCores {2} -F {','.join(files_to_transfer)}"
        os.system(csub_command)

    os.chdir(cwd)