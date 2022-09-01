#!/usr/bin/env python3
# Run gridpack to NANO on condor

import os
import sys
import subprocess
import socket
import json

tarball_dir = "/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/tarballs/"
tarball_path = f"{tarball_dir}/usercode.tar.gz"
def make_tarball():
    os.system(f"tar -czvf {tarball_dir}/usercode.tar.gz -C /afs/cern.ch/user/d/dryu/workspace/snowmass/MC/rivet batch env.sh \
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
    args = parser.parse_args()

    if args.retar:
        make_tarball()

    # Create and move to working directory
    csub_dir = "jobs/{}".format(args.dataset)
    if os.path.isdir(csub_dir) and not args.overwrite:
        raise ValueError("Working directory {} already exists! Specify -f to overwrite".format(csub_dir))
    os.system("mkdir -pv {}".format(csub_dir))
    cwd = os.getcwd()
    os.chdir("{}".format(csub_dir))

    # Input files
    with open("/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/rivet/batch/yodamerged_index.json", "r") as f:
        yoda_index = json.load(f)
        input_files = yoda_index[args.dataset]

    # Modify input files to find once transferred to condor
    local_input_files = [f"$_CONDOR_SCRATCH_DIR/{os.path.basename(x)}" for x in input_files]

    # Submit to condor
    with open("run_script.sh", 'w') as run_script:
        run_script.write(f"""#!/bin/bash
echo "Starting run_script"
pwd
ls -lrth
mkdir work
cd work
echo "Contents of working directory:"
ls -lrth
mv $_CONDOR_SCRATCH_DIR/usercode.tar.gz .
tar -xzf usercode.tar.gz
source env.sh
#rivet-merge -e {' '.join(local_input_files)} -o merged.yoda
rivet-mkhtml {' '.join(local_input_files)} -o plots_{args.dataset}
tar -czvf plots_{args.dataset}.tar.gz plots_{args.dataset}
mv plots_{args.dataset}.tar.gz $_CONDOR_SCRATCH_DIR
echo "Job done"
echo "Contents of condor scratch dir:"
ls -lrth $_CONDOR_SCRATCH_DIR
""")

    files_to_transfer = [tarball_path]
    files_to_transfer.extend(input_files)
    csub_command = "/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/rivet/batch/csub run_script.sh -t tomorrow --mem {} --nCores {} -F {}".format(
                        4000,
                        2, 
                        ",".join(files_to_transfer), 
                        ) # 
    os.system(csub_command)

    os.chdir(cwd)