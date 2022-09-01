# Merge yoda files with rivet-merge
# To avoid bad scaling with N(files), splits merge jobs recursively into fixed-size batches
# (Why do the authors of these merging utilities never do this themselves???)
import os
import sys
import glob
import json
from pprint import pprint
import copy
import math

def chunkify(lst, n):
    return [lst[i::n] for i in range(n)]


import argparse
parser = argparse.ArgumentParser(description="Run rivet-mkhtml and make-plots on condor")
parser.add_argument("dataset", type=str, help="Name of dataset (see index_inputs.py for full list)")
parser.add_argument("--reindex", action="store_true", help="Remake index of inputs")
#parser.add_argument("--overwrite", "-f", action="store_true", help="Overwrite output folder")
args = parser.parse_args()


v1p0pre7dir = "/collab/project/snowmass21/data/meloam/v2/demo/v1.0pre7/"
input_dirs = {
	"100TeV_BBB": f"{v1p0pre7dir}/100TeV_BBB.tar.gz", 
	"100TeV_BB": f"{v1p0pre7dir}/100TeV_BB.tar.gz", 
	"100TeV_B": f"{v1p0pre7dir}/100TeV_B.tar.gz", 
	"100TeV_H": f"{v1p0pre7dir}/100TeV_H.tar.gz", 
	"100TeV_LLB": f"{v1p0pre7dir}/100TeV_LLB.tar.gz", 
	"100TeV_LL": f"{v1p0pre7dir}/100TeV_LL.tar.gz", 
	"100TeV_tB": f"{v1p0pre7dir}/100TeV_tB.tar.gz", 
	"100TeV_t": f"{v1p0pre7dir}/100TeV_t.tar.gz", 
	"100TeV_ttB": f"{v1p0pre7dir}/100TeV_ttB.tar.gz", 
	"100TeV_tt": f"{v1p0pre7dir}/100TeV_tt.tar.gz", 
	"100TeV_vbf": f"{v1p0pre7dir}/100TeV_vbf.tar.gz", 
	"13TeV_BBB": f"{v1p0pre7dir}/13TeV_BBB.tar.gz", 
	"13TeV_BB": f"{v1p0pre7dir}/13TeV_BB.tar.gz", 
	"13TeV_B": f"{v1p0pre7dir}/13TeV_B.tar.gz", 
	"13TeV_H": f"{v1p0pre7dir}/13TeV_H.tar.gz", 
	"13TeV_LLB": f"{v1p0pre7dir}/13TeV_LLB.tar.gz", 
	"13TeV_LL": f"{v1p0pre7dir}/13TeV_LL.tar.gz", 
	"13TeV_tB": f"{v1p0pre7dir}/13TeV_tB.tar.gz", 
	"13TeV_t": f"{v1p0pre7dir}/13TeV_t.tar.gz", 
	"13TeV_ttB": f"{v1p0pre7dir}/13TeV_ttB.tar.gz", 
	"13TeV_tt": f"{v1p0pre7dir}/13TeV_tt.tar.gz", 
	"13TeV_vbf": f"{v1p0pre7dir}/13TeV_vbf.tar.gz", 
}

if args.reindex:
	index = {}
	for sample_name, sample_dir in input_dirs.items():
		index[sample_name] = glob.glob(f"{sample_dir}/rivet/*yoda.gz")
		index[sample_name] = [x for x in index[sample_name] if os.path.getsize(x) > 0]
	with open("merge_index.json", "w") as f:
		json.dump(index, f, sort_keys=True, indent=2)
else:
	with open("merge_index.json", "r") as f:
		index = json.load(f)

for sample_name, file_list in index.items():
	print(f"{sample_name} => {len(file_list)} files")

if args.dataset = "all":
	samples_to_run = index.keys()
else:
	samples_to_run = args.dataset

for sample_name in sample_to_run:
	file_list = index[sample_name]

	if len(file_list) == 0:
		continue

	jobdir = f"/home/davidryu/MC/rivet/batch/jobs/merge_{sample_name}"
	os.system(f"mkdir -pv {jobdir}")
	os.chdir(jobdir)

	with open("merge_script.sh", "w") as merge_script:
		merge_script.write(f"""#!/bin/bash
source /cvmfs/sft.cern.ch/lcg/views/LCG_101/x86_64-centos7-gcc8-opt/setup.sh
source /cvmfs/sft.cern.ch/lcg/releases/LCG_101/MCGenerators/rivet/3.1.4/x86_64-centos7-gcc8-opt/rivetenv.sh
""")

		condor_file_list = [f"$_CONDOR_SCRATCH_DIR/{os.path.basename(x)}" for x in file_list]

		# All in one go
		# merge_command = f"rivet-merge -e -o /project/users/davidryu/yodamerge/{sample_name}.yoda {' '.join(file_list)}"	
		# f.write(merge_command + "\n")

		# Batches of 10
		current_inputs = copy.deepcopy(condor_file_list)
		mergestep = 0
		final_output = None
		while len(current_inputs) > 1:
			mergestep += 1
			nchunks = int(math.ceil(len(current_inputs) / 10.))
			chunked_inputs = chunkify(current_inputs, nchunks)
			outputs = []
			for ichunk, chunk in enumerate(chunked_inputs):
				merge_command = f"rivet-merge -e -o merged_step{mergestep}_chunk{ichunk}.yoda {' '.join(chunk)}"
				merge_script.write(merge_command + "\n")
				outputs.append(f"merged_step{mergestep}_chunk{ichunk}.yoda")

			# Last step
			if len(outputs) == 1:
				final_output = f"merged_step{mergestep}_chunk{ichunk}.yoda"
			
			# Next step
			current_inputs = outputs

		# Move around outputs
		if not final_output:
			raise ValueError("Something has gone wrong, I don't know the final output")
		merge_script.write(f"mv {final_output} {sample_name}_merged.yoda\n")
		merge_script.write(f"gzip {sample_name}_merged.yoda -c > {sample_name}_merged.yoda.gz\n")
		merge_script.write("mkdir hide\n")
		merge_script.write("mv *yoda hide\n")
		merge_script.write("echo 'Done moving around outputs, here are the current directory contents:'\n")
		merge_script.write("ls -lrth\n")
	csub_command = f"/home/davidryu/MC/rivet/batch/csub merge_script.sh -t tomorrow --mem 8000 --nCores 4 -F {','.join(file_list)}"
	#print(csub_command)
	os.system(csub_command)
	with open("resubmit.sh", "w") as f:
		f.write("#!/bin/bash\n")
		f.write(csub_command + "\n")

