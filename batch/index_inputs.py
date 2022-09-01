# Make an index of input .yoda files, produced automatically by Andrew's framework
import os
import sys
import glob
import json
from pprint import pprint

import argparse
parser = argparse.ArgumentParser(description="Index inputs")
parser.add_argument("what", type=str, default="yoda", help="What to index (mg, pythia, rivet, delphes)")
parser.add_argument("--info", action="store_true", help="Print info about samples")
args = parser.parse_args()

if not args.what in ["mg", "pythia", "rivet", "delphes"]:
	raise ValueError("Usage: python index_inputs.py [mg | pythia | rivet | delphes]")

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

index = {}
for sample_name, sample_dir in input_dirs.items():
	if args.what == "mg": 
		index[sample_name] = glob.glob(f"{sample_dir}/mg/*lhe.gz")
	elif args.what == "pythia": 
		index[sample_name] = glob.glob(f"{sample_dir}/pythia/*hepmc.gz")
	elif args.what == "rivet":
		index[sample_name] = glob.glob(f"{sample_dir}/rivet/*yoda.gz")
	elif args.what == "delphes":
		index[sample_name] = glob.glob(f"{sample_dir}/delphes/*root")
	index[sample_name] = [x for x in index[sample_name] if os.path.getsize(x) > 0]

with open(f"{args.what}_index.json", "w") as f:
	json.dump(index, f, sort_keys=True, indent=2)

if args.info:
	total_size = 0
	sample_size = {}
	for sample_name, file_list in index.items():
		sample_size[sample_name] = 0
		for fname in file_list:
			sample_size[sample_name] += os.path.getsize(fname)
			total_size += os.path.getsize(fname)
	print(f"Total size: {total_size}")
	pprint(sample_size)
