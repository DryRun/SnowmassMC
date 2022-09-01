# Make an index of input .yoda files, produced automatically by Andrew's framework
# This version is for lxplus + pre-merged inputs
import os
import sys
import glob
import json
from pprint import pprint

index = {
	"100TeV_BBB": ["/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/yoda/100TeV_BBB_merged.yoda.gz"], 
	"100TeV_LL": ["/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/yoda/100TeV_LL_merged.yoda.gz"], 
	"100TeV_t": ["/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/yoda/100TeV_t_merged.yoda.gz"], 
	"100TeV_tB": ["/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/yoda/100TeV_tB_merged.yoda.gz"], 
	"100TeV_vbf": ["/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/yoda/100TeV_vbf_merged.yoda.gz"], 
	"13TeV_B": ["/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/yoda/13TeV_B_merged.yoda.gz"], 
	"13TeV_BBB": ["/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/yoda/13TeV_BBB_merged.yoda.gz"], 
	"13TeV_H": ["/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/yoda/13TeV_H_merged.yoda.gz"], 
	"13TeV_LL": ["/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/yoda/13TeV_LL_merged.yoda.gz"], 
	"13TeV_LLB": ["/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/yoda/13TeV_LLB_merged.yoda.gz"], 
	"13TeV_t": ["/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/yoda/13TeV_t_merged.yoda.gz"], 
	"13TeV_ttB": ["/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/yoda/13TeV_ttB_merged.yoda.gz"], 
	"13TeV_tB": ["/afs/cern.ch/user/d/dryu/workspace/snowmass/MC/yoda/13TeV_tB_merged.yoda.gz"], 
}

with open("yodamerged_index.json", "w") as f:
	json.dump(index, f, sort_keys=True, indent=2)
