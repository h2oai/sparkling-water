"""
Test harness for RSparkling

This subprocess will call all *.R scripts in the "tests" folder

"""

import glob
import subprocess

def run_all():
	for file in glob.glob("*.R"):
		print("Running the following test: " + file)
		subprocess.check_call(['R','-f', file], shell=False)


if __name__ == "__main__":
	run_all()