#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mriqc_group.py is a wrapper around mriqc that runs a group-level mriqc job
on the compute cluster.
"""

import os
import subprocess
from pathlib import Path


def main(bidsdir, outputdir='', force=False, manager='torque', mem_gb=1, args='', qargs='', nosub=False):

    # Defaults
    manager   = 'slurm' if 'slurm' in os.getenv('PATH') else 'torque'
    bidsdir   = Path(bidsdir)
    outputdir = Path(outputdir)
    print(f"Detected cluster manager: {manager}")
    if not outputdir.name:
        outputdir = bidsdir/'derivatives'/'mriqc'

    # Checks
    if not outputdir.is_dir():
        print(f"ERROR: {outputdir} does not exist")
        return
    reports = list(outputdir.glob('sub-*.html'))
    print(f"\n>>> Found {len(reports)} existing MRIQC-reports in {outputdir}")
    if not reports:
        return

    # Generate the submit-command
    if manager == 'torque':
        submit = f"qsub -l walltime=0:10:00,mem={mem_gb}gb -N mriqc_group {qargs}"
        running = subprocess.run('if [ ! -z "$(qselect -s RQH)" ]; then qstat -f $(qselect -s RQH) | grep Job_Name | grep mriqc_; fi', shell=True, capture_output=True, text=True)
    elif manager == 'slurm':
        submit = f"sbatch --job-name=mriqc_group --mem={mem_gb}G --time=0:10:00 {qargs}"
        running = subprocess.run('squeue -h -o format=%j | grep mriqc_', shell=True, capture_output=True, text=True)
    else:
        print(f"ERROR: Invalid resource manager `{manager}`")
        exit(1)

    # Generate the mriqc-job
    job = textwrap.dedent(f"""\
        #!/bin/bash
        ulimit -v unlimited
        cd {Path.cwd()}              
        apptainer run --cleanenv {os.getenv("DCCN_OPT_DIR")}/mriqc/{os.getenv("MRIQC_VERSION")}/mriqc-{os.getenv("MRIQC_VERSION")}.simg {bidsdir} {outputdir} group --nprocs 1 {args}""")

    # Submit the job to the compute cluster
    command = job if nosub else f"{submit} <<EOF\n{job}\nEOF"
    if not force and 'mriqc_' in running.stdout.decode():
        print(f'--> Skipping mriqc_goup because there are still mriqc_sub/group jobs running / scheduled. Use the -f option to override')
    else:
        print(f'--> Submitting job:\n{command}')
        process = subprocess.run(command, shell=True, capture_output=True, text=True)
        if process.stderr or process.returncode!=0:
            print(f"ERROR {process.returncode}: Job submission failed\n{process.stderr}\n{process.stdout}")
        else:
            print('\n----------------\n'
                  f"Done! Now wait for the job to finish... Check that e.g. with this command:\n\n"
                  f"  {'qstat -a $(qselect -s RQ)' if manager=='torque' else 'squeue -u '+os.getenv('USER')} | grep mriqc_group\n\n ")


# Shell usage
if __name__ == "__main__":

    # Parse the input arguments and run bidscoiner(args)
    import argparse
    import textwrap

    class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
        pass

    parser = argparse.ArgumentParser(formatter_class=CustomFormatter, description=textwrap.dedent(__doc__),
                                     epilog='for more information see:\n'
                                            '  module help mriqc\n'
                                            '  mriqc -h\n\n'
                                            'examples:\n'
                                            '  mriqc_group.py /project/3022026.01/bids\n'
                                            '  mriqc_group.py /project/3022026.01/bids -o /project/3022026.01/mriqc\n\n'
                                            'Author:\n'
                                            '  Marcel Zwiers\n ')
    parser.add_argument('bidsdir',                help='The bids-directory with the subject data')
    parser.add_argument('-o','--outputdir',       help='The mriqc output-directory where the html-reports are stored (None -> bidsdir/derivatives/mriqc)', default='')
    parser.add_argument('-f','--force',           help='If this flag is given then already running or scheduled mriqc_sub/group jobs with the same name are ignored, otherwise this function-call is cancelled', action='store_false')
    parser.add_argument('-r','--resourcemanager', help='Resource manager to which the jobs are submitted', choices=('torque', 'slurm'), default='torque')
    parser.add_argument('-m','--mem_gb',          help='Maximum required amount of memory', default=1, type=int)
    parser.add_argument('-a','--args',            help='Additional arguments that are passed to mriqc (NB: Use quotes and a leading space to prevent unintended argument parsing)', type=str, default='')
    parser.add_argument('-q','--qargs',           help='Additional arguments that are passed to qsub (NB: Use quotes and a leading space to prevent unintended argument parsing)', type=str, default='')
    parser.add_argument('-n','--nosub',           help='Add this flag to run the mriqc commands locally, without submitting them (useful for debugging)', action='store_true')
    args = parser.parse_args()

    main(bidsdir=args.bidsdir, outputdir=args.outputdir, force=args.force, manager=args.resourcemanager, mem_gb=args.mem_gb, args=args.args, qargs=args.qargs, nosub=args.nosub)
