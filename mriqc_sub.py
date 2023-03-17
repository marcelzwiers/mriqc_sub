#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mriqc_sub.py is a wrapper around mriqc that queries the bids directory for new
participants and then runs participant-level mriqc jobs on the compute cluster.
If the participant-level jobs have all finished, you can run the group-level
"mriqc bidsdir outputdir group" command to generate group level results (the
group level report and the features CSV table)

"""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path


def main(bidsdir: str, outputdir: str, workdir_: str, sessions=(), force=False, mem_gb=18, walltime=8, file_gb_=50, args='', qargs='', dryrun=False, nosub=False, skip=True):

    # Default
    bidsdir   = Path(bidsdir)
    outputdir = Path(outputdir)
    if not outputdir.name:
        outputdir = bidsdir/'derivatives/mriqc'

    # Map the bids session-directories
    if not sessions:
        sessions = list(bidsdir.glob('sub-*/ses-*'))
        if not sessions:
            sessions = list(bidsdir.glob('sub-*'))      # Try without session-subfolders
    else:
        sessions = [bidsdir/session for session in sessions]

    # Loop over bids session-directories and submit a job for every (new) session
    for n, session in enumerate(sessions):

        if not session.is_dir():
            print(f">>> Directory does not exist: {session}")
            continue

        sub_id = [part for part in session.parts if part.startswith('sub-')][0]
        ses_id = [part for part in session.parts if part.startswith('ses-')]
        if ses_id:
            ses_id     = ses_id[0]
            ses_id_opt = f" --session-id {ses_id[4:]}"
        else:
            ses_id     = ''
            ses_id_opt = ''

        tempdir = Path(tempfile.gettempdir() if nosub else '\$TMPDIR')
        if not workdir_:
            workdir = tempdir/f"{sub_id}_{ses_id}"
            file_gb = f",file={file_gb_}gb"
        else:
            workdir = Path(workdir_)/f"{sub_id}_{ses_id}"
            file_gb = ''                                                 # We don't need to allocate local scratch space

        # A session is considered already done if there are html-reports for every anat/*_T?w and every func/*_bold file
        nrniifiles = len(list((bidsdir/sub_id/ses_id/'anat')      .glob(f"{sub_id}_{ses_id}*T?w.nii*")))  + \
                     len(list((bidsdir/sub_id/ses_id/'extra_data').glob(f"{sub_id}_{ses_id}*T?w.nii*")))  + \
                     len(list((bidsdir/sub_id/ses_id/'func')      .glob(f"{sub_id}_{ses_id}*bold.nii*"))) + \
                     len(list((bidsdir/sub_id/ses_id/'extra_data').glob(f"{sub_id}_{ses_id}*bold.nii*")))
        reports    = list(outputdir.glob(f"{sub_id}_{ses_id}*.html"))
        print(f"\n>>> Found {len(reports)}/{nrniifiles} existing MRIQC-reports for: {sub_id}_{ses_id}")

        # Submit the mriqc job to the cluster
        if force or not len(reports)==nrniifiles:

            # Start with a clean directory if we are forcing to reprocess the data (as presumably something went wrong or has changed)
            if force:
                if workdir.is_dir():
                    print(f"Cleaning: {workdir}")
                    if not dryrun:
                        shutil.rmtree(workdir, ignore_errors=True)          # NB: This can also be done in parallel on the cluster if it takes too much time
                for report in reports:
                    print(f"Cleaning: {report}")
                    if not dryrun:
                        report.unlink()

            qsub  = f"qsub -l walltime={walltime}:00:00,mem={mem_gb}gb{file_gb} -N mriqc_{sub_id}_{ses_id} {qargs}"
            mriqc = f'unset PYTHONPATH; export PYTHONNOUSERSITE=1; singularity run --cleanenv --bind {tempdir}:/tmp {os.getenv("DCCN_OPT_DIR")}/mriqc/{os.getenv("MRIQC_VERSION")}/mriqc-{os.getenv("MRIQC_VERSION")}.simg {bidsdir} {outputdir} participant -w {workdir} --participant-label {sub_id[4:]} {ses_id_opt} --verbose-reports --mem_gb {mem_gb} --ants-nthreads 1 --nprocs 1 {args}'
            if nosub:
                workdir.mkdir(parents=True, exist_ok=True)
                command = f"cd {Path.cwd()}\n{mriqc}"
            else:
                command = f"{qsub} <<EOF\ncd {Path.cwd()}\n{mriqc}\nEOF"

            running = subprocess.run('if [ ! -z "$(qselect -s RQH)" ]; then qstat -f $(qselect -s RQH) | grep Job_Name | grep mriqc_sub-; fi', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            if skip and f"mriqc_{sub_id}_{ses_id}" in running.stdout.decode():
                print(f"--> Skipping already running / scheduled job ({n+1}/{len(sessions)}): mriqc_{sub_id}_{ses_id}")
            else:
                print(f"--> {'Running' if nosub else 'Submitting'} job ({n+1}/{len(sessions)}):\n{command}")
                if not dryrun:
                    process = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                    if process.stderr.decode('utf-8') or process.returncode!=0:
                        print(f"ERROR {process.returncode}: Job submission failed\n{process.stderr.decode('utf-8')}\n{process.stdout.decode('utf-8')}")
                    if nosub:
                        shutil.rmtree(workdir, ignore_errors=True)

        else:
            print(f"--> Nothing to do for job ({n+1}/{len(sessions)}): {session}")

    if not sessions:
        print(f"No BIDS subject/session folders found in {bidsdir}")
    elif dryrun:
        print('\n----------------\nDone! NB: The printed jobs were not actually submitted')
    elif nosub:
        print('\n----------------\nDone!')
    else:
        print('\n----------------\n'
              'Done! Now wait for the jobs to finish... Check that e.g. with this command:\n\n  qstat -a $(qselect -s RQ) | grep mriqc_sub\n\n'
              'When finished you can run e.g. a group-level QC analysis like this:\n\n'
              f'  mriqc_group {bidsdir}\n\n')


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
                                            '  mriqc_sub.py /project/3022026.01/bids\n'
                                            '  mriqc_sub.py /project/3022026.01/bids -w /project/3022026.01/mriqc_work\n'
                                            '  mriqc_sub.py /project/3022026.01/bids -o /project/3022026.01/mriqc --sessions sub-010/ses-mri01 sub-011/ses-mri01\n'
                                            '  mriqc_sub.py /project/3022026.01/bids -a " --fft-spikes-detector --no-sub"\n'
                                            '  mriqc_sub.py -f -m 16 /project/3022026.01/bids -s sub-013/ses-mri01\n\n'
                                            'Author:\n' 
                                            '  Marcel Zwiers\n ')
    parser.add_argument('bidsdir',          help='The bids-directory with the subject data')
    parser.add_argument('-o','--outputdir', help='The mriqc output-directory where the html-reports will be stored (default = bidsdir/derivatives/mriqc)', default='')
    parser.add_argument('-w','--workdir',   help='The working-directory where intermediate files are stored (default = a temporary directory', default='')
    parser.add_argument('-s','--sessions',  help='Space separated list of selected sub-#/ses-# names / folders to be processed. Otherwise all sessions in the bidsfolder will be selected', nargs='+')
    parser.add_argument('-f','--force',     help='If this flag is given subjects will be processed with a clean working directory, regardless of existing folders in the bidsfolder. Otherwise existing folders will be skipped', action='store_true')
    parser.add_argument('-i','--ignore',    help='If this flag is given then already running or scheduled jobs with the same name are ignored, otherwise job submission is skipped', action='store_false')
    parser.add_argument('-m','--mem_gb',    help='Required amount of memory in GB', default=18, type=int)
    parser.add_argument('-t','--time',      help='Required walltime in hours', default=8, type=int)
    parser.add_argument('-l','--local_gb',  help='Required free diskspace of the local temporary workdir (in gb)', default=50, type=int)
    parser.add_argument('-a','--args',      help='Additional arguments that are passed to mriqc (NB: Use quotes and a leading space to prevent unintended argument parsing)', type=str, default='')
    parser.add_argument('-q','--qargs',     help='Additional arguments that are passed to qsub (NB: Use quotes and a leading space to prevent unintended argument parsing)', type=str, default='')
    parser.add_argument('-n','--nosub',     help='Add this flag to run the mriqc commands locally, without submitting them (useful for debugging)', action='store_true')
    parser.add_argument('-d','--dryrun',    help='Add this flag to just print the mriqc qsub commands without actually submitting them (useful for debugging)', action='store_true')
    args = parser.parse_args()

    main(bidsdir   = args.bidsdir,
         outputdir = args.outputdir,
         workdir_  = args.workdir,
         sessions  = args.sessions,
         force     = args.force,
         mem_gb    = args.mem_gb,
         walltime  = args.time,
         file_gb_  = args.local_gb,
         args      = args.args,
         qargs     = args.qargs,
         dryrun    = args.dryrun,
         nosub     = args.nosub,
         skip      = args.ignore)
