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
import glob
import subprocess


def main(bidsdir, outputdir, sessions=(), force=False, mem_gb=18, argstr='', dryrun=False, skip=True):

    # Default
    if not outputdir:
        outputdir = os.path.join(bidsdir,'derivatives')         # NB: A mriqc subfolder is added to the outputdir later to be match the BIDS drivatives draft of one folder per pipeline

    # Map the bids directory. TODO: make robust against datasets without session-subfolders
    if not sessions:
        sessions = glob.glob(os.path.join(bidsdir, 'sub-*'+os.sep+'ses-*'))
    else:
        sessions = [os.path.join(bidsdir, session) for session in sessions]

    # Submit a job for every (new) session
    for session in sessions:

        if not os.path.isdir(session):
            print('Not a directory: ' + session)
            continue

        sub_id = session.rsplit('sub-')[1].split(os.sep)[0]
        ses_id = session.rsplit('ses-')[1]

        # A session is considered already done if there is a html-report
        if force or not glob.glob(os.path.join(outputdir, 'mriqc', 'sub-' + sub_id + '_ses-' + ses_id + '_*.html')):

            # Submit the mriqc jobs to the cluster
            # usage: mriqc [-h] [--version]
            #              [--participant_label PARTICIPANT_LABEL [PARTICIPANT_LABEL ...]]
            #              [--session-id SESSION_ID [SESSION_ID ...]]
            #              [--run-id RUN_ID [RUN_ID ...]] [--task-id TASK_ID [TASK_ID ...]]
            #              [-m [{T1w,bold,T2w} [{T1w,bold,T2w} ...]]] [-w WORK_DIR]
            #              [--report-dir REPORT_DIR] [--verbose-reports] [--write-graph]
            #              [--dry-run] [--profile] [--use-plugin USE_PLUGIN] [--no-sub]
            #              [--email EMAIL] [-v] [--webapi-url WEBAPI_URL]
            #              [--webapi-port WEBAPI_PORT] [--upload-strict] [--n_procs N_PROCS]
            #              [--mem_gb MEM_GB] [--testing] [-f] [--ica] [--hmc-afni]
            #              [--hmc-fsl] [--fft-spikes-detector] [--fd_thres FD_THRES]
            #              [--ants-nthreads ANTS_NTHREADS] [--ants-float]
            #              [--ants-settings ANTS_SETTINGS] [--deoblique] [--despike]
            #              [--start-idx START_IDX] [--stop-idx STOP_IDX]
            #              [--correct-slice-timing]
            #              bids_dir output_dir {participant,group} [{participant,group} ...]
            # mriqc --verbose-reports -w mriqc/work/010 --participant_label 010 --mem_gb 23 --ants-nthreads 1 --nprocs 1 bids/ mriqc/ participant

            command = """qsub -l walltime=24:00:00,mem={mem_gb}gb -N mriqc_{sub_id}_{ses_id} <<EOF
                         module rm fsl; module add mriqc; source activate /opt/mriqc; cd {pwd}
                         mriqc {bidsdir} {outputdir} participant -w {workdir} --participant-label {sub_id} --session-id {ses_id} --verbose-reports --mem_gb {mem_gb} --ants-nthreads 1 --nprocs 1 {args}\nEOF"""\
                         .format(bidsdir=bidsdir, outputdir=os.path.join(outputdir,'mriqc'), workdir=os.path.join(outputdir,'work_mriqc','sub-'+sub_id+'_ses-'+ses_id), sub_id=sub_id, ses_id=ses_id, mem_gb=mem_gb, args=argstr, pwd=os.getcwd())
            running = subprocess.run('qstat -f $(qselect -s RQH) | grep Job_Name | grep mriqc_',
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            if skip and 'mriqc_' + sub_id in running.stdout.decode():
                print('>>> Skipping already running / scheduled job: mriqc_' + sub_id)
            else:
                print('>>> Submitting job:\n' + command)
                if not dryrun:
                    proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                    if proc.returncode != 0:
                        print('Job submission failed with error-code: {}\n'.format(proc.returncode))

    print('\n----------------\n' 
          'Done! Now wait for the jobs to finish before running the group-level QC, e.g. like this:\n\n'
          '  source activate /opt/mriqc\n'
          '  mriqc {bidsdir} {outputdir} group\n\n' 
          'You may remove the (large) {outputdir}/work_mriqc subdirectory when the jobs are finished; for more details, see:\n\n'
          '  mriqc -h\n '.format(bidsdir=bidsdir, outputdir=outputdir))


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
                                            '  mriqc_sub.py /project/3022026.01/bids -o /project/3022026.01/derivatives --sessions sub-010/ses-mri01 sub-011/ses-mri01\n'
                                            '  mriqc_sub.py /project/3022026.01/bids -a "--fft-spikes-detector --no-sub"\n'
                                            '  mriqc_sub.py -f -m 16 /project/3022026.01/bids -s sub-013/ses-mri01\n\n'
                                            'Author:\n' 
                                            '  Marcel Zwiers\n ')
    parser.add_argument('bidsdir',          help='The bids-directory with the (new) subject data')
    parser.add_argument('-o','--outputdir', help='The output-directory where the mriqc-reports are stored (None = bidsdir/derivatives)')
    parser.add_argument('-s','--sessions',  help='Space seperated list of selected sub-#/ses-# names / folders to be processed. Otherwise all sessions in the bidsfolder will be selected', nargs='+')
    parser.add_argument('-f','--force',     help='If this flag is given subjects will be processed, regardless of existing folders in the bidsfolder. Otherwise existing folders will be skipped', action='store_true')
    parser.add_argument('-i','--ignore',            help='If this flag is given then already running or scheduled jobs with the same name are ignored, otherwise job submission is skipped', action='store_false')
    parser.add_argument('-m','--mem_gb',    help='Maximum required amount of memory', default=18)
    parser.add_argument('-a','--args',      help='Additional arguments that are passed to mriqc (NB: Use quotes to prevent parsing of spaces)', type=str, default='')
    parser.add_argument('-d','--dryrun',    help='Add this flag to just print the mriqc qsub commands without actually submitting them (useful for debugging)', action='store_true')
    args = parser.parse_args()

    main(bidsdir=args.bidsdir, outputdir=args.outputdir, sessions=args.sessions, force=args.force, mem_gb=args.mem_gb, argstr=args.args, dryrun=args.dryrun, skip=args.ignore)
