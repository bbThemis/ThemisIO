# Instructions for reproducing test results in the paper "ThemisIO: Software-defined Input/Output on Supercomputers"

All the tests were run on [Frontera](https://www.tacc.utexas.edu/systems/frontera).

1. Download and build ThemisIO 
    ```
    git clone https://github.com/bbThemis/ThemisIO.git
    cd ThemisIO
    make
    (cd src/client; ./compile.sh)
    ```
1. Start an interactive six node job.
    ```
    idev -N 6 -t 1:00:00
    ```
1. The server and clients will be running in the same Slurm job from different command shells,
so it will be necessary to copy Slurm settings from the original command shell (the "**server-shell**") to a second command shell (the "**client-shell**").
When the job starts, output environment variables set by Slurm by running the script `tests/slurmvars`.
    ```
    [server-shell]$ tests/slurmvars
    export SLURM_NODELIST=... ; export SLURM_JOBID=... ; export SLURM_NNODES=6
    ```

1. Start a second command shell (the client shell). Log into the second node listed in $SLURM_NODELIST 
(because the server will be generating a heavy load on the first node).
Set the Slurm environment variables by pasting the output of tests/slurmvars from the server shell.
Modify this step as needed if you use a shell other than bash.
Change current directory to the ThemisIO/testfair directory.
Build the rw_speed executable.
    ```
    [client-shell]$ ssh c207-015
    [client-shell]$ export SLURM_NODELIST=c207-[014-015],c210-[018-021] ; export SLURM_JOBID=4236761 ; export SLURM_NNODES=6
    [client-shell]$ cd ThemisIO/testfair
    [client-shell]$ make rw_speed
    ```

1. In the server shell, start the server in size-fair mode
    ```
    [server-shell]$ ibrun -n 1 ./server.sh --policy size-fair
    ```
    
1. In the client shell, run `./test.4v1.sh`.  There is a line in the output with the prefix `rw_speed.job1` and
another with the prefix `rw_speed.job2` each containing a set of name=value pairs. 
In each of those is a field titled `mbps_1sec_time_slices=` which contains the
throughput of each job during each second of the test. This is the data used to generate figure 6(a) in the paper.
    ```
    rw_speed time=...
    rw_speed.job1 time=... mbps_1sec_time_slices=(0.0,16303.0,20762.0,...)
    rw_speed.job2 time=... mbps_1sec_time_slices=(0.0,0.0,0.0,6527.0,13646.0,14919.0,...)
    ```

1. In the server shell, kill the server with ctrl-c and restart it in job-fair mode.
    ```
    ...Sending Ctrl-C to processes as requested...
    [server-shell]$ ibrun -n 1 ./server.sh --policy job-fair
    ```
1. In the client shell, run `./test.4v1.sh` again. This output is the data used to generate figure 6(b) in the paper.
1. In the server shell, kill the server with ctrl-c and restart it in user-fair mode.
    ```
    ...Sending Ctrl-C to processes as requested...
    [server-shell]$ ibrun -n 1 ./server.sh --policy user-fair
    ```
1 . In the client shell, run `./test.user-fair.4v1.sh`. This output is the data used to generate figure 6(c) in the paper.

## Instructions for recreating TBF results on ThemisIO

1. After cloning the repository, switch to the `TBF_on_themis` branch. 
2. Follow steps 1 - 5 above exactly
3. In the client shell, run `./test_tbf.sh`.  There is a line in the output with the prefix `rw_speed.job1` and
   another with the prefix `rw_speed.job2` each containing a set of name=value pairs. 
   In each of those is a field titled `mbps_1sec_time_slices=` which contains the
   throughput of each job during each second of the test. This is the data used to generate figure 7(c) in the paper.

