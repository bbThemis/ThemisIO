# ThemisIO

ThemisIO is a first of its kind software-defined I/O system for supercomputers. 
It enables policy-driven I/O capacity sharing on supercomputers. 
At its core, ThemisIO disassociates I/O control (i.e., I/O request processing order) from processing by incorporating job metadata such as user, job id, and job size (i.e., node count)
ThemisIO can precisely balance the I/O cycles between applications via time slicing to enforce processing isolation, enabling a variety of fair sharing policies. 
ThemisIO can precisely allocate I/O resources to jobs so that every job gets at least its fair share of the I/O capacity as defined by the sharing policy.
ThemisIO can decrease the slowdown of real applications due to I/O interference by two to three orders of magnitude when using fair sharing polices compared to the first-in-first-out (FIFO) baseline.

<p align = "center">
<img src="https://github.com/bbThemis/ThemisIO/blob/main/docs/figures/test.size-fair.4v1.png" width="320">
<!--- ![alt text](https://github.com/bbThemis/ThemisIO/blob/main/docs/figures/test.size-fair.4v1.png =320x240 " Size-fair, 4-node job competing with 1-node job") --->
</p>
<p align = "center">
Fig.1 - Size-fair, 4-node job competing with 1-node job
</p>

<p align = "center">
<img src="https://github.com/bbThemis/ThemisIO/blob/main/docs/figures/test.job-fair.4v1.png" width="320">
<!--- ![alt text](https://github.com/bbThemis/ThemisIO/blob/main/docs/figures/test.size-fair.4v1.png =320x240 " Job-fair, 4-node job competing with 1-node job") --->
</p>
<p align = "center">
Fig.2 - Job-fair, 4-node job competing with 1-node job
</p>

<p align = "center">
<img src="https://github.com/bbThemis/ThemisIO/blob/main/docs/figures/test.user-fair.4v1.png" width="320">
<!--- ![alt text](https://github.com/bbThemis/ThemisIO/blob/main/docs/figures/test.size-fair.4v1.png =320x240 " User-fair, Two 2-node jobs competing with a 1-node job") --->
</p>
<p align = "center">
Fig.3 - User-fair, Two 2-node jobs competing with a 1-node job
</p>
## Benchmark
<p align = "center">
<img src="https://github.com/bbThemis/ThemisIO/blob/main/docs/figures/write_read_single_server.PNG" width="320">
<!--- ![alt text](https://github.com/bbThemis/ThemisIO/blob/main/docs/figures/write_read_single_server.PNG =320x240 " IOR benchmark for one server") --->
</p>
<p align = "center">
Fig.4 - IOR benchmark for one server.
</p>

## Build Instruction
Compile server and wrapper.so, <br>
`git clone https://github.com/bbThemis/ThemisIO`<br>
`cd ThemisIO`<br>
`mkdir obj` <br>
`make`<br>
`cd src/client`<br>
`./compile.sh`<br>
<br>

You need to revise the impi path in Makefile.<br>

Run a server, <br>
`cd ThemisIO`<br>
`./server`<br>
<br>
Run on client side<br>
`export MYFS_CONF="/full_path/ThemisIO/myfs.param"`<br>
`export LD_PRELOAD="/full_path/ThemisIO/wrapper.so"`<br>
<br>
`ls -l /myfs`<br>
`touch /myfs/a`<br>
`ls -l /myfs`<br>

<br>
There are still many bugs. You can "unset LD_PRELOAD" whenever you get issues. I normally add "LD_PRELOAD=xxxx" before the command I need to test. 


