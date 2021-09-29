# ThemisIO

Compile server and wrapper.so, <br>
`git clone https://github.com/bbThemis/ThemisIO`<br>
`cd ThemisIO`<br>
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


