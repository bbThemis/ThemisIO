# TBD

Compile server and wrapper.so, <br>
`git clone https://github.com/bbThemis/TBD`
`cd TBD`
`make`
`cd client`
`./compile.sh`
<br>

You need to reivse the impi path in Makefile.<br>

Run a server, <br>
`cd TBD`
`./server`
<br>
Run on client side<br>
`export MYFS_CONF="/full_path/TBD/myfs.param"`
`export LD_PRELOAD="/full_path/TBD/client/wrapper.so"`

`ls -l /myfs`
`touch /myfs/a`
`ls -l /myfs`

<br>
There are still many bugs. You can "unset LD_PRELOAD" whenever you get issues. I normally add "LD_PRELOAD=xxxx" before the command I need to test. 


