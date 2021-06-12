# TBD

# Compile server and wrapper.so
git clone https://github.com/bbThemis/TBD
cd TBD
make (# You need to reivse the impi path in Makefile)
cd client
./compile.sh


# Run
# Server side
cd TBD
./server

# Client side
export MYFS_CONF="/full_path/TBD/myfs.param"
export LD_PRELOAD="/full_path/TBD/client/wrapper.so"

ls -l /myfs
touch /myfs/a
ls -l /myfs

# There are still many bugs. You can "unset LD_PRELOAD" whenever you get issues. I normally add "LD_PRELOAD=xxxx" before the command I need to test. 


