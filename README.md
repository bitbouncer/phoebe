# phoebe 
phoebe is a fast read only, in memory, key-value database written in C++

- uses kafka compacted topics for persistense.
- a typical use-case would be a authentication database for users in the range of 10M users per server

Building 
- first see https://github.com/bitbouncer/csi-build-scripts
- bash build_linux.sh (should work on centos7 and ubuntu14)
- rebuild_win64_vc12.bat (windows)

