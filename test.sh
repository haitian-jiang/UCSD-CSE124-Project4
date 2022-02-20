rm ../test/test1/index.txt

# start meta store and two block store, register them
./SurfstoreServerExec -s both -p 80 -l -d -r 512 localhost:80 
./SurfstoreServerExec -s block -p 8080 -l -d -r 512 localhost:8080 &
./SurfstoreAdminExec -s add localhost:80 localhost:8080

# sync file to server
./SurfstoreClientExec -d localhost:80 ../test/test1 512

# sanity check
./SurfstoreDebugExec -r 512 localhost:80 | wc -l
./SurfstoreDebugExec -r 512 localhost:8080 | wc -l

# add block server
./SurfstoreServerExec -s block -p 8888 -l -d -r 512 localhost:8888 &
./SurfstoreAdminExec -s add localhost:80 localhost:8888
./SurfstoreDebugExec -r 512 localhost:8888 | wc -l

# remove another block server
./SurfstoreAdminExec -s remove localhost:80 localhost:8080
./SurfstoreDebugExec -r 512 localhost:80 | wc -l

# sync file into second directory
./SurfstoreClientExec -d localhost:80 ../test/test2 512

# start yet another server
./SurfstoreServerExec -s block -p 777 -l -d -r 512 localhost:777 &
./SurfstoreAdminExec -s add localhost:80 localhost:777
./SurfstoreDebugExec -r 512 localhost:777 | wc -l

# do some changes on directory 2
rm ../test/test2/file1

# push the change to server
./SurfstoreClientExec -d localhost:80 ../test/test2 512

# add server
./SurfstoreServerExec -s block -p 222 -l -d -r 512 localhost:222 &
./SurfstoreAdminExec -s add localhost:80 localhost:222
./SurfstoreDebugExec -r 512 localhost:222 | wc -l

# pull changes
./SurfstoreClientExec -d localhost:80 ../test/test1 512

