docker stop wtsdevenv
docker rm wtsdevenv
docker rmi wtsdev
docker build -t wtsdev:latest . 
../wtsdevenv.sh
docker logs wtsdevenv
