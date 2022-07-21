# Environment
Using Ubuntu 18.04 ; GoLang version 1.16; Not works with GoLang version 1.10.x

***Note: The following setup steps only works on Ubuntu 18.04. For other versions or OS, the command will be similar.***

# Setup GoLang(Dependency install)
***Note: If you have a different version than 1.16 of GoLang. Please uninstall it first, then follow the steps below. Batch-Job image does not work with versions other than 1.16***
1. Open terminal and change directory to the home by running `cd ` 
2. Download the golang 1.16 pkg into home directory. You could find the pkg from this link [golang](https://go.dev/doc/install).
3. Run `sudo rm -rf /usr/local/go`
4. Run `sudo tar -C /usr/local -xzf go1.16.linux-amd64.tar.gz`
5. Then run `sudo vi /etc/profile` and add `export PATH=$PATH:/usr/local/go/bin` to the end of file
6. Run `source /etc/profile` to apply the changes.
7. Run `go version` to check you are install the right version of the golang

# Setup Docker
1. Following the Option1 in [docker](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-18-04)
2. Run `sudo service docker start` and you could run `sudo service docker status` to see whether is running or not.
3. Run `docker ps` to see the containers you have. It should be empty for this case. 
If you are facing error like `Got permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock: Get "http://%2Fvar%2Frun%2Fdocker.sock/v1.24/version": dial unix /var/run/docker.sock: connect: permission denied`
You could run `sudo chmod 666 /var/run/docker.sock` to solve this. Then when you run `docker ps` it should be fine.

# Build
1. Run `env` or `printenv` to check whether you have already setup the environment variable for `PLANETROVER_REGISTRY_USERNAME` and `PLANETROVER_REGISTRY_PASSWORD`
If not, then run `sudo vi /etc/profile` and add `export PLANETROVER_REGISTRY_USERNAME=xxx` and `export PLANETROVER_REGISTRY_PASSWORD=xxx` to the end of file.
2. Run `./build.sh` under linux-docker directory.
3. Run `docker images` to check the newly build images

# Push images
***Note: If you want to push that newly build images to a registry.***
1. Run `docker images` to get the value for tag or copy the tag value that printed out.
2. Run `export TAG=xxx` where xxx should be replace by the value you just copied.
3. Run `docker image tag batch-job:$TAG container-registry.planetrover.ca:31320/sequoiadp/batch-job:<name>` where you have to fill in the name. Names with all numerical numbers counts as invalid image name.
4. If you first time push the locally build image to registry: run `sudo vi /etc/docker/daemon.json` and add `{"insecure-registries" : ["container-registry.planetrover.ca:31320"]}` to the file and save it. And restart the docker by run `sudo service docker restart`. If you have the same error before, please look at the steps for setup docker.
5. Run `docker push container-registry.planetrover.ca:31320/sequoiadp/batch-job:<name>`
6. You could run `curl --location --request GET 'http://container-registry.planetrover.ca:31320/v2/sequoiadp/batch-job/tags/list'` to check whether you successfully push the image.