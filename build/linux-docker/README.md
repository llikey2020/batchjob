# Environment
Using Ubuntu 18.04 
GoLang version 1.16

***Note: The following setup steps only works on Ubuntu 18.04. For other versions or OS, the command will be similar.***

# Setup GoLang(Dependency install)
1. Open Ubuntu terminal
2. `cd ` (cd to home directory)
3. Download or scp the golang 1.16 pkg into home directory. You could find the pkg from this link [golang](https://go.dev/doc/install)
4. Run `sudo rm -rf /usr/local/go`
5. Run `sudo tar -C /usr/local -xzf go1.16.linux-amd64.tar.gz`
6. Then run `sudo vi /etc/profile` and add `export PATH=$PATH:/usr/local/go/bin` to the end of file
7. Run `source /etc/profile` to apply the changes.
8. Run `go version` to check you are install the right version of the golang

# Setup Docker
1. Following the Option1 and stops at link [docker](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-18-04)
2. Run `sudo service docker start` and you could run `sudo service docker status` to see whether is running or not.
3. Run `docker ps` to see the containers you have. It should be empty for this case. 
If you are facing error like `Got permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock: Get "http://%2Fvar%2Frun%2Fdocker.sock/v1.24/version": dial unix /var/run/docker.sock: connect: permission denied`
You could run `sudo chmod 666 /var/run/docker.sock` to solve this. Then when you run `docker ps` it should be fine.

# Develop (Build the source)
1. Run `env` or `printenv` to check whether you have already setup the environment variable for `PLANETROVER_REGISTRY_USERNAME` and `PLANETROVER_REGISTRY_PASSWORD`
If not, then run `sudo vi /etc/profile` and add `export PLANETROVER_REGISTRY_USERNAME=xxx` and `export PLANETROVER_REGISTRY_PASSWORD=xxx` to the end of file.
2. Run `./build.sh` under linux-docker directory.

# Push imagess
***Note: If you want to push that newly build images to a registry.***
1. re-tag the image with <register>:tag
2. push that image to the registry.