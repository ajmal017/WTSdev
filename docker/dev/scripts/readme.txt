Folders:
wtsdev	
	This folder contains the Dockerfile to create an image wtsdev from anaconda3 image taken from dockerhub.
	The dockerfile also does the following on top of the image.
		1. Installs Jupyter notebooks inside.
		2. Installed some python libraries to test projects in Python bible.
		...To be further customised.
		
	To build this file execute the 4th command from this folder. Before that we need to clear the existing setup.
		1. docker stop wtsdevenv
		2. docker rm wtsdevenv
		3. docker rmi wtsdev
		4. docker build -t wtsdev:latest .
		5. Execute ../wtsdevenv.sh	

Files:
wtsdevenv.sh
	This script creates and runs a container wtsdevenv from wtsdev.
		1. Exposes port 8888 to the host.
		2. Maps /home/wts/dev/jupyter_notebooks of the host as /opt/notebooks of the container.
		3. Executes Jupyter notes Server to listen to port 8888 and maps the above folder as notebook folder.
		4. Container runs in the background (detached mode)

	To connect to this Jupyter Server from the host, we need the URL to connect. This can be collected
		1. from the logs using the command "Docker logs wtsdevenv" and scroll for the URL
		2. OR execute the following commands in another terminal
			1. docker exec -it wtsdevenv bash # This command interactively opens the bash command inside the container
			2. jupyter notebook list # This command lists all the running notebook servers and their URLs.
			
devpgdbsvr.sh
	This script creates and runs a container devpgdbsvr from postgres (taken from dockerhub).
		1. Exposes port 5432 (Postgresql default port) to the host.
		2. Maps /home/wts/dev/postgresqldata of the host as /var/lib/postgresql/data of the container.
		3. Sets the PostgreSQL DB password.
		4. Container runs in the background (detached mode)

devpgadmin.sh
	This script creates and runs a container devpgadmin from dpage/pgadmin4 (taken from dockerhub).
		1. Exposes port 80 (http port) to the host.
		2. Sets the PgAdmin default email id (userid) and password.
		3. Container runs in the background (detached mode)
		
