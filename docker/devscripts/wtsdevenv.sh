# WTS development environment from the image wtsdev derived from Anaconda 3
docker run -p 8888:8888  -d --name wtsdevenv -v /home/wts/wtsdevgit/jupyter_notebooks:/opt/notebooks wtsdev /bin/bash -c "/opt/conda/bin/jupyter notebook --notebook-dir=/opt/notebooks --ip='*' --port=8888 --no-browser --allow-root"

# -p 7497:7497
# docker commit devConda MyCondaImage


# You can then view the Jupyter Notebook by opening http://localhost:8888 in your browser, or http://<DOCKER-MACHINE-IP>:8888 if you are using a Docker Machine VM.

