# WTS development environment from the image wtsdev derived from Anaconda 3
docker run -d -p 8888:8888 --name wtsdevenv -v /home/wts/wts-github/jupyter_notebooks:/opt/notebooks wtsdev /bin/bash -c "/opt/conda/bin/jupyter notebook --notebook-dir=/opt/notebooks --ip='*' --port=8888 --no-browser --allow-root"
# docker commit devConda MyCondaImage


# You can then view the Jupyter Notebook by opening http://localhost:8888 in your browser, or http://<DOCKER-MACHINE-IP>:8888 if you are using a Docker Machine VM.

