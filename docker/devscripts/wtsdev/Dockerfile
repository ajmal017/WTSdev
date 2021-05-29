# This is a Docker file to customise and extend Anaconda 3 for WTS
FROM continuumio/anaconda3
MAINTAINER mailtocap@gmail.com

EXPOSE 8888/tcp
EXPOSE 7497/tcp

RUN /opt/conda/bin/conda install jupyter -y --quiet
RUN pip install pandas
RUN pip install pandas-datareader
RUN pip install matplotlib
RUN pip install mpl-finance
RUN pip install mplfinance
RUN pip install scikit-learn
RUN pip install beautifulsoup4

#WORKDIR /home/wts/wtsdevgit/jupyter_notebooks

#COPY twsapi_macunix.976.01  /home


# RUN python ~/wtsdevgit/jupyter_notebooks/twsapi_macunix.976.01/IBJts/source/pythonclient/setup.py install

