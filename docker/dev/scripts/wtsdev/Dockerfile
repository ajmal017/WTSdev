# This is a Docker file to customise and extend Anaconda 3 for WTS
FROM continuumio/anaconda3
MAINTAINER mailtocap@gmail.com

RUN /opt/conda/bin/conda install jupyter -y --quiet
RUN pip install pandas
RUN pip install pandas-datareader
RUN pip install matplotlib
RUN pip install mpl-finance
RUN pip install scikit-learn
RUN pip install beautifulsoup4

