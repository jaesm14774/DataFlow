FROM apache/airflow:2.10.5-python3.12
COPY requirements.txt .
USER root
RUN sudo apt-get update
RUN sudo apt-get install python3-dev default-libmysqlclient-dev build-essential -y
RUN sudo apt-get install libopencv-dev python3-opencv -y
RUN sudo apt install vim -y
RUN pip3 install --upgrade pip
RUN sudo apt-get install -y wget && wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# 安裝 Chrome 所需的依賴項
RUN sudo apt-get install -y fonts-liberation libasound2 libgbm1 xdg-utils
RUN sudo apt-get install -y libvulkan1
RUN sudo dpkg -i google-chrome-stable_current_amd64.deb
RUN sudo apt-get install -f
RUN sudo pip install -r requirements.txt
