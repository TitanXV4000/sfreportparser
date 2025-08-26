FROM ghcr.io/puppeteer/puppeteer:22.15.0
WORKDIR /usr/src/apps
USER root
RUN apt-get update
RUN apt-get -y install git
RUN apt-get -y install vim
RUN mkdir /sfexports
RUN chown -R pptruser:pptruser /usr/src/apps /sfexports
USER pptruser
RUN git clone https://github.com/TitanXV4000/sfreportparser.git
WORKDIR /usr/src/apps/sfreportparser
RUN npm install
# If you are building your code for production
# RUN npm ci --only=production
CMD [ "node", "index.js" ]
