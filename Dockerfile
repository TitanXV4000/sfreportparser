FROM buildkite/puppeteer:5.2.1
WORKDIR /usr/src/apps
RUN apt-get update
RUN apt-get -y install git
RUN mkdir /sfexports
RUN git clone https://github.com/johndwalker/sfreportparser.git
WORKDIR /usr/src/apps/sfreportparser
RUN npm install
# If you are building your code for production
# RUN npm ci --only=production
CMD [ "node", "index.js" ]