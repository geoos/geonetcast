# desde el M1
# docker buildx build --push --platform linux/amd64 -t docker.homejota.net/geoos/geonetcast:latest -t docker.homejota.net/geoos/geonetcast:0.21 .

FROM docker.homejota.net/geoos/gdal-node14
EXPOSE 8080
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install --production

COPY . .
CMD ["node", "index"]