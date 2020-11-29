## Web UI for SmartRider:
1. smartrider_app.py is to provide web service to users. It take users input(geo location, month, traffic, social restriction etc.) and queries data for matched records, then plots the result.
1. assets folder contains favicon, logo images, and css sheets.

smartrider_app.py binds to 8080 port by default, a reverse proxy is needed to bridge the web service to port 80 or 443. In this project, a nginx docker is used to meet this need. Please check setup/playbooks for detail.


## Updates in 11/29/2020:
1. a dockerfile is provided to create a docker image for smartrider app.
1. nginx.conf and docker-compose.yml were udpated to use the docker image and Cloudflare's SSL service.
