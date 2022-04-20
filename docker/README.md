This module is used to publish docker images to acryldata's docker repository. 

# Building the Docker Image 
docker build -f docker/datahub_actions/Dockerfile . --no-cache --build-arg

# Running the Docker Image
docker run <image-id>