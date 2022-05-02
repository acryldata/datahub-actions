This module is used to publish docker images to acryldata's docker repository. 

# Building the Docker Image 

From the root directory,

```
docker build -f docker/datahub_actions/Dockerfile . --no-cache
```

# Running the Docker Image

```
docker run <image-id> --env-file <env-file>
docker run --env-file docker/actions.env --network datahub_network <image-id>
```