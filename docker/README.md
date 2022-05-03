This module is used to publish docker images to acryldata's docker repository. 

# Building the Docker Image 

From the root directory,

```
docker build -f docker/datahub-actions/Dockerfile . --no-cache
```

# Running the Docker Image

```
docker image ls # Grab the container id 
docker run --env-file docker/actions.env --network datahub_network <image-id>
```

And to mount a custom Action configuration, you can do the following:

```
docker run --env-file docker/actions.env --network datahub_network --mount type=bind,source="$(pwd)"/examples/hello_world.yaml,target=/etc/datahub/actions/conf/hello_world.yaml <image-id>
```