# Parallelized hierarchical Infomap community detection for large networks

Jason Portenoy 2021

For very large networks (on the scale of ~1 billion edges), Infomap clustering does not complete in a reasonable time. 
Instead, run RelaxMap (parallel implementation of Infomap) to get a two-level partition, then extract subgraphs and run hierarchical infomap on those.

- [Parallelized hierarchical Infomap community detection for large networks](#parallelized-hierarchical-infomap-community-detection-for-large-networks)
	- [Getting Started](#getting-started)
		- [Using Docker](#using-docker)
	- [Additional info](#additional-info)

## Getting Started

### Using Docker

To run on example data, first build the Docker image:

```sh
docker build --pull --rm -f "Dockerfile" -t infomaplargenetwork:latest "."
```

Then run this command within this repository:

```sh
docker run --rm -v `pwd`/data:/data infomaplargenetwork data/flow.net data 18 --min-size 1 --debug
```

This will find community detection for the small example network `flow.net`, using a maximum of 18 CPUs for the RelaxMap step, and outputting to the `data` directory.

The output file is a `.tsv` file which will be in the `data` directory, as well as intermediate `.tree` and `.clu` files from RelaxMap.

## Additional info

The RelaxMap step uses the `h1theswan/RelaxMap` Docker image from Docker Hub. See <https://github.com/h1-the-swan/RelaxMap> for details.

The full Docker image is built on the Ubuntu image and is ~1-2 GB.