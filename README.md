# Parallelized hierarchical Infomap community detection for large networks

Jason Portenoy 2021

For very large networks (on the scale of ~1 billion edges), Infomap clustering does not complete in a reasonable time. 
Instead, run RelaxMap (parallel implementation of Infomap) to get a two-level partition, then extract subgraphs and run hierarchical infomap on those.

- [Parallelized hierarchical Infomap community detection for large networks](#parallelized-hierarchical-infomap-community-detection-for-large-networks)
	- [Getting Started](#getting-started)
		- [Using Docker](#using-docker)

## Getting Started

### Using Docker

To run on example data, first, build the Docker image, then run this command within this repository:

```sh
docker run --rm -v `pwd`/data:/data infomaplargenetwork data/flow.net data 18 --min-size 1 --debug
```
