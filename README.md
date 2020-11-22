# SearchEngine - CS1660 Project
&nbsp;

Quick Start:

- open Xquartz
- export DISPLAY=:0.0
- xhost +
- docker build --tag <insert tag> .
- docker run -e DISPLAY=(IP address):0 searchengine:1.0

Walkthrough video:

Docker image: zliu3/searchengine:1.0

Docker repo:https://hub.docker.com/repository/docker/zliu3/searchengine

- GUI.java - the main java file to set up GUI on the client side

- InvertedIndex.java - mapreduce program implemented on GCP to conduct inverted indice

- Searching.java - mapreduce program implemented on GCP to search for specific words

- TopN.java - mapreduce program implemented on GCP to find the Top-N most common words in the data files





