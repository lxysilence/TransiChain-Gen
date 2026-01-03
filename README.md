# Overview
This repository contains the source code for the paper "TransiChain-Gen: Adaptive and Constraint-Aware Differential Privacy for Public Transit Trajectories".
### Description of paper
TransiChain-Gen is a novel DP-based trajectory synthesis framework featuring a dynamic temporal partitioning strategy guided by passenger flow and distribution uniformity to preserve low-density time intervals, along with an adaptive budget allocation mechanism that prevents over-perturbation of short paths while enforcing route constraints to generate realistic trips. Extensive experiments on real-world smart card datasets demonstrate that TransiChain-Gen substan-tially improves data utility and trajectory realism, consistently outperforming state-of-the-art methods.
# Getting Started
The raw data format is as follows:

The raw data preprocessing code is available in ```src/TripChain/pre```, ```adaptive.py``` implements adaptive time discretization and converts data formats into preprocessed formats via ```DataFrame.scala```.

preprocessed data format is as follows：

The data generation code is available in ``` src/TripChain/generator/Main_TransiChain.java ```.

The evaluation code is available in ```src/TripChain/evaluation```
# Evaluating benchmarks

there are two open-source benchmarks available, which are:

-Adatrace: The code repository is available at [Adatrace GitHub repositoryr](https://github.com/FIBLAB/MoveSim).

-MoveSim: The code repository is available at [MoveSim GitHub repositoryr](https://github.com/git-disl/AdaTrace).
