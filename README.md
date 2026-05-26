# Overview
This repository contains the source code for the paper "TransiChain-Gen: Adaptive and Constraint-Aware Differential Privacy for Public Transit Trajectories".
### Description of paper
TransiChain-Gen is a novel DP-based trajectory synthesis framework featuring a dynamic temporal partitioning strategy guided by passenger flow and distribution uniformity to preserve low-density time intervals, along with an adaptive budget allocation mechanism that prevents over-perturbation of short paths while enforcing route constraints to generate realistic trips. Extensive experiments on real-world smart card datasets demonstrate that TransiChain-Gen substan-tially improves data utility and trajectory realism, consistently outperforming state-of-the-art methods.
# Getting Started
The raw data format is as follows:

Fields are comma-separated with the following notation: O for Origin, D for Destination, H for Home, and W for Workplace.
```id,Ostation,Odate,Olon,Olat,Ddate,Dstation,Dlon,Dlat,trans,Hlat,Hlon,Wlat,Wlon,fare,translist,detail,bus,sub;subtans```

The most critical field is 'detail', which connects multiple OD sequences to form complete trip chains. Each entry contains 
```id;Odate;Ostation;Olon;Olat;Ddate;Dstation;Dlon;Dlat;...```

The raw data preprocessing code is available in ```src/TripChain/pre```, ```adaptive.py``` implements adaptive time discretization and converts data formats into preprocessed formats via ```DataFrame.scala```. This process involves complete trip chain sequence extraction, adaptive temporal generalization, and spatial transfer point simplification.

preprocessed data format is as follows：
```timestamp1 station1,timestamp2 station2,...```

The data generation code is available in ``` src/TripChain/generator/Main_TransiChain.java ```.

The evaluation code is available in ```src/TripChain/evaluation```
# Evaluating benchmarks

there are two open-source benchmarks available, which are:

-MoveSim: The code repository is available at [MoveSim GitHub repository](https://github.com/FIBLAB/MoveSim).

-Adatrace: The code repository is available at [Adatrace GitHub repository](https://github.com/git-disl/AdaTrace).
