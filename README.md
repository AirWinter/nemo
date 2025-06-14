# NEMO
This repository contains the code for the implementation of `NEMO` along with the code for the baselines it is compared to. All the different protocols can be found in `src`, taking in a `Vec<Transaction>` as input and producing an `ExecutionResult`. This execution result contains the duration of the execution (given in seconds), the number of transaction executions, the number of transaction validations (if applicable) and the number of greedy commits (if applicable).

This repository does not contain the code used for generating synthetic workloads.
