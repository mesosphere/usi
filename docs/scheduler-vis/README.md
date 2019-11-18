# Scheduler Stream visualization

This small project facilitates the generation of a stream diagram visualization from the USI code itself. To run, simple invoke:

```
gradle scheduler-vis:run
```

It will output the file `../scheduler-stream.svg`

# Dependencies

The graph generation library has no mandatory dependencies, since it includes a JVM implementation of graphviz. However, if graphviz is installed and the `dot` executable is reachable from the PATH, then it will use that to generate the diagram, instead (it is faster).
